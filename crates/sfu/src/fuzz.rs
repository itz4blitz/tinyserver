//! Entry points for the cargo-fuzz targets in `fuzz/`. Each takes any bytes and must not
//! panic. Not part of the server.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use str0m::change::SdpOffer;
use str0m::config::{CryptoProvider, DtlsCert};
use str0m::media::{Direction, MediaKind, Simulcast, SimulcastLayer};
use str0m::net::{DatagramRecv, Protocol, Receive};
use str0m::{Input, Output, Rtc, RtcConfig};

use crate::engine::{candidates, rtc_config, H264_PROFILE};
use crate::sdp::{share_sending, Caps};
use crate::tcp::{encode, BadFrame, Framer, FRAME_MAX};
use crate::video::{ceiling, pick_layer, Floor, Layer, Link, Quality, Rebase};
use crate::Config;

const PUBLIC: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 50000);
const CLIENT: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)), 40000);

/// One certificate for every run, as the server makes one per process.
fn crypto() -> &'static (Arc<CryptoProvider>, DtlsCert) {
    static CRYPTO: OnceLock<(Arc<CryptoProvider>, DtlsCert)> = OnceLock::new();
    CRYPTO.get_or_init(|| {
        let crypto = Arc::new(str0m::crypto::from_feature_flags());
        let cert = crypto
            .dtls_provider
            .generate_certificate()
            .unwrap_or_else(|| panic!("no DTLS certificate"));
        (crypto, cert)
    })
}

/// An ICE-lite `Rtc` like the media thread's, with its candidates.
fn server(now: Instant) -> Rtc {
    let (crypto, cert) = crypto();
    let mut rtc = rtc_config(crypto, cert, Some(1000)).build(now);
    for candidate in candidates(PUBLIC, None).into_iter().flatten() {
        rtc.add_local_candidate(candidate);
    }
    rtc
}

/// Polls until a timeout, as the media thread does after every input.
fn drain(rtc: &mut Rtc) {
    for _ in 0..1000 {
        match rtc.poll_output() {
            Ok(Output::Timeout(_)) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// A TCP byte stream, cut into reads at the size the first byte names. Every frame that
/// comes out is within the cap and frames again.
pub fn tcp_frames(data: &[u8]) {
    let Some((&step, stream)) = data.split_first() else {
        return;
    };
    let mut framer = Framer::new(FRAME_MAX);
    for read in stream.chunks(usize::from(step).max(1)) {
        framer.push(read);
        loop {
            match framer.next_frame() {
                Ok(Some(frame)) => {
                    assert!(!frame.is_empty() && frame.len() <= FRAME_MAX);
                    let mut out = Vec::new();
                    assert!(encode(&frame, &mut out));
                    assert_eq!(out.len(), frame.len() + 2);
                }
                Ok(None) => break,
                Err(BadFrame) => return,
            }
        }
    }
}

/// An offer as the API passes it on: the caps, the share check, str0m's parser, then a
/// fresh participant accepting it.
pub fn offer(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let config = Config::new(PUBLIC, PUBLIC.ip());
    if Caps::from(&config).check(text).is_err() {
        return;
    }
    let _ = share_sending(text);
    let Ok(offer) = SdpOffer::from_sdp_string(text) else {
        return;
    };
    let now = Instant::now();
    let mut rtc = server(now);
    if let Ok(answer) = rtc.sdp_api().accept_offer(offer) {
        let _ = answer.to_sdp_string();
    }
    drain(&mut rtc);
    let _ = rtc.handle_input(Input::Timeout(now + Duration::from_secs(1)));
    drain(&mut rtc);
}

/// Packets from the network into a participant that has an answer out, the way the media
/// thread hands them over: a microphone, an audio slot, a simulcast camera, a share, and a
/// video slot.
/// The first byte of each packet says UDP or TCP and how long it is.
pub fn packets(data: &[u8]) {
    let now = Instant::now();
    let mut config = RtcConfig::new().clear_codecs().enable_opus(true, false);
    config
        .codec_config()
        .add_h264(108.into(), Some(109.into()), true, H264_PROFILE);
    let mut client = config.build(now);
    let mut change = client.sdp_api();
    change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
    change.add_media(MediaKind::Audio, Direction::RecvOnly, None, None, None);
    let mut layers = Simulcast::new();
    layers.add_send_layer(SimulcastLayer::new("h"));
    layers.add_send_layer(SimulcastLayer::new("l"));
    change.add_media(
        MediaKind::Video,
        Direction::SendOnly,
        None,
        None,
        Some(layers),
    );
    change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    change.add_media(MediaKind::Video, Direction::RecvOnly, None, None, None);
    let Some((offer, _)) = change.apply() else {
        return;
    };
    let mut rtc = server(now);
    if rtc.sdp_api().accept_offer(offer).is_err() {
        return;
    }
    drain(&mut rtc);
    let mut rest = data;
    let mut at = now;
    while let Some((&head, tail)) = rest.split_first() {
        let len = usize::from(head & 0x7f).min(tail.len());
        let (packet, tail) = tail.split_at(len);
        rest = tail;
        let proto = if head & 0x80 == 0 {
            Protocol::Udp
        } else {
            Protocol::Tcp
        };
        at += Duration::from_millis(5);
        let Ok(contents) = DatagramRecv::try_from(packet) else {
            continue;
        };
        let input = Input::Receive(
            at,
            Receive {
                proto,
                source: CLIENT,
                destination: PUBLIC,
                contents,
            },
        );
        // The media thread asks first and lets strangers through only as STUN. Past that too.
        let _ = rtc.accepts(&input);
        let _ = rtc.handle_input(input);
        drain(&mut rtc);
    }
}

/// The video routing arithmetic, driven by bytes: timestamps from sources switching on one
/// slot, speakers and levels for the floor, and estimates and budgets for the layer. Nothing
/// may overflow or panic, and a slot's output time never stands still across a switch.
pub fn video(data: &[u8]) {
    let start = Instant::now();
    let mut rebase: Rebase<u8> = Rebase::default();
    let mut floor: Floor<u8> = Floor::new();
    let mut link = Link::default();
    let mut at = start;
    let mut last: Option<(u64, u8)> = None;
    for chunk in data.chunks(10) {
        let [op, a, b, rest @ ..] = chunk else {
            return;
        };
        let mut wide = [0u8; 8];
        for (to, from) in wide.iter_mut().zip(rest) {
            *to = *from;
        }
        let value = u64::from_le_bytes(wide);
        at += Duration::from_millis(u64::from(*a) * 4);
        match op % 4 {
            0 => {
                let source = b % 4;
                let out = rebase.map(source, value, at);
                if let Some((previous, from)) = last {
                    if from != source {
                        assert_ne!(out, previous, "a switch repeated the last time");
                    }
                }
                last = Some((out, source));
            }
            1 => {
                let speakers: Vec<(u8, f32)> = rest
                    .iter()
                    .map(|byte| (byte % 4, -f32::from(byte & 0x7f)))
                    .take(usize::from(b % 5))
                    .collect();
                floor.hear(&speakers, at);
                assert!(floor.recent().len() <= 4);
            }
            2 => floor.forget(&(b % 4)),
            _ => {
                let quality = [Quality::Off, Quality::Low, Quality::High][usize::from(b % 3)];
                let budget = (value & 1 == 1).then_some((value >> 1) as u32);
                let most = ceiling(budget, value >> 40, value >> 56, value >> 52);
                if budget.is_none() {
                    assert!(most.share && most.camera == Quality::High);
                }
                let estimate = (a % 2 == 1).then_some(value >> 8);
                let fit = link.update(estimate, Duration::from_millis(u64::from(*b)), at);
                let layer = pick_layer(fit, quality, most.camera);
                if most.camera == Quality::Off || quality == Quality::Off {
                    assert_eq!(layer, None);
                }
                if quality.min(most.camera) == Quality::Low {
                    assert_ne!(layer, Some(Layer::High));
                }
            }
        }
    }
}
