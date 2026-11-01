//! A headless call client on str0m. Tests use it, and tinyclient's `mediad` follows it.
//!
//! 1. [`Client::new`] binds a UDP socket and builds the offer: one `sendonly` audio m-line for
//!    the microphone, one `recvonly` audio m-line per receive slot, one video m-line for the
//!    camera with simulcast rids `h` and `l`, `inactive` until the camera starts, one video
//!    m-line for a screen share, `inactive` until it starts, and one `recvonly` video m-line
//!    for the one video slot. [`Client::tcp`] offers one active TCP
//!    candidate instead, and no UDP at all, as on a network that blocks UDP. It connects to
//!    the SFU's passive TCP candidate when str0m first sends to it, and frames each packet
//!    with a 2-byte length (RFC 4571).
//! 2. The offer goes out as `call.join` `sdp`. The `call.answer` `sdp` comes back to
//!    [`Client::accept`]. The client never trickles; the SFU learns its address from STUN.
//! 3. [`Client::start`] drives str0m on its own thread, with no async runtime: a 20 ms Opus
//!    frame out, a camera frame on each layer every 1/30 s while the camera is on, a share
//!    frame every 1/15 s while the share is on, and every frame in, tagged with the mid it
//!    arrived on.
//! 4. `call.slots` says which mid carries whom. The SFU rewrites sequence numbers, but a slot
//!    that changes hands carries a new speaker's timestamps, so reset that slot's jitter
//!    buffer and decoder when its `user_id` changes. The video slot changes hands on a
//!    keyframe; reset the video decoder then. A frame str0m marks not contiguous was lost:
//!    drop frames until a keyframe, and ask for one with PLI every 300 ms.
//! 5. [`Running::camera`] flips the camera m-line and returns the offer for `call.offer`;
//!    [`Running::accept`] takes its answer. [`Running::share`] does the same for the share.
//!    Either starts sending only once the answer is accepted: the SFU drops what comes before
//!    it takes the offer, and could keep the tail of a frame cut that way. str0m flips the
//!    m-line when it makes the offer, so an offer refused with `share_taken` is undone with
//!    [`Running::reject`]. A share an owner or admin ended is stopped with `share(false)`. A
//!    keyframe request from the SFU restarts that layer at its keyframe.
//!
//! The audio frames are not real Opus. Each one is a TOC byte, a marker byte that says who
//! sent it, and a counter. The SFU never decodes, so it cannot tell. The camera frames are
//! real H.264 constrained baseline from `fixtures/` (see its README), each with an SEI
//! message that says who sent it and on which layer, padded to the layer's nominal bitrate.
//! The share sends the `h` clip, tagged layer `s` and padded to the share's 2.5 Mb/s.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::io::{self, ErrorKind};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mio::net::{TcpStream, UdpSocket};
use mio::{Events, Interest, Poll, Token, Waker};
use str0m::bwe::Bitrate;
use str0m::change::{SdpAnswer, SdpPendingOffer};
use str0m::media::{Direction, Frequency, KeyframeRequestKind, MediaKind, MediaTime, Mid, Rid};
use str0m::media::{Simulcast, SimulcastLayer};
use str0m::net::{Protocol, Receive, TcpType};
use str0m::{Candidate, Event, Input, Output, Rtc, RtcConfig};

use crate::tcp::{Conn, Readout};

/// Received audio payloads kept for [`Running::take_payloads`]: 20 s of one speaker.
const PAYLOADS_KEPT: usize = 1000;
/// One Opus frame: 20 ms at 48 kHz.
const FRAME: Duration = Duration::from_millis(20);
const SAMPLES_PER_FRAME: u64 = 960;
/// The RFC 6464 level sent with each frame, in -dBov. Loud enough to count as speaking.
const LEVEL: i8 = -20;
/// One camera frame: 30 fps on the 90 kHz video clock.
const VIDEO_FRAME: Duration = Duration::from_nanos(33_333_333);
const TICKS_PER_VIDEO_FRAME: u64 = 3000;
/// The two simulcast layers: a 320×180 and a 160×90 clip, each 10 s of one keyframe and then
/// P frames, padded to 1.2 Mb/s and 350 kb/s.
const LAYERS: [Layer; 2] = [
    Layer {
        rid: "h",
        clip: include_bytes!("../fixtures/camera-h.h264"),
        bytes: 5000,
    },
    Layer {
        rid: "l",
        clip: include_bytes!("../fixtures/camera-l.h264"),
        bytes: 1458,
    },
];
/// One share frame: 15 fps on the 90 kHz video clock.
const SHARE_FRAME: Duration = Duration::from_nanos(66_666_667);
const TICKS_PER_SHARE_FRAME: u64 = 6000;
/// The share's nominal bitrate, in kb/s. [`Running::share_kbps`] changes it.
const SHARE_KBPS: u64 = 2500;
/// The share: the `h` clip, tagged `s`, padded to the share bitrate at 15 fps.
const SHARE: Layer = Layer {
    rid: "s",
    clip: include_bytes!("../fixtures/camera-h.h264"),
    bytes: share_bytes(SHARE_KBPS),
};

/// One share frame's size at `kbps`.
const fn share_bytes(kbps: u64) -> usize {
    (kbps * 1000 / 8 / 15) as usize
}
/// Names the SEI message that tags each camera frame. No zero bytes, so it needs no
/// emulation prevention.
const TAG: &[u8; 16] = b"tinyserver-video";
/// After a lost frame on the video slot, a PLI goes out this often until a keyframe comes.
const PLI_EVERY: Duration = Duration::from_millis(300);
/// A REMB goes out this often while one is set, as browsers send them.
const REMB_EVERY: Duration = Duration::from_secs(1);
const UDP: Token = Token(0);
const WAKE: Token = Token(1);
/// TCP streams take tokens from here up.
const FIRST_STREAM: usize = 2;
/// The discard port, which active TCP candidates advertise (RFC 6544).
const ACTIVE_PORT: u16 = 9;

/// H.264 constrained baseline, level 3.1, packetization mode 1, as the SFU takes it.
const H264_PROFILE: u32 = 0x42e01f;
const H264_PT: u8 = 108;
const H264_RTX_PT: u8 = 109;

/// An audio frame the client received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// The receive m-line it came in on. `call.slots` maps it to a person.
    pub mid: String,
    /// The sender's marker byte.
    pub marker: u8,
}

/// A camera or share frame the client received on its video slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoFrame {
    pub mid: String,
    /// The sender's marker byte, from the frame's SEI tag. 0 if it has none.
    pub marker: u8,
    /// The layer it was sent on, `b'h'` or `b'l'`, or `b's'` for a share, from the same tag.
    pub layer: u8,
    pub keyframe: bool,
}

struct Layer {
    /// The simulcast rid. The share sends none; its first byte only tags the frames.
    rid: &'static str,
    clip: &'static [u8],
    /// Every frame is padded to this size.
    bytes: usize,
}

/// How the client reaches the SFU.
enum Link {
    Udp(UdpSocket),
    /// One stream per SFU TCP candidate str0m sends to, opened on first use.
    Tcp {
        streams: HashMap<SocketAddr, (Token, Conn)>,
        next: usize,
    },
}

/// A request from [`Running`] to the client's thread.
enum Request {
    /// Turn the camera, or with `true` first the share, on or off.
    Flip(bool, bool, SyncSender<Result<String, String>>),
    /// The SFU refused the last offer.
    Reject(SyncSender<()>),
    Answer(String, SyncSender<Result<(), String>>),
}

/// A client with an offer out and no answer yet.
pub struct Client {
    rtc: Rtc,
    link: Link,
    /// The local candidate's address. Every packet in is handed to str0m with it.
    local: SocketAddr,
    microphone: Mid,
    camera: Mid,
    share: Mid,
    screen: Mid,
    pending: Option<SdpPendingOffer>,
    offer: String,
    /// The m-line and direction the pending offer changed from, to undo it if refused.
    flipped: Option<(Mid, Direction)>,
    /// A frame on the video slot was lost. Frames are dropped until a keyframe, as a decoder
    /// would, and asked for with PLI.
    lost: bool,
    asked: Option<Instant>,
}

impl Client {
    /// Binds `bind` and builds an offer with `slots` receive slots. Use the room cap less one.
    pub fn new(bind: SocketAddr, slots: usize) -> io::Result<Self> {
        let socket = UdpSocket::bind(bind)?;
        let local = socket.local_addr()?;
        let candidate = Candidate::host(local, "udp");
        Self::build(Link::Udp(socket), local, candidate, slots)
    }

    /// Like [`Client::new`], with one active TCP candidate on `ip` and no UDP.
    pub fn tcp(ip: IpAddr, slots: usize) -> io::Result<Self> {
        let local = SocketAddr::new(ip, ACTIVE_PORT);
        let candidate = Candidate::builder()
            .tcp()
            .host(local)
            .tcptype(TcpType::Active)
            .build();
        let link = Link::Tcp {
            streams: HashMap::new(),
            next: FIRST_STREAM,
        };
        Self::build(link, local, candidate, slots)
    }

    fn build(
        link: Link,
        local: SocketAddr,
        candidate: Result<Candidate, str0m::error::IceError>,
        slots: usize,
    ) -> io::Result<Self> {
        let mut config = crate::engine::release_audio_promptly(
            RtcConfig::new().clear_codecs().enable_opus(true, false),
        );
        config.codec_config().add_h264(
            H264_PT.into(),
            Some(H264_RTX_PT.into()),
            true,
            H264_PROFILE,
        );
        let mut rtc = config.build(Instant::now());
        let candidate =
            candidate.map_err(|error| io::Error::other(format!("candidate: {error:?}")))?;
        rtc.add_local_candidate(candidate);

        let mut change = rtc.sdp_api();
        let microphone = change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
        for _ in 0..slots {
            change.add_media(MediaKind::Audio, Direction::RecvOnly, None, None, None);
        }
        let mut layers = Simulcast::new();
        for layer in &LAYERS {
            layers.add_send_layer(SimulcastLayer::new(layer.rid));
        }
        let camera = change.add_media(
            MediaKind::Video,
            Direction::Inactive,
            None,
            None,
            Some(layers),
        );
        let share = change.add_media(MediaKind::Video, Direction::Inactive, None, None, None);
        let screen = change.add_media(MediaKind::Video, Direction::RecvOnly, None, None, None);
        let (offer, pending) = change
            .apply()
            .ok_or_else(|| io::Error::other("nothing to offer"))?;
        Ok(Self {
            rtc,
            link,
            local,
            microphone,
            camera,
            share,
            screen,
            pending: Some(pending),
            offer: offer.to_sdp_string(),
            flipped: None,
            lost: false,
            asked: None,
        })
    }

    /// The SDP for `call.join`.
    pub fn offer(&self) -> &str {
        &self.offer
    }

    /// Takes the SDP from `call.answer`.
    pub fn accept(&mut self, answer: &str) -> Result<(), String> {
        accept(&mut self.rtc, &mut self.pending, answer)
    }

    /// Runs the call on a thread, sending frames tagged `marker`.
    pub fn start(self, marker: u8) -> io::Result<Running> {
        let shared = Arc::new(Shared::default());
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKE)?);
        let (requests, inbox) = mpsc::sync_channel(4);
        let thread = {
            let shared = shared.clone();
            std::thread::Builder::new()
                .name("headless".to_string())
                .spawn(move || self.run(poll, &inbox, marker, &shared))?
        };
        Ok(Running {
            shared,
            requests,
            waker,
            thread: Some(thread),
        })
    }

    /// The `mediad` loop: drain str0m, answer a request, send a frame when one is due, then
    /// wait for a packet, a request, the next str0m timeout, or the next frame, whichever
    /// comes first. Each packet in is one input, and str0m is drained after every one.
    fn run(mut self, mut poll: Poll, inbox: &Receiver<Request>, marker: u8, shared: &Shared) {
        if let Link::Udp(socket) = &mut self.link {
            if poll
                .registry()
                .register(socket, UDP, Interest::READABLE)
                .is_err()
            {
                return;
            }
        }
        let mut events = Events::with_capacity(64);
        let mut camera = Camera::default();
        let mut share = Share::default();
        let mut next_frame = Instant::now();
        let mut next_video = Instant::now();
        let mut next_share = Instant::now();
        let mut next_remb = Instant::now();
        let mut sent: u64 = 0;
        let mut packets: VecDeque<Packet> = VecDeque::new();
        while !shared.stop.load(Ordering::Acquire) {
            let Some(timeout) = self.drain(shared, &poll, &mut camera, &mut share) else {
                return;
            };
            if let Some((proto, source, packet)) = packets.pop_front() {
                if let Some(input) = input(proto, source, self.local, packet.as_slice()) {
                    if self.rtc.handle_input(input).is_err() {
                        return;
                    }
                }
                continue;
            }
            if let Ok(request) = inbox.try_recv() {
                self.answer(request);
                continue;
            }
            let now = Instant::now();
            let connected = shared.connected.load(Ordering::Acquire);
            if connected && shared.sending.load(Ordering::Acquire) && now >= next_frame {
                let sealed = lock(&shared.sealed).clone();
                self.send_frame(marker, sent, now, sealed);
                sent += 1;
                next_frame += FRAME;
                continue;
            }
            // A camera or share starts once its answer is accepted, so the SFU never gets the
            // tail of a frame sent before it took the offer.
            let settled = self.pending.is_none();
            let camera_on = connected && self.sending_on(self.camera);
            let camera_due = camera_on && (camera.on || settled);
            if !camera_on {
                camera.on = false;
                next_video = now;
            } else if camera_due && now >= next_video {
                if !camera.on {
                    // A camera that starts begins each layer at its keyframe.
                    camera = Camera {
                        on: true,
                        ..Camera::default()
                    };
                }
                self.send_video(marker, &mut camera, now);
                next_video += VIDEO_FRAME;
                continue;
            }
            let share_on = connected && self.sending_on(self.share);
            let share_due = share_on && (share.on || settled);
            if !share_on {
                share.on = false;
                next_share = now;
            } else if share_due && now >= next_share {
                if !share.on {
                    share = Share {
                        on: true,
                        ..Share::default()
                    };
                }
                self.send_share(marker, &mut share, shared, now);
                next_share += SHARE_FRAME;
                continue;
            }
            let remb = shared.remb.load(Ordering::Acquire);
            if connected && remb > 0 && now >= next_remb {
                if let Some(stream) = self.rtc.direct_api().stream_rx_by_mid(self.screen, None) {
                    stream.request_remb(Bitrate::kbps(remb));
                }
                next_remb = now + REMB_EVERY;
                continue;
            }
            let mut wake = timeout.min(next_frame.max(now + Duration::from_millis(1)));
            if camera_due {
                wake = wake.min(next_video);
            }
            if share_due {
                wake = wake.min(next_share);
            }
            let wait = wake
                .saturating_duration_since(now)
                .max(Duration::from_millis(1));
            if let Err(error) = poll.poll(&mut events, Some(wait)) {
                if error.kind() != ErrorKind::Interrupted {
                    return;
                }
            }
            for event in &events {
                self.receive(event.token(), event.is_writable(), &mut packets);
            }
            if packets.is_empty()
                && self
                    .rtc
                    .handle_input(Input::Timeout(Instant::now()))
                    .is_err()
            {
                return;
            }
        }
    }

    /// Renegotiates for [`Running`]: a new offer with the camera or share on or off, or its
    /// answer.
    fn answer(&mut self, request: Request) {
        match request {
            Request::Flip(share, on, reply) => {
                let direction = if on {
                    Direction::SendOnly
                } else {
                    Direction::Inactive
                };
                let mid = if share { self.share } else { self.camera };
                self.flipped = self.rtc.media(mid).map(|media| (mid, media.direction()));
                let mut change = self.rtc.sdp_api();
                change.set_direction(mid, direction);
                let offer = match change.apply() {
                    Some((offer, pending)) => {
                        self.pending = Some(pending);
                        Ok(offer.to_sdp_string())
                    }
                    None => Err("nothing changed".to_string()),
                };
                let _ = reply.send(offer);
            }
            Request::Answer(answer, reply) => {
                self.flipped = None;
                let _ = reply.send(accept(&mut self.rtc, &mut self.pending, &answer));
            }
            Request::Reject(reply) => {
                // str0m flipped the m-line when it made the offer. Flip it back, and drop
                // both offers: the SFU still has the state before the first.
                if let Some((mid, direction)) = self.flipped.take() {
                    let mut change = self.rtc.sdp_api();
                    change.set_direction(mid, direction);
                    let _ = change.apply();
                }
                self.pending = None;
                let _ = reply.send(());
            }
        }
    }

    fn sending_on(&self, mid: Mid) -> bool {
        self.rtc
            .media(mid)
            .is_some_and(|media| media.direction().is_sending())
    }

    /// Reads what `token` has waiting into `packets`, and flushes a writable stream.
    fn receive(&mut self, token: Token, writable: bool, packets: &mut VecDeque<Packet>) {
        match &mut self.link {
            Link::Udp(socket) => {
                let mut buf = vec![0; 2000];
                while let Ok((n, source)) = socket.recv_from(&mut buf) {
                    if let Some(packet) = buf.get(..n) {
                        packets.push_back((Protocol::Udp, source, packet.to_vec()));
                    }
                }
            }
            Link::Tcp { streams, .. } => {
                let Some((&server, (_, conn))) =
                    streams.iter_mut().find(|(_, (held, _))| *held == token)
                else {
                    return;
                };
                if writable && conn.flush().is_err() {
                    streams.remove(&server);
                    return;
                }
                let end = conn.read(usize::MAX, |frame| {
                    packets.push_back((Protocol::Tcp, server, frame.to_vec()));
                    true
                });
                if end == Readout::Closed {
                    streams.remove(&server);
                }
            }
        }
    }

    /// Sends one packet. Over TCP, the first one to an address opens the stream.
    fn transmit(&mut self, poll: &Poll, proto: Protocol, to: SocketAddr, packet: &[u8]) {
        match &mut self.link {
            Link::Udp(socket) if proto == Protocol::Udp => {
                let _ = socket.send_to(packet, to);
            }
            Link::Tcp { streams, next } if proto == Protocol::Tcp => {
                let (_, conn) = match streams.entry(to) {
                    Entry::Occupied(held) => held.into_mut(),
                    Entry::Vacant(free) => {
                        let Ok(mut socket) = TcpStream::connect(to) else {
                            return;
                        };
                        let _ = socket.set_nodelay(true);
                        let token = Token(*next);
                        *next += 1;
                        let interest = Interest::READABLE | Interest::WRITABLE;
                        if poll
                            .registry()
                            .register(&mut socket, token, interest)
                            .is_err()
                        {
                            return;
                        }
                        free.insert((token, Conn::new(socket, to)))
                    }
                };
                if conn.queue(packet, false) && conn.flush().is_err() {
                    streams.remove(&to);
                }
            }
            _ => {}
        }
    }

    /// Polls str0m until it asks for a timeout. `None` once the connection is over.
    fn drain(
        &mut self,
        shared: &Shared,
        poll: &Poll,
        camera: &mut Camera,
        share: &mut Share,
    ) -> Option<Instant> {
        loop {
            if !self.rtc.is_alive() {
                return None;
            }
            match self.rtc.poll_output().ok()? {
                Output::Timeout(at) => return Some(at),
                Output::Transmit(transmit) => {
                    let (proto, to) = (transmit.proto, transmit.destination);
                    self.transmit(poll, proto, to, &transmit.contents);
                }
                Output::Event(Event::Connected) => shared.connected.store(true, Ordering::Release),
                Output::Event(Event::MediaData(data)) if data.mid == self.screen => {
                    let keyframe = data.is_keyframe();
                    self.lost = !keyframe && (self.lost || !data.contiguous);
                    if self.lost {
                        self.ask_keyframe();
                        continue;
                    }
                    let (marker, layer) = tag(&data.data).unwrap_or_default();
                    let frame = VideoFrame {
                        mid: data.mid.to_string(),
                        marker,
                        layer,
                        keyframe,
                    };
                    lock(&shared.video).push(frame);
                }
                Output::Event(Event::MediaData(data)) => {
                    let mut payloads = lock(&shared.payloads);
                    if payloads.len() >= PAYLOADS_KEPT {
                        payloads.remove(0);
                    }
                    payloads.push((data.mid.to_string(), data.data.to_vec()));
                    drop(payloads);
                    let marker = data.data.get(1).copied().unwrap_or_default();
                    let frame = Frame {
                        mid: data.mid.to_string(),
                        marker,
                    };
                    lock(&shared.frames).push(frame);
                }
                Output::Event(Event::KeyframeRequest(request)) if request.mid == self.camera => {
                    shared.keyframes_asked.fetch_add(1, Ordering::AcqRel);
                    let rid = request.rid.as_deref();
                    for (at, layer) in LAYERS.iter().enumerate() {
                        if rid.is_none() || rid == Some(layer.rid) {
                            camera.restart[at] = true;
                        }
                    }
                }
                Output::Event(Event::KeyframeRequest(request)) if request.mid == self.share => {
                    shared.share_keyframes_asked.fetch_add(1, Ordering::AcqRel);
                    share.restart = true;
                }
                Output::Event(_) => {}
            }
        }
    }

    /// Asks the SFU for a keyframe on the video slot, at most every `PLI_EVERY`.
    fn ask_keyframe(&mut self) {
        let now = Instant::now();
        if self
            .asked
            .is_some_and(|at| now.saturating_duration_since(at) < PLI_EVERY)
        {
            return;
        }
        self.asked = Some(now);
        if let Some(stream) = self.rtc.direct_api().stream_rx_by_mid(self.screen, None) {
            stream.request_keyframe(KeyframeRequestKind::Pli);
        }
    }

    /// One audio frame: the marker frame, or `sealed` as it is when set.
    fn send_frame(&mut self, marker: u8, sent: u64, now: Instant, sealed: Option<Vec<u8>>) {
        let Some(writer) = self.rtc.writer(self.microphone) else {
            return;
        };
        let Some(pt) = writer.payload_params().next().map(|params| params.pt()) else {
            return;
        };
        // TOC byte 0x78: SILK wideband, 20 ms, one frame.
        let payload = sealed.unwrap_or_else(|| {
            let mut payload = vec![0x78, marker];
            payload.extend_from_slice(&sent.to_be_bytes());
            payload
        });
        let time = MediaTime::new(sent * SAMPLES_PER_FRAME, Frequency::FORTY_EIGHT_KHZ);
        let _ = writer
            .audio_level(LEVEL, true)
            .write(pt, now, time, payload);
    }

    /// One frame on each layer, with the same RTP time, as a simulcast encoder sends them.
    fn send_video(&mut self, marker: u8, camera: &mut Camera, now: Instant) {
        let time = MediaTime::new(camera.sent * TICKS_PER_VIDEO_FRAME, Frequency::NINETY_KHZ);
        camera.sent += 1;
        for (at, layer) in LAYERS.iter().enumerate() {
            let frames = &camera.clips[at];
            if camera.restart[at] {
                camera.restart[at] = false;
                camera.next[at] = 0;
            }
            let Some(picture) = frames.get(camera.next[at]) else {
                continue;
            };
            camera.next[at] = (camera.next[at] + 1) % frames.len();
            let frame = tagged(picture, marker, layer);
            let Some(writer) = self.rtc.writer(self.camera) else {
                return;
            };
            let Some(pt) = writer.payload_params().next().map(|params| params.pt()) else {
                return;
            };
            let _ = writer.rid(Rid::from(layer.rid)).write(pt, now, time, frame);
        }
    }
}

impl Client {
    /// One share frame, without a rid.
    fn send_share(&mut self, marker: u8, share: &mut Share, shared: &Shared, now: Instant) {
        let time = MediaTime::new(share.sent * TICKS_PER_SHARE_FRAME, Frequency::NINETY_KHZ);
        share.sent += 1;
        if share.restart {
            share.restart = false;
            share.next = 0;
        }
        let Some(picture) = share.clip.get(share.next) else {
            return;
        };
        share.next = (share.next + 1) % share.clip.len();
        let kbps = shared.share_kbps.load(Ordering::Acquire);
        let layer = Layer {
            bytes: share_bytes(kbps),
            ..SHARE
        };
        let frame = tagged(picture, marker, &layer);
        let Some(writer) = self.rtc.writer(self.share) else {
            return;
        };
        let Some(pt) = writer.payload_params().next().map(|params| params.pt()) else {
            return;
        };
        let _ = writer.write(pt, now, time, frame);
    }
}

/// Where the share is in its clip.
struct Share {
    on: bool,
    clip: Vec<Vec<u8>>,
    next: usize,
    restart: bool,
    sent: u64,
}

impl Default for Share {
    fn default() -> Self {
        Self {
            on: false,
            clip: frames(SHARE.clip),
            next: 0,
            restart: false,
            sent: 0,
        }
    }
}

/// Where each camera layer is in its clip.
struct Camera {
    on: bool,
    /// Each layer's clip, cut into frames.
    clips: [Vec<Vec<u8>>; 2],
    next: [usize; 2],
    /// Start the layer over at its keyframe before its next frame.
    restart: [bool; 2],
    sent: u64,
}

impl Default for Camera {
    fn default() -> Self {
        Self {
            on: false,
            clips: LAYERS.map(|layer| frames(layer.clip)),
            next: [0; 2],
            restart: [false; 2],
            sent: 0,
        }
    }
}

fn accept(
    rtc: &mut Rtc,
    pending: &mut Option<SdpPendingOffer>,
    answer: &str,
) -> Result<(), String> {
    let answer = SdpAnswer::from_sdp_string(answer).map_err(|error| format!("{error:?}"))?;
    let pending = pending.take().ok_or("no offer pending")?;
    rtc.sdp_api()
        .accept_answer(pending, answer)
        .map_err(|error| format!("{error:?}"))
}

/// Cuts an H.264 Annex B stream into frames: each slice, with the parameter sets before it.
fn frames(clip: &[u8]) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    let mut frame = Vec::new();
    for unit in units(clip) {
        frame.extend_from_slice(&[0, 0, 0, 1]);
        frame.extend_from_slice(unit);
        // NAL types 1 and 5 are slices: a P frame and a keyframe.
        if matches!(unit.first().map(|header| header & 0x1f), Some(1 | 5)) {
            frames.push(std::mem::take(&mut frame));
        }
    }
    frames
}

/// The NAL units of an Annex B stream, without their start codes.
fn units(stream: &[u8]) -> Vec<&[u8]> {
    let mut starts = Vec::new();
    let mut at = 0;
    while let Some(found) = stream
        .get(at..)
        .and_then(|rest| rest.windows(3).position(|window| window == [0, 0, 1]))
    {
        starts.push(at + found + 3);
        at += found + 3;
    }
    starts
        .iter()
        .enumerate()
        .filter_map(|(n, &start)| {
            let end = match starts.get(n + 1) {
                // The next start code, and a fourth zero before it if there is one.
                Some(&next) => {
                    let end = next - 3;
                    if end > start && stream.get(end - 1) == Some(&0) {
                        end - 1
                    } else {
                        end
                    }
                }
                None => stream.len(),
            };
            stream.get(start..end)
        })
        .collect()
}

/// `picture` with an SEI tag in front: user data unregistered (payload type 5) holding
/// [`TAG`], the marker, the layer's rid, and padding up to the layer's frame size.
fn tagged(picture: &[u8], marker: u8, layer: &Layer) -> Vec<u8> {
    // Start code, NAL header, payload type, and trailing bits around the payload and its size.
    let room = layer.bytes.saturating_sub(picture.len() + 4 + 1 + 1 + 1);
    // The size takes one byte, and one more for each 255 of it.
    let length = room.saturating_sub(room / 256 + 1).max(TAG.len() + 2);
    let mut payload = Vec::with_capacity(length);
    payload.extend_from_slice(TAG);
    payload.push(marker);
    payload.push(layer.rid.as_bytes().first().copied().unwrap_or(b'h'));
    // No zero bytes, so no emulation prevention.
    payload.resize(payload.capacity(), 0xaa);
    let mut frame = Vec::with_capacity(layer.bytes + 8);
    frame.extend_from_slice(&[0, 0, 0, 1, 0x06, 0x05]);
    let mut size = payload.len();
    while size >= 255 {
        frame.push(0xff);
        size -= 255;
    }
    frame.push(u8::try_from(size).unwrap_or(0));
    frame.extend_from_slice(&payload);
    frame.push(0x80);
    frame.extend_from_slice(picture);
    frame
}

/// The marker and layer from a frame's SEI tag.
fn tag(frame: &[u8]) -> Option<(u8, u8)> {
    let at = frame.windows(TAG.len()).position(|window| window == TAG)?;
    let marker = *frame.get(at + TAG.len())?;
    let layer = *frame.get(at + TAG.len() + 1)?;
    Some((marker, layer))
}

/// A packet read and not yet handed to str0m.
type Packet = (Protocol, SocketAddr, Vec<u8>);

/// A packet as str0m input, with `local` as its destination.
fn input(
    proto: Protocol,
    source: SocketAddr,
    local: SocketAddr,
    packet: &[u8],
) -> Option<Input<'_>> {
    let contents = packet.try_into().ok()?;
    Some(Input::Receive(
        Instant::now(),
        Receive {
            proto,
            source,
            destination: local,
            contents,
        },
    ))
}

struct Shared {
    stop: AtomicBool,
    connected: AtomicBool,
    sending: AtomicBool,
    /// The REMB to send on the video slot, in kb/s. 0 sends none.
    remb: AtomicU64,
    keyframes_asked: AtomicU64,
    share_keyframes_asked: AtomicU64,
    share_kbps: AtomicU64,
    frames: Mutex<Vec<Frame>>,
    video: Mutex<Vec<VideoFrame>>,
    /// Sent as every audio frame in place of the marker frame, while set.
    sealed: Mutex<Option<Vec<u8>>>,
    /// Every audio frame received, byte for byte, with its m-line.
    payloads: Mutex<Vec<(String, Vec<u8>)>>,
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            stop: AtomicBool::default(),
            connected: AtomicBool::default(),
            sending: AtomicBool::default(),
            remb: AtomicU64::default(),
            keyframes_asked: AtomicU64::default(),
            share_keyframes_asked: AtomicU64::default(),
            share_kbps: AtomicU64::new(SHARE_KBPS),
            frames: Mutex::default(),
            video: Mutex::default(),
            sealed: Mutex::default(),
            payloads: Mutex::default(),
        }
    }
}

/// A client in a call. Dropping it stops it.
pub struct Running {
    shared: Arc<Shared>,
    requests: SyncSender<Request>,
    waker: Arc<Waker>,
    thread: Option<JoinHandle<()>>,
}

impl Running {
    /// ICE and DTLS are up.
    pub fn connected(&self) -> bool {
        self.shared.connected.load(Ordering::Acquire)
    }

    /// Starts or stops the microphone. It starts stopped.
    pub fn sending(&self, on: bool) {
        self.shared.sending.store(on, Ordering::Release);
    }

    /// Sends `payload` as every audio frame from now on, byte for byte, as an app that
    /// encrypts end to end would; `None` goes back to the marker frames.
    pub fn send_payload(&self, payload: Option<Vec<u8>>) {
        *lock(&self.shared.sealed) = payload;
    }

    /// Audio payloads received since the last call, byte for byte, with their m-line.
    pub fn take_payloads(&self) -> Vec<(String, Vec<u8>)> {
        std::mem::take(&mut *lock(&self.shared.payloads))
    }

    /// Audio frames received since the last call.
    pub fn take(&self) -> Vec<Frame> {
        std::mem::take(&mut *lock(&self.shared.frames))
    }

    /// Camera and share frames received since the last call, in order.
    pub fn take_video(&self) -> Vec<VideoFrame> {
        std::mem::take(&mut *lock(&self.shared.video))
    }

    /// Keyframe requests the SFU has sent for the camera.
    pub fn keyframes_asked(&self) -> u64 {
        self.shared.keyframes_asked.load(Ordering::Acquire)
    }

    /// Keyframe requests the SFU has sent for the share.
    pub fn share_keyframes_asked(&self) -> u64 {
        self.shared.share_keyframes_asked.load(Ordering::Acquire)
    }

    /// The offer for `call.offer` that turns the camera on or off. The camera sends once
    /// its answer is accepted.
    pub fn camera(&self, on: bool) -> Result<String, String> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.ask(Request::Flip(false, on, reply))?;
        answer.recv().map_err(|_| "client stopped".to_string())?
    }

    /// The offer for `call.offer` that starts or stops the share. The share sends once its
    /// answer is accepted.
    pub fn share(&self, on: bool) -> Result<String, String> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.ask(Request::Flip(true, on, reply))?;
        answer.recv().map_err(|_| "client stopped".to_string())?
    }

    /// Pads share frames to `kbps` from the next one on. It starts at 2,500.
    pub fn share_kbps(&self, kbps: u64) {
        self.shared.share_kbps.store(kbps, Ordering::Release);
    }

    /// Undoes the last [`Running::camera`] or [`Running::share`] offer, which the SFU refused,
    /// as with `share_taken`.
    pub fn reject(&self) -> Result<(), String> {
        let (reply, done) = mpsc::sync_channel(1);
        self.ask(Request::Reject(reply))?;
        done.recv().map_err(|_| "client stopped".to_string())
    }

    /// Takes the `call.answer` for the last [`Running::camera`] or [`Running::share`] offer.
    pub fn accept(&self, answer: &str) -> Result<(), String> {
        let (reply, done) = mpsc::sync_channel(1);
        self.ask(Request::Answer(answer.to_string(), reply))?;
        done.recv().map_err(|_| "client stopped".to_string())?
    }

    /// Sends a REMB of `kbps` on the video slot every second, as a receiver on a slow link
    /// would. `None` stops.
    pub fn remb(&self, kbps: Option<u64>) {
        self.shared
            .remb
            .store(kbps.unwrap_or_default(), Ordering::Release);
    }

    /// Stops the thread and closes the socket, as a killed process would. Nothing is sent.
    pub fn stop(mut self) {
        self.halt();
    }

    fn ask(&self, request: Request) -> Result<(), String> {
        self.requests
            .send(request)
            .map_err(|_| "client stopped".to_string())?;
        self.waker.wake().map_err(|error| error.to_string())
    }

    fn halt(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        let _ = self.waker.wake();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.halt();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{frames, tag, tagged, units, LAYERS};

    #[test]
    fn each_clip_is_one_keyframe_then_p_frames() {
        for layer in &LAYERS {
            let clip = frames(layer.clip);
            assert_eq!(clip.len(), 300, "{}", layer.rid);
            let types = |frame: &[u8]| -> Vec<u8> {
                units(frame)
                    .iter()
                    .filter_map(|unit| unit.first().map(|header| header & 0x1f))
                    .collect()
            };
            // Parameter sets, then the IDR slice.
            assert_eq!(types(&clip[0]), vec![7, 8, 5]);
            assert!(clip[1..].iter().all(|frame| types(frame) == vec![1]));
        }
    }

    #[test]
    fn a_tagged_frame_is_padded_and_says_who_sent_it() {
        let layer = &LAYERS[1];
        let clip = frames(layer.clip);
        let frame = tagged(&clip[3], b'A', layer);
        assert_eq!(frame.len(), layer.bytes);
        assert_eq!(tag(&frame), Some((b'A', b'l')));
        let types: Vec<u8> = units(&frame)
            .iter()
            .filter_map(|unit| unit.first().map(|header| header & 0x1f))
            .collect();
        assert_eq!(types, vec![6, 1]);
    }
}
