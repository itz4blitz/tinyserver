//! Media plane. One OS thread owns one UDP socket, an ICE-TCP listener on the same port
//! number, and one `str0m::Rtc` per participant. It forwards Opus audio and H.264 video to
//! each listener's one video slot: the room's screen share while there is one, else one
//! simulcast layer of one camera.
//!
//! The API sends [`Media`] commands and reads [`Event`]s. str0m types stay in this crate.
//! RTP stays in memory. Postgres never stores it.

mod engine;
mod limit;
mod room;
mod sdp;
mod tcp;
mod video;

#[cfg(feature = "headless")]
pub mod client;

#[cfg(feature = "fuzz")]
pub mod fuzz;

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc as async_mpsc, oneshot};

pub use video::Quality;

static PEER_IPS: AtomicBool = AtomicBool::new(false);

/// When set, debug media lines may include a client address. The operator sink still drops it.
pub fn set_peer_ips(on: bool) {
    PEER_IPS.store(on, Ordering::Relaxed);
}

pub fn peer_ips_enabled() -> bool {
    PEER_IPS.load(Ordering::Relaxed)
}

/// A stable name for a media error. The SDP text inside `RemoteSdp` is not returned.
pub fn rtc_reason(error: &str0m::RtcError) -> &'static str {
    match error {
        str0m::RtcError::NoSenderSource => "no_sender",
        str0m::RtcError::WriteWithoutPoll => "write_without_poll",
        str0m::RtcError::Dtls(_) => "dtls",
        str0m::RtcError::Ice(_) => "ice",
        str0m::RtcError::RemoteSdp(_) | str0m::RtcError::Sdp(_) => "remote_sdp",
        str0m::RtcError::Io(_) | str0m::RtcError::Net(_) => "io",
        _ => "other",
    }
}

#[cfg(test)]
mod rtc_reason_tests {
    use super::rtc_reason;

    #[test]
    fn remote_sdp_is_a_reason_not_the_sdp() {
        let reason = rtc_reason(&str0m::RtcError::RemoteSdp("a=ice-pwd:secret".to_string()));
        assert_eq!(reason, "remote_sdp");
        assert!(!reason.contains("ice-pwd"));
    }
}

/// Commands waiting for the media thread. A full queue refuses the next one.
const COMMANDS: usize = 1024;
/// Events waiting for the API. A full queue drops `Speaking` and `Load`, and the media thread
/// keeps the rest until there is room.
const EVENTS: usize = 1024;

/// Where the media thread listens, what it advertises, and its limits.
#[derive(Debug, Clone)]
pub struct Config {
    /// The UDP socket, and the TCP listener on the same port. Port 0 picks one; tests do that.
    pub bind: SocketAddr,
    /// The address members reach. The advertised port is the one bound.
    pub public_ip: IpAddr,
    /// A second address for members on the same network, whose router may not hairpin.
    pub lan_ip: Option<IpAddr>,
    /// A participant whose ICE and DTLS are not up by then is dropped.
    pub connect_timeout: Duration,
    /// A connected participant silent for this long is dropped. No packet at all, not even
    /// a STUN consent check. A client's own ICE gives up in 4 to 7 s and it joins again with a
    /// fresh offer, so this only clears away what is really gone.
    pub consent_timeout: Duration,
    /// Audio a publisher may send, in kb/s of payload, with a burst of two seconds of it.
    /// Frames over it are not forwarded. Clients encode at the room's bitrate, up to 128 kb/s;
    /// this leaves room for an encrypted frame's seal and Opus running over in loud passages,
    /// so it only ever stops abuse.
    pub audio_kbps: u32,
    /// Camera video a publisher may send on each simulcast layer, in kb/s of payload, from a
    /// bucket of one second. A frame over it is dropped, and so is that layer until its next
    /// keyframe that fits.
    pub camera_high_kbps: u32,
    pub camera_low_kbps: u32,
    /// The same for a screen share's one layer.
    pub share_kbps: u32,
    /// What the server may upload to everyone, in kb/s. `None` is no budget. Shares are
    /// planned first; over it, camera listeners drop to `l`, then to audio only.
    pub egress_kbps: Option<u32>,
    /// Where each listener's TWCC bandwidth estimate starts, in kb/s. Below 1,600 a listener
    /// starts on `l` and moves up as the estimate climbs. `None` turns the estimator off;
    /// layers then follow only the client's REMB, `call.quality`, and the upload budget.
    pub bwe_start_kbps: Option<u32>,
    /// How long a listener's link must hold before their video moves up a step, and the first
    /// wait before what a full link took is tried again. Moves down are at once.
    pub layer_hold: Duration,
    /// Packets one source address may send a second, and the burst above that.
    pub packets_per_second: u32,
    pub packet_burst: u32,
    /// TCP streams open at once, from one source address, and not yet past their first STUN.
    /// A stream over any of them is closed as soon as it is accepted.
    pub tcp_streams: usize,
    pub tcp_per_source: usize,
    pub tcp_unverified: usize,
    /// A TCP stream with no STUN for a participant by then is closed.
    pub tcp_handshake: Duration,
    /// A TCP stream that sends nothing for this long is closed. Half-open ones end this way.
    pub tcp_idle: Duration,
    /// The largest offer, in bytes.
    pub offer_bytes: usize,
    /// Audio and video m-lines an offer may have. Audio is the room cap: one microphone and
    /// a slot for everyone else. Other kinds are refused.
    pub offer_audio: usize,
    pub offer_video: usize,
}

impl Config {
    /// The defaults in `docs/calls.md`, on `bind`, advertising `public_ip`.
    pub fn new(bind: SocketAddr, public_ip: IpAddr) -> Self {
        Self {
            bind,
            public_ip,
            lan_ip: None,
            connect_timeout: Duration::from_secs(15),
            consent_timeout: Duration::from_secs(15),
            audio_kbps: 192,
            camera_high_kbps: 2400,
            camera_low_kbps: 700,
            share_kbps: 5000,
            egress_kbps: None,
            bwe_start_kbps: Some(1000),
            layer_hold: Duration::from_secs(5),
            packets_per_second: 2000,
            packet_burst: 4000,
            tcp_streams: 256,
            tcp_per_source: 16,
            tcp_unverified: 64,
            tcp_handshake: Duration::from_secs(5),
            tcp_idle: Duration::from_secs(30),
            offer_bytes: 32 * 1024,
            offer_audio: 12,
            offer_video: 3,
        }
    }
}

/// The SFU's reply to a join or an offer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// Names this participant until they leave. A rejoin gets a new one.
    pub call_id: String,
    pub sdp: String,
    /// The offer's camera m-line is sending.
    pub camera: bool,
    /// This participant holds the room's share.
    pub share: bool,
}

/// Why the SFU refused a join or an offer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// Not an SDP offer str0m accepts, or over the offer caps.
    BadSdp,
    /// No such participant. For an offer only.
    NotFound,
    /// The offer starts a share, and someone else in the room holds it.
    ShareTaken,
    /// The media thread is gone or its queue is full.
    Unavailable,
}

/// Why the SFU dropped a participant on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lost {
    /// ICE and DTLS were not up within `connect_timeout`.
    Timeout,
    /// No packet for `consent_timeout`, a DTLS failure, or ICE gave up.
    Gone,
}

/// One receive slot: the m-line `mid` carries `user_id`'s audio.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slot {
    pub mid: String,
    pub user_id: String,
}

/// What a listener's one video slot carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoKind {
    Camera,
    Share,
}

impl VideoKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Camera => "camera",
            Self::Share => "share",
        }
    }
}

/// The video receive m-line `mid` carries `user_id`'s `kind`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoSlot {
    pub mid: String,
    pub user_id: String,
    pub kind: VideoKind,
}

/// What the media thread tells the API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// ICE and DTLS are up.
    Connected { call_id: String },
    /// The whole slot map for one listener. Sent on every change. `video` changes on the
    /// keyframe that starts a new source, so frames after it decode on their own.
    Slots {
        call_id: String,
        audio: Vec<Slot>,
        video: Option<VideoSlot>,
    },
    /// Who is speaking in a room. Sent when it changes, at most 4 times a second.
    Speaking {
        room_id: String,
        user_ids: Vec<String>,
    },
    /// The SFU dropped a participant. A `leave` from the API sends nothing.
    Left { call_id: String, lost: Lost },
    /// Every 5 s.
    Load {
        egress_kbps: u64,
        participants: usize,
    },
}

pub(crate) type Reply = oneshot::Sender<Result<Answer, Refused>>;

pub(crate) enum Command {
    Join {
        room_id: String,
        user_id: String,
        offer: String,
        reply: Reply,
    },
    Offer {
        call_id: String,
        offer: String,
        reply: Reply,
    },
    State {
        call_id: String,
        muted: bool,
        deafened: bool,
    },
    Moderate {
        call_id: String,
        silenced: bool,
        hushed: bool,
    },
    Quality {
        call_id: String,
        video: Quality,
    },
    EndShare {
        call_id: String,
    },
    Leave {
        call_id: String,
    },
    Shutdown,
}

/// A handle on the media thread. Cheap to clone.
#[derive(Clone)]
pub struct Media {
    commands: SyncSender<Command>,
    waker: Arc<mio::Waker>,
    alive: Arc<AtomicBool>,
    addr: SocketAddr,
    lan: bool,
    offers: sdp::Caps,
}

/// Binds the UDP socket and the TCP listener and starts the media thread.
pub fn start(config: Config) -> io::Result<(Media, async_mpsc::Receiver<Event>)> {
    let (commands, inbox) = mpsc::sync_channel(COMMANDS);
    let (events, outbox) = async_mpsc::channel(EVENTS);
    let alive = Arc::new(AtomicBool::new(true));
    let (engine, waker) = engine::Engine::bind(config.clone(), inbox, events, alive.clone())?;
    let addr = engine.advertised();
    let lan = config.lan_ip.is_some();
    let offers = sdp::Caps::from(&config);
    std::thread::Builder::new()
        .name("sfu".to_string())
        .spawn(move || engine.run())?;
    let media = Media {
        commands,
        waker: Arc::new(waker),
        alive,
        addr,
        lan,
        offers,
    };
    Ok((media, outbox))
}

impl Media {
    /// The public address and port in every answer's candidates, UDP and TCP.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The transports members can reach while the thread runs: `udp`, `tcp`, and `lan` when
    /// a LAN address is advertised too. Empty once it stopped.
    pub fn transports(&self) -> Vec<&'static str> {
        if !self.running() {
            return Vec::new();
        }
        let mut up = vec!["udp", "tcp"];
        if self.lan {
            up.push("lan");
        }
        up
    }

    /// False once the media thread has stopped.
    pub fn running(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    /// Adds `user_id` to `room_id` with `offer`, replacing any participant they already are.
    pub async fn join(&self, room_id: &str, user_id: &str, offer: &str) -> Result<Answer, Refused> {
        self.offers.check(offer)?;
        let (reply, answer) = oneshot::channel();
        self.send(Command::Join {
            room_id: room_id.to_string(),
            user_id: user_id.to_string(),
            offer: offer.to_string(),
            reply,
        })?;
        answer.await.unwrap_or(Err(Refused::Unavailable))
    }

    /// Renegotiates with a new offer from the same participant.
    pub async fn offer(&self, call_id: &str, offer: &str) -> Result<Answer, Refused> {
        self.offers.check(offer)?;
        let (reply, answer) = oneshot::channel();
        self.send(Command::Offer {
            call_id: call_id.to_string(),
            offer: offer.to_string(),
            reply,
        })?;
        answer.await.unwrap_or(Err(Refused::Unavailable))
    }

    /// A muted publisher is not forwarded. A deafened listener is not sent anything.
    pub fn state(&self, call_id: &str, muted: bool, deafened: bool) {
        let _ = self.send(Command::State {
            call_id: call_id.to_string(),
            muted,
            deafened,
        });
    }

    /// Moderation, apart from the participant's own mute and deafen. A `silenced` publisher's
    /// audio is not forwarded, for a server mute or a room where they may not speak. A
    /// `hushed` listener is sent no audio, for a server deafen; video still flows both ways.
    pub fn moderate(&self, call_id: &str, silenced: bool, hushed: bool) {
        let _ = self.send(Command::Moderate {
            call_id: call_id.to_string(),
            silenced,
            hushed,
        });
    }

    /// What a listener's video slot may carry, from `call.quality`. It starts at `High`.
    pub fn quality(&self, call_id: &str, video: Quality) {
        let _ = self.send(Command::Quality {
            call_id: call_id.to_string(),
            video,
        });
    }

    /// Stops forwarding a participant's share. It stays stopped until an offer sets the
    /// share m-line inactive and a later one starts it again.
    pub fn end_share(&self, call_id: &str) {
        let _ = self.send(Command::EndShare {
            call_id: call_id.to_string(),
        });
    }

    /// Drops a participant. No `Left` follows.
    pub fn leave(&self, call_id: &str) {
        let _ = self.send(Command::Leave {
            call_id: call_id.to_string(),
        });
    }

    /// Ends every call and stops the thread.
    pub fn shutdown(&self) {
        let _ = self.send(Command::Shutdown);
    }

    fn send(&self, command: Command) -> Result<(), Refused> {
        match self.commands.try_send(command) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                tracing::warn!("media command queue full");
                return Err(Refused::Unavailable);
            }
            Err(TrySendError::Disconnected(_)) => return Err(Refused::Unavailable),
        }
        if let Err(error) = self.waker.wake() {
            tracing::warn!(%error, "media thread wake failed");
        }
        Ok(())
    }
}
