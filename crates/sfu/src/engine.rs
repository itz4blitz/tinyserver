//! The media thread: one `mio` loop over the UDP socket, the ICE-TCP listener and its
//! streams, and a waker for commands.
//!
//! Every `Rtc` is driven the way str0m requires: one input, then `poll_output` until it
//! returns a timeout. Audio frames are copied from each publisher to everyone else in the
//! room, on the receive slot that listener keeps for them. Video goes to each listener's one
//! video slot: the room's share while there is one, else one layer of one publisher's camera,
//! switched on a keyframe.

use std::collections::hash_map::RandomState;
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mio::event::Event as Ready;
use mio::net::{TcpListener, TcpStream, UdpSocket};
use mio::{Events, Interest, Poll, Registry, Token, Waker};
use socket2::SockRef;
use str0m::bwe::{Bitrate, BweKind};
use str0m::change::SdpOffer;
use str0m::config::{CryptoProvider, DtlsCert};
use str0m::media::{Direction, Frequency, KeyframeRequestKind, MediaData, MediaKind, MediaTime};
use str0m::media::{Mid, Pt, Rid};
use str0m::net::{DatagramRecv, Protocol, Receive, TcpType};
use str0m::{Candidate, Event as RtcEvent, Input, Output, Rtc, RtcConfig};
use tokio::sync::mpsc as async_mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::limit::Bucket;
use crate::room::{Announced, Slots, Speech};
use crate::sdp;
use crate::tcp::{Conn, Readout};
use crate::video::{self, Ceiling, Fit, Floor, Layer, Link, Quality, Rebase};
use crate::{Answer, Command, Config, Event, Lost, Refused, Reply, Slot, VideoKind, VideoSlot};

const SOCKET: Token = Token(0);
const WAKE: Token = Token(1);
const LISTENER: Token = Token(2);
/// TCP streams take tokens from here up.
const FIRST_STREAM: usize = 3;
/// Datagrams read from the socket per turn, so TCP streams, commands and timeouts get a say.
const READ_BATCH: usize = 64;
/// Bytes read from one TCP stream per turn of the loop, so one cannot starve the rest.
const STREAM_READ: usize = 64 * 1024;
/// Port 0 picks a UDP port, and TCP may already hold that number, or on Windows have reserved
/// a run of ports around it. Tries before giving up.
const BIND_TRIES: usize = 256;
/// The loop wakes at least this often.
const TICK: Duration = Duration::from_millis(50);
/// What the UDP socket asks the kernel to buffer each way. Enough for a burst of keyframes to
/// everyone without a drop. Linux silently caps it at `net.core.rmem_max` and `wmem_max`.
const SOCKET_BUFFER: usize = 4 * 1024 * 1024;
/// What IP, UDP, RTP and SRTP add to a forwarded audio frame, in bytes.
const AUDIO_HEADERS: u64 = 60;
/// Larger than any datagram str0m sends or accepts.
const DATAGRAM: usize = 2000;
/// Source addresses with a packet budget. A new source past this is dropped.
const SOURCES: usize = 4096;
/// A source silent this long loses its budget entry.
const SOURCE_IDLE: Duration = Duration::from_secs(10);
const LOAD_EVERY: Duration = Duration::from_secs(5);
/// Seconds of `audio_kbps` a publisher may run ahead of it, for a burst after a stall.
const AUDIO_BURST: u32 = 2;
/// Video slots are planned again at least this often.
const PLAN_EVERY: Duration = Duration::from_millis(50);
/// Who is speaking and which TCP streams expired are looked at this often. They scan every
/// room and stream, and the loop turns thousands of times a second under load.
const SCAN_EVERY: Duration = Duration::from_millis(50);

type PeerId = u64;
/// One layer of one publisher's camera, or their share.
type Source = (PeerId, Layer);

/// One simulcast layer of a publisher's camera, or their share, as it arrives.
struct Inbound {
    /// A frame has arrived since the camera started, with this rid.
    arrived: bool,
    rid: Option<Rid>,
    budget: Bucket,
    /// The last frame within budget. A layer dropped over budget goes stale.
    seen: Option<Instant>,
    /// A frame was dropped over budget. Nothing more is forwarded until a keyframe fits.
    broken: bool,
    asked: Option<Instant>,
    /// The last keyframe that arrived, forwarded or not.
    keyframe: Option<Instant>,
}

impl Inbound {
    fn new(kbps: u32, now: Instant) -> Self {
        let rate = kbps.saturating_mul(1000) / 8;
        Self {
            arrived: false,
            rid: None,
            budget: Bucket::new(rate, rate, now),
            seen: None,
            broken: false,
            asked: None,
            keyframe: None,
        }
    }

    /// Forgets what arrived, so a restarted camera or share waits for fresh frames.
    fn restart(&mut self) {
        self.arrived = false;
        self.seen = None;
        self.broken = false;
    }

    fn fresh(&self, now: Instant) -> bool {
        self.seen
            .is_some_and(|at| now.saturating_duration_since(at) < video::STALE)
    }

    /// Whether a frame of `size` bytes goes on. Over budget the layer breaks. A broken layer
    /// spends nothing on frames that cannot pass, so the budget fills for the next keyframe.
    fn admit(&mut self, keyframe: bool, size: u32, now: Instant) -> bool {
        if keyframe {
            self.keyframe = Some(now);
        }
        if self.broken && !keyframe {
            return false;
        }
        let fits = self.budget.take(size, now);
        self.broken = !fits;
        if fits {
            self.seen = Some(now);
        }
        fits
    }

    /// Whether a keyframe request may go out now. Not right after a keyframe, which the asker
    /// is about to get, and less often while the layer is broken.
    fn may_ask(&self, now: Instant) -> bool {
        let every = if self.broken {
            video::KEYFRAME_BROKEN_EVERY
        } else {
            video::KEYFRAME_EVERY
        };
        let since =
            |at: Option<Instant>, gap| at.is_some_and(|at| now.saturating_duration_since(at) < gap);
        self.arrived && !since(self.asked, every) && !since(self.keyframe, video::KEYFRAME_EVERY)
    }
}

/// A listener's one video receive m-line.
struct Screen {
    mid: Mid,
    /// What it carries now.
    showing: Option<Source>,
    /// What it should carry. It switches on that source's next keyframe.
    wanted: Option<Source>,
    rebase: Rebase<Source>,
    /// The bitrate the estimator was last told to probe for, in kb/s.
    probing: Option<u64>,
    /// What this listener's link has room for.
    link: Link,
}

struct Peer {
    call_id: String,
    room_id: String,
    user_id: String,
    rtc: Rtc,
    /// The client's microphone m-line.
    publish: Option<Mid>,
    /// The payload type audio goes to them on, for putting it first on a TCP stream.
    audio_pt: Option<Pt>,
    slots: Slots<Mid, PeerId>,
    /// The last slot map sent for this listener.
    told: Vec<Slot>,
    connected: bool,
    joined_at: Instant,
    heard_at: Instant,
    muted: bool,
    deafened: bool,
    /// Server muted, or without speak in the room: their audio is not forwarded.
    silenced: bool,
    /// Server deafened: they are sent no audio.
    hushed: bool,
    speech: Speech,
    audio: Bucket,
    /// The client's camera m-line: the first video m-line it may send on.
    camera: Option<Mid>,
    /// The camera m-line is sending, as the last offer said.
    camera_on: bool,
    /// The client's share m-line: the second video m-line it may send on.
    share: Option<Mid>,
    /// The share m-line is sending, as the last offer said.
    share_on: bool,
    /// This participant holds the room's share. It ends when the m-line stops or an owner or
    /// admin ends it, and an ended share waits for the m-line to stop and start again.
    sharing: bool,
    /// The camera's layers, `h` then `l`, then the share.
    layers: [Inbound; 3],
    screen: Option<Screen>,
    quality: Quality,
    /// The latest bandwidth estimates for what the SFU sends this listener, in kb/s: str0m's
    /// TWCC estimator, and the last REMB the client sent.
    twcc: Option<u64>,
    remb: Option<u64>,
    /// Audio sent to this listener since it was last measured, with headers, in bytes.
    audio_sent: u64,
    /// That audio as a rate, in kb/s, measured every second. It comes off the estimate.
    audio_kbps: u64,
    told_video: Option<VideoSlot>,
    timeout: Instant,
    lost: Option<Lost>,
}

impl Peer {
    /// Their audio is forwarded: not muted by themselves or by moderation.
    fn audible(&self) -> bool {
        !self.muted && !self.silenced
    }

    fn camera_live(&self) -> bool {
        self.connected && self.camera_on
    }

    fn share_live(&self) -> bool {
        self.connected && self.sharing
    }

    /// Whether this publisher still sends `layer`.
    fn sends(&self, layer: Layer) -> bool {
        match layer {
            Layer::Share => self.share_live(),
            Layer::High | Layer::Low => self.camera_live(),
        }
    }

    fn estimate(&self) -> Option<u64> {
        match (self.twcc, self.remb) {
            (Some(twcc), Some(remb)) => Some(twcc.min(remb)),
            (twcc, remb) => twcc.or(remb),
        }
    }

    /// The estimate less the audio this listener is getting, in kb/s.
    fn room_for_video(&self) -> Option<u64> {
        let estimate = self.estimate()?;
        Some(estimate.saturating_sub(self.audio_kbps))
    }
}

struct Room {
    /// In join order.
    members: Vec<PeerId>,
    announced: Announced,
    floor: Floor<PeerId>,
    /// Who holds the room's one share.
    sharer: Option<PeerId>,
}

impl Default for Room {
    fn default() -> Self {
        Self {
            members: Vec::new(),
            announced: Announced::default(),
            floor: Floor::new(),
            sharer: None,
        }
    }
}

/// One accepted TCP stream.
struct Stream {
    conn: Conn,
    opened: Instant,
    /// The last whole frame.
    heard: Instant,
    /// The participant whose STUN it carried first. Unverified until then.
    owner: Option<PeerId>,
    /// Registered for writable too, because its backlog did not drain.
    writing: bool,
    closed: bool,
}

/// The ICE-TCP streams, by token and by the client's address.
struct Tcp {
    registry: Registry,
    streams: HashMap<Token, Stream>,
    by_peer: HashMap<SocketAddr, Token>,
    next: usize,
    /// Streams that stopped at their read budget with bytes still waiting.
    unread: Vec<Token>,
}

impl Tcp {
    /// Frames `packet` onto the stream from `to`, ahead of waiting video if `urgent`. Dropped
    /// if there is none or it is backed up, and then false.
    fn send(&mut self, to: SocketAddr, packet: &[u8], urgent: bool) -> bool {
        let Some(token) = self.by_peer.get(&to).copied() else {
            return false;
        };
        let queued = self
            .streams
            .get_mut(&token)
            .is_some_and(|stream| stream.conn.queue(packet, urgent));
        if queued {
            self.flush(token);
        }
        queued
    }

    /// Writes the backlog, and watches for writable only while some is left.
    fn flush(&mut self, token: Token) {
        let Some(stream) = self.streams.get_mut(&token) else {
            return;
        };
        let done = match stream.conn.flush() {
            Ok(done) => done,
            Err(_) => {
                stream.closed = true;
                return;
            }
        };
        if done == !stream.writing {
            return;
        }
        let interest = if done {
            Interest::READABLE
        } else {
            Interest::READABLE | Interest::WRITABLE
        };
        match self
            .registry
            .reregister(&mut stream.conn.socket, token, interest)
        {
            Ok(()) => stream.writing = !done,
            Err(_) => stream.closed = true,
        }
    }

    fn close(&mut self, token: Token) -> Option<Stream> {
        let mut stream = self.streams.remove(&token)?;
        let _ = self.registry.deregister(&mut stream.conn.socket);
        if self.by_peer.get(&stream.conn.peer) == Some(&token) {
            self.by_peer.remove(&stream.conn.peer);
        }
        Some(stream)
    }
}

/// Events on their way to the API. The media thread must not wait for it, so what the queue
/// cannot take now stays here in order, and `tick` tries again. These come from joins, leaves,
/// and slot changes, which the API already limits, so they stay few if it stalls.
struct Outbox {
    sender: async_mpsc::Sender<Event>,
    waiting: VecDeque<Event>,
}

impl Outbox {
    fn new(sender: async_mpsc::Sender<Event>) -> Self {
        Self {
            sender,
            waiting: VecDeque::new(),
        }
    }

    /// An event the API must see.
    fn tell(&mut self, event: Event) {
        self.waiting.push_back(event);
        self.flush();
    }

    /// An event that may be dropped when the queue is full.
    fn hint(&self, event: Event) {
        let _ = self.sender.try_send(event);
    }

    /// Hands the API what it has room for.
    fn flush(&mut self) {
        while let Some(event) = self.waiting.pop_front() {
            match self.sender.try_send(event) {
                Ok(()) => {}
                Err(TrySendError::Full(event)) => {
                    self.waiting.push_front(event);
                    return;
                }
                Err(TrySendError::Closed(_)) => {
                    self.waiting.clear();
                    return;
                }
            }
        }
    }
}

/// Where a drained `Rtc` puts what it produced.
struct Sink<'a> {
    socket: &'a UdpSocket,
    tcp: &'a mut Tcp,
    events: &'a mut Outbox,
    egress: &'a mut u64,
    forward: &'a mut VecDeque<(PeerId, MediaData)>,
    /// Listeners who asked for a keyframe on their video slot.
    asks: &'a mut Vec<PeerId>,
    /// Participants with new receive m-lines.
    fresh: &'a mut Vec<PeerId>,
    counts: &'a mut Counts,
}

pub(crate) struct Engine {
    config: Config,
    poll: Poll,
    socket: UdpSocket,
    /// The last read stopped at `READ_BATCH` with datagrams perhaps still waiting.
    socket_unread: bool,
    listener: TcpListener,
    tcp: Tcp,
    public: SocketAddr,
    lan: Option<SocketAddr>,
    inbox: Receiver<Command>,
    events: Outbox,
    alive: Arc<AtomicBool>,
    crypto: Arc<CryptoProvider>,
    cert: DtlsCert,
    peers: HashMap<PeerId, Peer>,
    by_call: HashMap<String, PeerId>,
    by_addr: HashMap<(Protocol, SocketAddr), PeerId>,
    sources: HashMap<SocketAddr, Bucket>,
    rooms: HashMap<String, Room>,
    forward: VecDeque<(PeerId, MediaData)>,
    asks: Vec<PeerId>,
    /// Participants whose `Rtc` reported new receive m-lines since the slots were last filled.
    fresh: Vec<PeerId>,
    /// Something changed that moves video slots; plan them at the next tick.
    replan: bool,
    planned_at: Instant,
    next_id: u64,
    ids: RandomState,
    egress: u64,
    counts: Counts,
    load_at: Instant,
    swept_at: Instant,
    /// The last look at who is speaking and which streams expired.
    scanned_at: Instant,
}

/// What the media thread dropped or asked for since the last time it reported.
#[derive(Debug, Default)]
struct Counts {
    /// Audio frames over a publisher's budget.
    audio_dropped: u64,
    /// Video frames over a layer's budget, or waiting for a keyframe after one.
    video_dropped: u64,
    /// Keyframe requests sent to publishers.
    keyframe_asks: u64,
    /// Packets the socket or a TCP stream had no room for.
    send_dropped: u64,
}

/// Marks the thread stopped however it ends, a panic included.
struct Running(Arc<AtomicBool>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl Engine {
    pub fn bind(
        config: Config,
        inbox: Receiver<Command>,
        events: async_mpsc::Sender<Event>,
        alive: Arc<AtomicBool>,
    ) -> io::Result<(Self, Waker)> {
        let (mut socket, mut listener, port) = bind(config.bind)?;
        let poll = Poll::new()?;
        poll.registry()
            .register(&mut socket, SOCKET, Interest::READABLE)?;
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)?;
        let tcp = Tcp {
            registry: poll.registry().try_clone()?,
            streams: HashMap::new(),
            by_peer: HashMap::new(),
            next: FIRST_STREAM,
            unread: Vec::new(),
        };
        let waker = Waker::new(poll.registry(), WAKE)?;
        let crypto = Arc::new(str0m::crypto::from_feature_flags());
        let cert = crypto
            .dtls_provider
            .generate_certificate()
            .ok_or_else(|| io::Error::other("no DTLS certificate"))?;
        let now = Instant::now();
        let engine = Self {
            public: SocketAddr::new(config.public_ip, port),
            lan: config.lan_ip.map(|ip| SocketAddr::new(ip, port)),
            config,
            poll,
            socket,
            socket_unread: false,
            listener,
            tcp,
            inbox,
            events: Outbox::new(events),
            alive,
            crypto,
            cert,
            peers: HashMap::new(),
            by_call: HashMap::new(),
            by_addr: HashMap::new(),
            sources: HashMap::new(),
            rooms: HashMap::new(),
            forward: VecDeque::new(),
            asks: Vec::new(),
            fresh: Vec::new(),
            replan: false,
            planned_at: now,
            next_id: 0,
            ids: RandomState::new(),
            egress: 0,
            counts: Counts::default(),
            load_at: now,
            swept_at: now,
            scanned_at: now,
        };
        Ok((engine, waker))
    }

    pub fn advertised(&self) -> SocketAddr {
        self.public
    }

    pub fn run(mut self) {
        let _running = Running(self.alive.clone());
        tracing::info!(addr = %self.public, lan = ?self.lan, "media listening on UDP and TCP");
        let mut events = Events::with_capacity(256);
        let mut buf = vec![0; DATAGRAM];
        loop {
            let wait = if self.tcp.unread.is_empty() && !self.socket_unread {
                self.deadline()
                    .saturating_duration_since(Instant::now())
                    .min(TICK)
            } else {
                Duration::ZERO
            };
            if let Err(error) = self.poll.poll(&mut events, Some(wait)) {
                if error.kind() != ErrorKind::Interrupted {
                    tracing::error!(%error, "media poll failed");
                    return;
                }
            }
            if !self.commands() {
                tracing::info!("media stopped");
                return;
            }
            let ready: Vec<(Token, bool, bool)> = events.iter().map(readiness).collect();
            let mut unread = std::mem::take(&mut self.tcp.unread);
            let mut socket_ready = std::mem::take(&mut self.socket_unread);
            for (token, readable, writable) in ready {
                match token {
                    SOCKET => socket_ready = true,
                    WAKE => {}
                    LISTENER => self.accept(Instant::now(), &mut unread),
                    token => {
                        if writable {
                            self.tcp.flush(token);
                        }
                        if readable && !unread.contains(&token) {
                            unread.push(token);
                        }
                    }
                }
            }
            if socket_ready {
                self.socket_unread = self.read(&mut buf);
            }
            for token in unread {
                self.read_stream(token, Instant::now());
            }
            self.tick(Instant::now());
        }
    }

    /// Takes every waiting TCP connection, or closes it at once when it is over a cap.
    fn accept(&mut self, now: Instant, unread: &mut Vec<Token>) {
        loop {
            match self.listener.accept() {
                Ok((socket, peer)) => {
                    if let Some(token) = self.admit(socket, peer, now) {
                        // Bytes may have come before it was registered.
                        unread.push(token);
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => return,
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => {
                    tracing::warn!(%error, "media accept failed");
                    return;
                }
            }
        }
    }

    fn admit(&mut self, mut socket: TcpStream, peer: SocketAddr, now: Instant) -> Option<Token> {
        let streams = &self.tcp.streams;
        let from_source = streams
            .values()
            .filter(|stream| stream.conn.peer.ip() == peer.ip())
            .count();
        let unverified = streams
            .values()
            .filter(|stream| stream.owner.is_none())
            .count();
        if streams.len() >= self.config.tcp_streams
            || from_source >= self.config.tcp_per_source
            || unverified >= self.config.tcp_unverified
        {
            let reason = if streams.len() >= self.config.tcp_streams
                || unverified >= self.config.tcp_unverified
            {
                "budget"
            } else {
                "source_cap"
            };
            if crate::peer_ips_enabled() {
                tracing::debug!(%peer, reason, "media dropped");
            } else {
                tracing::debug!(reason, "media dropped");
            }
            return None;
        }
        let _ = socket.set_nodelay(true);
        let token = Token(self.tcp.next);
        self.tcp.next += 1;
        if let Err(error) = self
            .tcp
            .registry
            .register(&mut socket, token, Interest::READABLE)
        {
            tracing::warn!(%error, "media TCP register failed");
            return None;
        }
        self.tcp.by_peer.insert(peer, token);
        self.tcp.streams.insert(
            token,
            Stream {
                conn: Conn::new(socket, peer),
                opened: now,
                heard: now,
                owner: None,
                writing: false,
                closed: false,
            },
        );
        Some(token)
    }

    /// Reads one stream's frames up to its budget. An unverified stream must start with
    /// STUN; anything else closes it.
    fn read_stream(&mut self, token: Token, now: Instant) {
        let Some(stream) = self.tcp.streams.get_mut(&token) else {
            return;
        };
        let mut frames = Vec::new();
        let end = stream.conn.read(STREAM_READ, |frame| {
            frames.push(frame.to_vec());
            true
        });
        let peer = stream.conn.peer;
        if !frames.is_empty() {
            stream.heard = now;
        }
        for frame in frames {
            let owner = self.tcp.streams.get(&token).and_then(|stream| stream.owner);
            if owner.is_none() && !is_stun(&frame) {
                self.close_stream(token);
                return;
            }
            let taken = self.receive(&frame, Protocol::Tcp, peer, now);
            if let (None, Some(id)) = (owner, taken) {
                if let Some(stream) = self.tcp.streams.get_mut(&token) {
                    stream.owner = Some(id);
                }
            }
        }
        match end {
            Readout::Drained => {}
            Readout::More => self.tcp.unread.push(token),
            Readout::Closed => self.close_stream(token),
        }
    }

    fn close_stream(&mut self, token: Token) {
        if let Some(stream) = self.tcp.close(token) {
            self.by_addr.remove(&(Protocol::Tcp, stream.conn.peer));
            if crate::peer_ips_enabled() {
                tracing::debug!(peer = %stream.conn.peer, "media TCP stream closed");
            } else {
                tracing::debug!("media TCP stream closed");
            }
        }
    }

    /// The earliest `Rtc` timeout.
    fn deadline(&self) -> Instant {
        let later = Instant::now() + TICK;
        self.peers
            .values()
            .map(|peer| peer.timeout)
            .min()
            .map_or(later, |soonest| soonest.min(later))
    }

    /// Runs every queued command. False on `Shutdown` or once every handle is gone.
    fn commands(&mut self) -> bool {
        loop {
            let command = match self.inbox.try_recv() {
                Ok(command) => command,
                Err(TryRecvError::Empty) => return true,
                Err(TryRecvError::Disconnected) => return false,
            };
            let now = Instant::now();
            match command {
                Command::Join {
                    room_id,
                    user_id,
                    offer,
                    reply,
                } => self.join(room_id, user_id, &offer, reply, now),
                Command::Offer {
                    call_id,
                    offer,
                    reply,
                } => self.renegotiate(&call_id, &offer, reply),
                Command::State {
                    call_id,
                    muted,
                    deafened,
                } => {
                    if let Some(peer) = self.peer_mut(&call_id) {
                        peer.muted = muted;
                        peer.deafened = deafened;
                        if muted {
                            peer.speech.hush();
                        }
                    }
                    self.replan = true;
                }
                Command::Moderate {
                    call_id,
                    silenced,
                    hushed,
                } => {
                    if let Some(peer) = self.peer_mut(&call_id) {
                        peer.silenced = silenced;
                        peer.hushed = hushed;
                        if silenced {
                            peer.speech.hush();
                        }
                    }
                    self.replan = true;
                }
                Command::Quality { call_id, video } => {
                    if let Some(peer) = self.peer_mut(&call_id) {
                        peer.quality = video;
                    }
                    self.replan = true;
                }
                Command::EndShare { call_id } => {
                    if let Some(id) = self.by_call.get(&call_id).copied() {
                        self.stop_share(id);
                    }
                }
                Command::Leave { call_id } => {
                    if let Some(id) = self.by_call.get(&call_id).copied() {
                        self.remove(id, None);
                    }
                }
                Command::Shutdown => return false,
            }
        }
    }

    fn join(&mut self, room_id: String, user_id: String, offer: &str, reply: Reply, now: Instant) {
        let starts_share = sdp::share_sending(offer);
        let Ok(offer) = SdpOffer::from_sdp_string(offer) else {
            let _ = reply.send(Err(Refused::BadSdp));
            return;
        };
        let sharer = self.sharer(&room_id);
        if starts_share && sharer.is_some_and(|sharer| sharer.user_id != user_id) {
            let _ = reply.send(Err(Refused::ShareTaken));
            return;
        }
        let replaced: Vec<PeerId> = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.user_id == user_id)
            .map(|(id, _)| *id)
            .collect();
        for id in replaced {
            self.remove(id, None);
        }
        let mut rtc = rtc_config(&self.crypto, &self.cert, self.config.bwe_start_kbps).build(now);
        for candidate in candidates(self.public, self.lan) {
            match candidate {
                Ok(candidate) => {
                    rtc.add_local_candidate(candidate);
                }
                Err(_error) => {
                    tracing::warn!(direction = "offer", reason = "ice", "media failed");
                    let _ = reply.send(Err(Refused::Unavailable));
                    return;
                }
            }
        }
        let answer = match rtc.sdp_api().accept_offer(offer) {
            Ok(answer) => answer,
            Err(error) => {
                tracing::warn!(
                    direction = "offer",
                    reason = crate::rtc_reason(&error),
                    "media failed"
                );
                let _ = reply.send(Err(Refused::BadSdp));
                return;
            }
        };
        self.next_id += 1;
        let id = self.next_id;
        let call_id = self.call_id();
        let peer = Peer {
            call_id: call_id.clone(),
            room_id: room_id.clone(),
            user_id,
            rtc,
            publish: None,
            audio_pt: None,
            slots: Slots::new(),
            told: Vec::new(),
            connected: false,
            joined_at: now,
            heard_at: now,
            muted: false,
            deafened: false,
            silenced: false,
            hushed: false,
            speech: Speech::new(),
            audio: audio_budget(self.config.audio_kbps, now),
            camera: None,
            camera_on: false,
            share: None,
            share_on: false,
            sharing: false,
            layers: [
                Inbound::new(self.config.camera_high_kbps, now),
                Inbound::new(self.config.camera_low_kbps, now),
                Inbound::new(self.config.share_kbps, now),
            ],
            screen: None,
            quality: Quality::High,
            twcc: None,
            remb: None,
            audio_sent: 0,
            audio_kbps: 0,
            told_video: None,
            timeout: now,
            lost: None,
        };
        self.peers.insert(id, peer);
        self.by_call.insert(call_id.clone(), id);
        self.rooms.entry(room_id).or_default().members.push(id);
        self.drain(id);
        let camera = self.camera_changed(id);
        let share = self.share_changed(id);
        self.seat(id);
        self.settle();
        tracing::info!(%call_id, camera, share, "media joined");
        let _ = reply.send(Ok(Answer {
            call_id,
            sdp: answer.to_sdp_string(),
            camera,
            share,
        }));
    }

    fn renegotiate(&mut self, call_id: &str, offer: &str, reply: Reply) {
        let Some(id) = self.by_call.get(call_id).copied() else {
            let _ = reply.send(Err(Refused::NotFound));
            return;
        };
        let starts_share = sdp::share_sending(offer);
        let Ok(offer) = SdpOffer::from_sdp_string(offer) else {
            let _ = reply.send(Err(Refused::BadSdp));
            return;
        };
        let Some(peer) = self.peers.get(&id) else {
            let _ = reply.send(Err(Refused::NotFound));
            return;
        };
        let taken = self
            .rooms
            .get(&peer.room_id)
            .and_then(|room| room.sharer)
            .is_some_and(|sharer| sharer != id);
        if starts_share && !peer.share_on && taken {
            let _ = reply.send(Err(Refused::ShareTaken));
            return;
        }
        let Some(peer) = self.peers.get_mut(&id) else {
            let _ = reply.send(Err(Refused::NotFound));
            return;
        };
        let answer = match peer.rtc.sdp_api().accept_offer(offer) {
            Ok(answer) => answer,
            Err(error) => {
                tracing::warn!(
                    call_id,
                    direction = "offer",
                    reason = crate::rtc_reason(&error),
                    "media failed"
                );
                let _ = reply.send(Err(Refused::BadSdp));
                return;
            }
        };
        self.drain(id);
        let camera = self.camera_changed(id);
        let share = self.share_changed(id);
        self.settle();
        let _ = reply.send(Ok(Answer {
            call_id: call_id.to_string(),
            sdp: answer.to_sdp_string(),
            camera,
            share,
        }));
    }

    /// Reads whether `id`'s camera m-line sends, after an offer. A camera that stops forgets
    /// its layers, so a restart waits for fresh frames.
    fn camera_changed(&mut self, id: PeerId) -> bool {
        let Some(peer) = self.peers.get_mut(&id) else {
            return false;
        };
        let on = peer
            .camera
            .and_then(|mid| peer.rtc.media(mid))
            .is_some_and(|media| media.direction().is_receiving());
        if on != peer.camera_on {
            peer.camera_on = on;
            for layer in &mut peer.layers[..2] {
                layer.restart();
            }
            self.replan = true;
        }
        on
    }

    /// Reads whether `id`'s share m-line sends, after an offer, and whether they hold the
    /// room's share. A share starts when the m-line starts sending and nobody else holds it;
    /// the offer was refused before this if someone did. It stops when the m-line stops.
    fn share_changed(&mut self, id: PeerId) -> bool {
        let Some(peer) = self.peers.get_mut(&id) else {
            return false;
        };
        let on = peer
            .share
            .and_then(|mid| peer.rtc.media(mid))
            .is_some_and(|media| media.direction().is_receiving());
        let started = on && !peer.share_on;
        peer.share_on = on;
        if !on {
            self.stop_share(id);
            return false;
        }
        if started {
            let free = self
                .rooms
                .get(&peer.room_id)
                .is_some_and(|room| room.sharer.is_none_or(|sharer| sharer == id));
            if free {
                peer.sharing = true;
                peer.layers[Layer::Share.index()].restart();
                let room_id = peer.room_id.clone();
                if let Some(room) = self.rooms.get_mut(&room_id) {
                    room.sharer = Some(id);
                }
                self.replan = true;
            }
        }
        self.peers.get(&id).is_some_and(|peer| peer.sharing)
    }

    /// Ends `id`'s share, if they hold one. Listeners go back to a camera on its keyframe.
    fn stop_share(&mut self, id: PeerId) {
        let Some(peer) = self.peers.get_mut(&id) else {
            return;
        };
        if !peer.sharing {
            return;
        }
        peer.sharing = false;
        if let Some(room) = self.rooms.get_mut(&peer.room_id) {
            if room.sharer == Some(id) {
                room.sharer = None;
            }
        }
        self.replan = true;
    }

    /// The participant holding `room_id`'s share.
    fn sharer(&self, room_id: &str) -> Option<&Peer> {
        let sharer = self.rooms.get(room_id)?.sharer?;
        self.peers.get(&sharer)
    }

    /// Gives `id` a slot with every other member of its room, and them one with it.
    fn seat(&mut self, id: PeerId) {
        let Some(room_id) = self.peers.get(&id).map(|peer| peer.room_id.clone()) else {
            return;
        };
        let members = self.members(&room_id);
        for other in members.iter().copied().filter(|other| *other != id) {
            if let Some(peer) = self.peers.get_mut(&other) {
                peer.slots.assign(id);
            }
            if let Some(peer) = self.peers.get_mut(&id) {
                peer.slots.assign(other);
            }
        }
        for member in members {
            self.tell_slots(member);
        }
    }

    /// Takes `id` out of its room and frees its slots. `lost` is told to the API; a leave
    /// the API asked for is not.
    fn remove(&mut self, id: PeerId, lost: Option<Lost>) {
        let Some(mut peer) = self.peers.remove(&id) else {
            return;
        };
        peer.rtc.disconnect();
        self.by_call.remove(&peer.call_id);
        self.by_addr.retain(|_, owner| *owner != id);
        let owned: Vec<Token> = self
            .tcp
            .streams
            .iter()
            .filter(|(_, stream)| stream.owner == Some(id))
            .map(|(token, _)| *token)
            .collect();
        for token in owned {
            self.close_stream(token);
        }
        self.forward.retain(|(origin, _)| *origin != id);
        self.asks.retain(|asker| *asker != id);
        self.replan = true;
        if let Some(lost) = lost {
            tracing::info!(call_id = %peer.call_id, ?lost, "media lost");
            self.events.tell(Event::Left {
                call_id: peer.call_id.clone(),
                lost,
            });
        }
        let members = match self.rooms.get_mut(&peer.room_id) {
            Some(room) => {
                room.members.retain(|member| *member != id);
                room.floor.forget(&id);
                if room.sharer == Some(id) {
                    room.sharer = None;
                }
                room.members.clone()
            }
            None => Vec::new(),
        };
        if members.is_empty() {
            self.rooms.remove(&peer.room_id);
        }
        for member in members {
            let freed = self
                .peers
                .get_mut(&member)
                .is_some_and(|other| other.slots.release(&id));
            if freed {
                self.reseat(member);
                self.tell_slots(member);
            }
        }
    }

    /// Fills `id`'s free slots with members who have none, as after someone left a full room.
    fn reseat(&mut self, id: PeerId) {
        let Some(room_id) = self.peers.get(&id).map(|peer| peer.room_id.clone()) else {
            return;
        };
        for other in self.members(&room_id) {
            if other == id {
                continue;
            }
            if let Some(peer) = self.peers.get_mut(&id) {
                peer.slots.assign(other);
            }
        }
    }

    fn members(&self, room_id: &str) -> Vec<PeerId> {
        self.rooms
            .get(room_id)
            .map(|room| room.members.clone())
            .unwrap_or_default()
    }

    /// Sends `id`'s slot map if it changed.
    fn tell_slots(&mut self, id: PeerId) {
        let Some(peer) = self.peers.get(&id) else {
            return;
        };
        let audio: Vec<Slot> = peer
            .slots
            .held()
            .filter_map(|(mid, other)| {
                self.peers.get(other).map(|other| Slot {
                    mid: mid.to_string(),
                    user_id: other.user_id.clone(),
                })
            })
            .collect();
        let video = peer.screen.as_ref().and_then(|screen| {
            let (publisher, layer) = screen.showing?;
            self.peers.get(&publisher).map(|publisher| VideoSlot {
                mid: screen.mid.to_string(),
                user_id: publisher.user_id.clone(),
                kind: if layer == Layer::Share {
                    VideoKind::Share
                } else {
                    VideoKind::Camera
                },
            })
        });
        if audio == peer.told && video == peer.told_video {
            return;
        }
        let call_id = peer.call_id.clone();
        if let Some(peer) = self.peers.get_mut(&id) {
            peer.told.clone_from(&audio);
            peer.told_video.clone_from(&video);
        }
        self.events.tell(Event::Slots {
            call_id,
            audio,
            video,
        });
    }

    fn peer_mut(&mut self, call_id: &str) -> Option<&mut Peer> {
        let id = self.by_call.get(call_id)?;
        self.peers.get_mut(id)
    }

    fn call_id(&mut self) -> String {
        loop {
            let mut hasher = self.ids.build_hasher();
            hasher.write_u64(self.next_id);
            let call_id = format!("c-{:016x}", hasher.finish());
            if !self.by_call.contains_key(&call_id) {
                return call_id;
            }
            self.next_id += 1;
        }
    }

    /// Reads the datagrams waiting on the socket, up to `READ_BATCH`. True if it stopped there
    /// with more perhaps waiting: readiness is edge-triggered, so only reading to `WouldBlock`
    /// earns another event, and `run` must come back without one.
    fn read(&mut self, buf: &mut [u8]) -> bool {
        for _ in 0..READ_BATCH {
            match self.socket.recv_from(buf) {
                Ok((n, source)) => {
                    let now = Instant::now();
                    if let Some(packet) = buf.get(..n) {
                        self.receive(packet, Protocol::Udp, source, now);
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => return false,
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                // An ICMP error from an earlier send. Not this read's problem.
                Err(error) if error.kind() == ErrorKind::ConnectionRefused => {
                    tracing::debug!(reason = "icmp", "media dropped");
                }
                Err(error) => {
                    tracing::warn!(%error, "media read failed");
                    return false;
                }
            }
        }
        true
    }

    /// One datagram, or one frame from a TCP stream. Over its source's budget, or for
    /// nobody, it is dropped. An address no participant uses yet gets through only as STUN
    /// carrying a participant's ICE username. Returns the participant that took it.
    fn receive(
        &mut self,
        packet: &[u8],
        proto: Protocol,
        source: SocketAddr,
        now: Instant,
    ) -> Option<PeerId> {
        if !self.sources.contains_key(&source) && self.sources.len() >= SOURCES {
            tracing::debug!(reason = "source_cap", "media dropped");
            return None;
        }
        let (rate, burst) = (self.config.packets_per_second, self.config.packet_burst);
        let budget = self
            .sources
            .entry(source)
            .or_insert_with(|| Bucket::new(rate, burst, now));
        if !budget.take(1, now) {
            return None;
        }
        let known = self.by_addr.get(&(proto, source)).copied();
        if known.is_none() && !is_stun(packet) {
            return None;
        }
        let contents = DatagramRecv::try_from(packet).ok()?;
        // The socket binds every interface, so the destination is always the public
        // candidate. A LAN candidate's traffic is answered the same way.
        let input = Input::Receive(
            now,
            Receive {
                proto,
                source,
                destination: self.public,
                contents,
            },
        );
        let known = known.filter(|id| self.peers.get(id).is_some_and(|p| p.rtc.accepts(&input)));
        let id = known.or_else(|| {
            self.peers
                .iter()
                .find(|(_, peer)| peer.rtc.accepts(&input))
                .map(|(id, _)| *id)
        })?;
        self.by_addr.insert((proto, source), id);
        let peer = self.peers.get_mut(&id)?;
        peer.heard_at = now;
        if let Err(error) = peer.rtc.handle_input(input) {
            tracing::warn!(call_id = %peer.call_id, direction = "input", reason = crate::rtc_reason(&error), "media failed");
            peer.rtc.disconnect();
        }
        self.drain(id);
        self.forward_all(now);
        self.settle();
        Some(id)
    }

    /// Fills the slots of everyone in `fresh`.
    fn settle(&mut self) {
        if self.fresh.is_empty() {
            return;
        }
        let mut fresh = std::mem::take(&mut self.fresh);
        fresh.dedup();
        for id in fresh {
            self.reseat(id);
            self.tell_slots(id);
        }
    }

    /// Timeouts, deadlines, speaking, load, and the dead.
    fn tick(&mut self, now: Instant) {
        self.events.flush();
        let due: Vec<PeerId> = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.timeout <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            if let Some(peer) = self.peers.get_mut(&id) {
                if let Err(error) = peer.rtc.handle_input(Input::Timeout(now)) {
                    tracing::warn!(call_id = %peer.call_id, direction = "input", reason = crate::rtc_reason(&error), "media failed");
                    peer.rtc.disconnect();
                }
            }
            self.drain(id);
        }
        self.forward_all(now);
        self.settle();

        for peer in self.peers.values_mut() {
            if peer.lost.is_some() {
                continue;
            }
            if !peer.connected
                && now.saturating_duration_since(peer.joined_at) >= self.config.connect_timeout
            {
                peer.lost = Some(Lost::Timeout);
            } else if peer.connected
                && now.saturating_duration_since(peer.heard_at) >= self.config.consent_timeout
            {
                peer.lost = Some(Lost::Gone);
            }
        }
        let dead: Vec<(PeerId, Lost)> = self
            .peers
            .iter()
            .filter_map(|(id, peer)| peer.lost.map(|lost| (*id, lost)))
            .collect();
        for (id, lost) in dead {
            self.remove(id, Some(lost));
        }

        let sweep = now.saturating_duration_since(self.scanned_at) >= SCAN_EVERY;
        if sweep {
            self.scanned_at = now;
            self.speaking(now);
        }
        if self.replan || now.saturating_duration_since(self.planned_at) >= PLAN_EVERY {
            self.plan(now);
        }
        if sweep {
            self.expire_streams(now);
        }
        let since = now.saturating_duration_since(self.swept_at);
        if since >= Duration::from_secs(1) {
            self.swept_at = now;
            for peer in self.peers.values_mut() {
                let sent = std::mem::take(&mut peer.audio_sent);
                peer.audio_kbps = sent.saturating_mul(8) / since.as_millis().max(1) as u64;
            }
            self.sources
                .retain(|_, bucket| now.saturating_duration_since(bucket.used()) < SOURCE_IDLE);
        }
        let since = now.saturating_duration_since(self.load_at);
        if since >= LOAD_EVERY {
            let egress_kbps = self.egress.saturating_mul(8) / since.as_millis().max(1) as u64;
            self.egress = 0;
            self.load_at = now;
            self.events.hint(Event::Load {
                egress_kbps,
                participants: self.peers.len(),
            });
        }
    }

    /// Closes TCP streams that failed, never verified in time, or went quiet.
    fn expire_streams(&mut self, now: Instant) {
        let (handshake, idle) = (self.config.tcp_handshake, self.config.tcp_idle);
        let expired: Vec<Token> = self
            .tcp
            .streams
            .iter()
            .filter(|(_, stream)| {
                stream.closed
                    || (stream.owner.is_none()
                        && now.saturating_duration_since(stream.opened) >= handshake)
                    || now.saturating_duration_since(stream.heard) >= idle
            })
            .map(|(token, _)| *token)
            .collect();
        for token in expired {
            self.close_stream(token);
        }
    }

    fn speaking(&mut self, now: Instant) {
        for (room_id, room) in &mut self.rooms {
            let loud: Vec<(PeerId, f32)> = room
                .members
                .iter()
                .filter_map(|id| self.peers.get(id).map(|peer| (*id, peer)))
                .filter(|(_, peer)| peer.audible() && peer.speech.speaking(now))
                .map(|(id, peer)| (id, peer.speech.level()))
                .collect();
            if room.floor.hear(&loud, now) {
                self.replan = true;
            }
            let user_ids: Vec<String> = room
                .members
                .iter()
                .filter_map(|id| self.peers.get(id))
                .filter(|peer| peer.audible() && peer.speech.speaking(now))
                .map(|peer| peer.user_id.clone())
                .collect();
            if let Some(user_ids) = room.announced.due(user_ids, now) {
                self.events.hint(Event::Speaking {
                    room_id: room_id.clone(),
                    user_ids,
                });
            }
        }
    }

    /// Polls one `Rtc` until it returns its next timeout.
    fn drain(&mut self, id: PeerId) {
        let Some(peer) = self.peers.get_mut(&id) else {
            return;
        };
        let mut sink = Sink {
            socket: &self.socket,
            tcp: &mut self.tcp,
            events: &mut self.events,
            egress: &mut self.egress,
            forward: &mut self.forward,
            asks: &mut self.asks,
            fresh: &mut self.fresh,
            counts: &mut self.counts,
        };
        drain(id, peer, &mut sink);
    }

    /// Copies each queued audio frame to every listener in its room, and each camera or share
    /// frame to the listeners whose video slot carries it. Then passes on keyframe requests.
    fn forward_all(&mut self, now: Instant) {
        while let Some((origin, data)) = self.forward.pop_front() {
            let Some(publisher) = self.peers.get_mut(&origin) else {
                continue;
            };
            if publisher.camera == Some(data.mid) {
                let layer = Layer::of_rid(data.rid.as_deref());
                self.forward_video(origin, &data, layer, now);
                continue;
            }
            if publisher.share == Some(data.mid) {
                if publisher.sharing {
                    self.forward_video(origin, &data, Layer::Share, now);
                }
                continue;
            }
            if !publisher.audible() {
                continue;
            }
            publisher.speech.hear(data.ext_vals.audio_level, now);
            let size = u32::try_from(data.data.len()).unwrap_or(u32::MAX);
            if !publisher.audio.take(size, now) {
                self.counts.audio_dropped += 1;
                continue;
            }
            let Some(room) = self.rooms.get(&publisher.room_id) else {
                continue;
            };
            for &listener in &room.members {
                if listener == origin {
                    continue;
                }
                let Some(peer) = self.peers.get_mut(&listener) else {
                    continue;
                };
                if peer.deafened || peer.hushed || !peer.connected {
                    continue;
                }
                let Some(mid) = peer.slots.mid_of(&origin).copied() else {
                    continue;
                };
                let Some(writer) = peer.rtc.writer(mid) else {
                    continue;
                };
                let Some(pt) = writer.match_params(data.params) else {
                    continue;
                };
                let written = writer.write(pt, data.network_time, data.time, data.data.clone());
                peer.audio_pt = Some(pt);
                peer.audio_sent += u64::from(size) + AUDIO_HEADERS;
                if let Err(error) = written {
                    tracing::warn!(call_id = %peer.call_id, direction = "audio", reason = crate::rtc_reason(&error), "media failed");
                    peer.rtc.disconnect();
                }
                let mut sink = Sink {
                    socket: &self.socket,
                    tcp: &mut self.tcp,
                    events: &mut self.events,
                    egress: &mut self.egress,
                    forward: &mut self.forward,
                    asks: &mut self.asks,
                    fresh: &mut self.fresh,
                    counts: &mut self.counts,
                };
                release(listener, peer, &mut sink, now);
            }
        }
        for asker in std::mem::take(&mut self.asks) {
            let showing = self
                .peers
                .get(&asker)
                .and_then(|peer| peer.screen.as_ref())
                .and_then(|screen| screen.showing);
            if let Some((publisher, layer)) = showing {
                self.ask_keyframe(publisher, layer, now);
            }
        }
    }

    /// One camera or share frame on `layer`. Over its layer's budget it is dropped, and the
    /// layer stays dropped until a keyframe fits. Each listener showing this layer gets it; a
    /// listener waiting for this layer switches to it on a keyframe.
    fn forward_video(&mut self, origin: PeerId, data: &MediaData, layer: Layer, now: Instant) {
        let keyframe = data.is_keyframe();
        let size = u32::try_from(data.data.len()).unwrap_or(u32::MAX);
        let Some(publisher) = self.peers.get_mut(&origin) else {
            return;
        };
        let inbound = &mut publisher.layers[layer.index()];
        inbound.arrived = true;
        inbound.rid = data.rid;
        let was_broken = inbound.broken;
        let pass = inbound.admit(keyframe, size, now);
        if !pass {
            self.counts.video_dropped += 1;
            if !was_broken {
                tracing::debug!(call_id = %publisher.call_id, reason = "budget", "media dropped");
            }
        }
        if !pass || !data.contiguous {
            self.ask_keyframe(origin, layer, now);
        }
        if !pass {
            return;
        }
        let Some(room) = self
            .peers
            .get(&origin)
            .and_then(|publisher| self.rooms.get(&publisher.room_id))
        else {
            return;
        };
        let source = (origin, layer);
        let time = data.time.rebase(Frequency::NINETY_KHZ).numer();
        let mut switched_listeners = Vec::new();
        for &listener in &room.members {
            if listener == origin {
                continue;
            }
            let Some(peer) = self.peers.get_mut(&listener) else {
                continue;
            };
            if peer.deafened || !peer.connected {
                continue;
            }
            let Some(screen) = peer.screen.as_mut() else {
                continue;
            };
            let switched = screen.showing != Some(source);
            if switched && !(keyframe && screen.wanted == Some(source)) {
                continue;
            }
            screen.showing = Some(source);
            let mid = screen.mid;
            let out = screen.rebase.map(source, time, now);
            let Some(writer) = peer.rtc.writer(mid) else {
                continue;
            };
            let Some(pt) = writer.match_params(data.params) else {
                continue;
            };
            let at = MediaTime::new(out, Frequency::NINETY_KHZ);
            if let Err(error) = writer.write(pt, data.network_time, at, data.data.clone()) {
                tracing::warn!(call_id = %peer.call_id, direction = "video", reason = crate::rtc_reason(&error), "media failed");
                peer.rtc.disconnect();
            }
            let mut sink = Sink {
                socket: &self.socket,
                tcp: &mut self.tcp,
                events: &mut self.events,
                egress: &mut self.egress,
                forward: &mut self.forward,
                asks: &mut self.asks,
                fresh: &mut self.fresh,
                counts: &mut self.counts,
            };
            release(listener, peer, &mut sink, now);
            if switched {
                switched_listeners.push(listener);
            }
        }
        for listener in switched_listeners {
            self.tell_slots(listener);
        }
    }

    /// Asks a publisher for a keyframe on one layer, as often as `Inbound::may_ask` lets it. A
    /// layer that has not sent a frame has no rid to ask on yet.
    fn ask_keyframe(&mut self, publisher: PeerId, layer: Layer, now: Instant) {
        let Some(peer) = self.peers.get_mut(&publisher) else {
            return;
        };
        let mid = match layer {
            Layer::Share => peer.share,
            Layer::High | Layer::Low => peer.camera,
        };
        let Some(mid) = mid else {
            return;
        };
        let inbound = &mut peer.layers[layer.index()];
        if !inbound.may_ask(now) {
            return;
        }
        inbound.asked = Some(now);
        self.counts.keyframe_asks += 1;
        let rid = inbound.rid;
        if let Some(mut writer) = peer.rtc.writer(mid) {
            if let Err(error) = writer.request_keyframe(rid, KeyframeRequestKind::Pli) {
                tracing::debug!(call_id = %peer.call_id, direction = "video", reason = crate::rtc_reason(&error), "media failed");
            }
        }
        self.drain(publisher);
    }

    /// Chooses what every listener's video slot should carry: the room's share if there is
    /// one and the listener is not its sharer; else the newest holder of the floor with a
    /// live camera other than the listener, else the first such camera in join order, and its
    /// layer from the listener's estimate, their `call.quality`, and the server's upload
    /// budget. A listener keeps what it shows until the wanted source's keyframe. A deafened
    /// listener gets nothing, and neither does one whose link has no room for video beside its
    /// audio. A share gives way to a camera on a link too small for it.
    fn plan(&mut self, now: Instant) {
        self.replan = false;
        self.planned_at = now;
        let (mut audio_streams, mut share_viewers, mut camera_viewers) = (0u64, 0u64, 0u64);
        for room in self.rooms.values() {
            let sharer = room
                .sharer
                .filter(|id| self.peers.get(id).is_some_and(Peer::share_live));
            let cameras: Vec<PeerId> = room
                .members
                .iter()
                .copied()
                .filter(|id| self.peers.get(id).is_some_and(Peer::camera_live))
                .collect();
            for id in &room.members {
                let Some(peer) = self.peers.get(id) else {
                    continue;
                };
                if !peer.connected || peer.deafened {
                    continue;
                }
                audio_streams += peer.slots.held().count() as u64;
                if peer.screen.is_none() || peer.quality == Quality::Off {
                    continue;
                }
                if sharer.is_some_and(|sharer| sharer != *id) {
                    share_viewers += 1;
                } else if cameras.iter().any(|camera| camera != id) {
                    camera_viewers += 1;
                }
            }
        }
        let ceiling = video::ceiling(
            self.config.egress_kbps,
            audio_streams,
            share_viewers,
            camera_viewers,
        );
        let ids: Vec<PeerId> = self.peers.keys().copied().collect();
        for id in ids {
            self.plan_screen(id, ceiling, now);
        }
    }

    fn plan_screen(&mut self, id: PeerId, ceiling: Ceiling, now: Instant) {
        let hold = self.config.layer_hold;
        let Some(peer) = self.peers.get_mut(&id) else {
            return;
        };
        let spare = peer.room_for_video();
        let Some(screen) = peer.screen.as_mut() else {
            return;
        };
        let fit = screen.link.update(spare, hold, now);
        let Some(peer) = self.peers.get(&id) else {
            return;
        };
        let Some(screen) = &peer.screen else {
            return;
        };
        let sends = |(other, layer): &Source| {
            *other != id && self.peers.get(other).is_some_and(|peer| peer.sends(*layer))
        };
        let live = |other: &PeerId| sends(&(*other, Layer::High));
        // A deafened listener's slot empties, so it starts again on a keyframe.
        let watching = peer.connected && !peer.deafened;
        let room = self.rooms.get(&peer.room_id).filter(|_| watching);
        let share = room
            .and_then(|room| room.sharer)
            .map(|sharer| (sharer, Layer::Share))
            .filter(sends);
        let share_fits = fit.is_none_or(|fit| fit >= Fit::Share);
        let ceiling_camera = ceiling.camera;
        let publisher = room
            .filter(|_| share.is_none() || !share_fits)
            .and_then(|room| {
                room.floor
                    .recent()
                    .iter()
                    .chain(&room.members)
                    .find(|other| live(other))
                    .copied()
            });
        let allowed = peer.quality.min(ceiling_camera);
        let most = allowed.min(fit.map_or(Quality::High, Fit::quality));
        let camera = publisher.and_then(|publisher| {
            let layer = video::pick_layer(fit, peer.quality, ceiling_camera)?;
            let layers = &self.peers.get(&publisher)?.layers;
            let fresh = |layer: Layer| layers[layer.index()].fresh(now);
            let other = layer.other();
            let fallback =
                !fresh(layer) && fresh(other) && (other == Layer::Low || most == Quality::High);
            Some((publisher, if fallback { other } else { layer }))
        });
        // A share has one layer: `off` and the budget may withhold it. On a link too small for
        // it, a camera's `l` is what can still get through.
        let wants_share = ceiling.share && peer.quality != Quality::Off;
        let wanted = match share {
            Some(share) if share_fits => wants_share.then_some(share),
            _ => camera,
        };
        let showing = screen
            .showing
            .filter(|source| wanted.is_some() && sends(source));
        // Probing asks for what would be shown if the link allowed it, or it could never climb.
        let paused = publisher.is_some() && camera.is_none() && allowed != Quality::Off;
        let probe = match wanted {
            Some((_, Layer::Share)) => video::SHARE_KBPS,
            _ if share.is_some() && wants_share => video::SHARE_KBPS,
            Some(_) if allowed == Quality::High => video::PROBE_KBPS,
            None if paused && allowed == Quality::High => video::PROBE_KBPS,
            None if paused => video::RESUME_PROBE_KBPS,
            _ => video::LOW_KBPS,
        };
        let Some(peer) = self.peers.get_mut(&id) else {
            return;
        };
        let Some(screen) = peer.screen.as_mut() else {
            return;
        };
        let changed = screen.showing != showing;
        screen.wanted = wanted;
        screen.showing = showing;
        if screen.probing != Some(probe) {
            screen.probing = Some(probe);
            peer.rtc.bwe().set_desired_bitrate(Bitrate::kbps(probe));
            self.drain(id);
        }
        if changed {
            self.tell_slots(id);
        }
        if let Some((publisher, layer)) = wanted.filter(|wanted| Some(*wanted) != showing) {
            self.ask_keyframe(publisher, layer, now);
        }
    }
}

/// Sends what a `write` queued. str0m turns a write into packets only on a timeout, so a
/// drain alone sends nothing, and the frame would wait for `tick` behind every other read.
fn release(id: PeerId, peer: &mut Peer, sink: &mut Sink<'_>, now: Instant) {
    if let Err(error) = peer.rtc.handle_input(Input::Timeout(now)) {
        tracing::warn!(call_id = %peer.call_id, direction = "input", reason = crate::rtc_reason(&error), "media failed");
        peer.rtc.disconnect();
    }
    drain(id, peer, sink);
}

fn drain(id: PeerId, peer: &mut Peer, sink: &mut Sink<'_>) {
    loop {
        if !peer.rtc.is_alive() {
            peer.lost.get_or_insert(Lost::Gone);
            return;
        }
        let output = match peer.rtc.poll_output() {
            Ok(output) => output,
            Err(error) => {
                tracing::warn!(call_id = %peer.call_id, direction = "output", reason = crate::rtc_reason(&error), "media failed");
                peer.rtc.disconnect();
                peer.lost.get_or_insert(Lost::Gone);
                return;
            }
        };
        match output {
            Output::Timeout(at) => {
                peer.timeout = at;
                return;
            }
            Output::Transmit(transmit) => {
                *sink.egress += transmit.contents.len() as u64;
                if transmit.proto == Protocol::Tcp {
                    let urgent = peer.audio_pt.is_some()
                        && payload_type(&transmit.contents) == peer.audio_pt;
                    let sent = sink
                        .tcp
                        .send(transmit.destination, &transmit.contents, urgent);
                    sink.counts.send_dropped += u64::from(!sent);
                } else if let Err(error) = sink
                    .socket
                    .send_to(&transmit.contents, transmit.destination)
                {
                    if error.kind() == ErrorKind::WouldBlock {
                        sink.counts.send_dropped += 1;
                    } else {
                        tracing::warn!(call_id = %peer.call_id, direction = "output", reason = "io", "media failed");
                    }
                }
            }
            Output::Event(event) => match event {
                RtcEvent::Connected => {
                    peer.connected = true;
                    tracing::info!(call_id = %peer.call_id, "media connected");
                    sink.events.tell(Event::Connected {
                        call_id: peer.call_id.clone(),
                    });
                }
                RtcEvent::MediaAdded(added) if added.kind == MediaKind::Audio => {
                    match added.direction {
                        Direction::RecvOnly | Direction::SendRecv if peer.publish.is_none() => {
                            peer.publish = Some(added.mid);
                        }
                        Direction::SendOnly => {
                            peer.slots.add_mid(added.mid);
                            sink.fresh.push(id);
                        }
                        _ => {}
                    }
                }
                RtcEvent::MediaAdded(added) if added.kind == MediaKind::Video => {
                    match added.direction {
                        Direction::SendOnly if peer.screen.is_none() => {
                            peer.screen = Some(Screen {
                                mid: added.mid,
                                showing: None,
                                wanted: None,
                                rebase: Rebase::default(),
                                probing: None,
                                link: Link::default(),
                            });
                        }
                        Direction::RecvOnly | Direction::SendRecv | Direction::Inactive
                            if peer.camera.is_none() =>
                        {
                            peer.camera = Some(added.mid);
                        }
                        Direction::RecvOnly | Direction::SendRecv | Direction::Inactive
                            if peer.share.is_none() =>
                        {
                            peer.share = Some(added.mid);
                        }
                        _ => {}
                    }
                }
                RtcEvent::MediaData(data)
                    if [peer.publish, peer.camera, peer.share].contains(&Some(data.mid)) =>
                {
                    sink.forward.push_back((id, data));
                }
                RtcEvent::KeyframeRequest(request)
                    if peer.screen.as_ref().map(|screen| screen.mid) == Some(request.mid) =>
                {
                    sink.asks.push(id);
                }
                RtcEvent::EgressBitrateEstimate(BweKind::Twcc { estimate, .. }) => {
                    peer.twcc = Some(kbps(estimate));
                }
                RtcEvent::EgressBitrateEstimate(BweKind::Remb { estimate, mid })
                    if peer.screen.as_ref().map(|screen| screen.mid) == Some(mid) =>
                {
                    peer.remb = Some(kbps(estimate));
                }
                RtcEvent::Closed => {
                    peer.lost.get_or_insert(Lost::Gone);
                }
                _ => {}
            },
        }
    }
}

/// Binds UDP, then TCP on the same port number. With port 0, TCP may find that number
/// taken, so it tries another.
fn bind(addr: SocketAddr) -> io::Result<(UdpSocket, TcpListener, u16)> {
    let mut tries = 0;
    loop {
        tries += 1;
        let udp = std::net::UdpSocket::bind(addr)?;
        let port = udp.local_addr()?.port();
        match std::net::TcpListener::bind(SocketAddr::new(addr.ip(), port)) {
            Ok(tcp) => {
                request_buffers(&udp);
                udp.set_nonblocking(true)?;
                tcp.set_nonblocking(true)?;
                return Ok((UdpSocket::from_std(udp), TcpListener::from_std(tcp), port));
            }
            Err(error)
                if addr.port() == 0
                    && matches!(
                        error.kind(),
                        ErrorKind::AddrInUse | ErrorKind::PermissionDenied
                    )
                    && tries < BIND_TRIES => {}
            Err(error) => return Err(error),
        }
    }
}

/// Asks for `SOCKET_BUFFER` each way and logs what it got, which is an operator's only sign
/// of a low cap. Linux reports double what it keeps for payload.
fn request_buffers(udp: &std::net::UdpSocket) {
    let socket = SockRef::from(udp);
    if let Err(error) = socket.set_recv_buffer_size(SOCKET_BUFFER) {
        tracing::warn!(%error, "media receive buffer not set");
    }
    if let Err(error) = socket.set_send_buffer_size(SOCKET_BUFFER) {
        tracing::warn!(%error, "media send buffer not set");
    }
    let receive = socket.recv_buffer_size().unwrap_or_default();
    let send = socket.send_buffer_size().unwrap_or_default();
    tracing::info!(receive, send, "media socket buffers");
}

/// What one publisher's audio may spend.
fn audio_budget(kbps: u32, now: Instant) -> Bucket {
    let rate = kbps.saturating_mul(1000) / 8;
    Bucket::new(rate, rate.saturating_mul(AUDIO_BURST), now)
}

/// A missing audio packet is released at once. str0m's default holds it until 15 later
/// packets arrive or a second passes, and every listener has gone silent by then. Video is
/// left to its own, larger, hold.
pub(crate) fn release_audio_promptly(config: RtcConfig) -> RtcConfig {
    config
        .set_reordering_size_audio(0)
        .set_reordering_timeout_audio(Some(Duration::from_millis(40)))
}

/// ICE-lite, the process's DTLS certificate, Opus, and H.264 constrained baseline in
/// packetization mode 1 with RTX. TWCC bandwidth estimation starts at `bwe_kbps`, for
/// choosing layers; `None` leaves it off.
pub(crate) fn rtc_config(
    crypto: &Arc<CryptoProvider>,
    cert: &DtlsCert,
    bwe_kbps: Option<u32>,
) -> RtcConfig {
    let mut config = release_audio_promptly(
        RtcConfig::new()
            .set_ice_lite(true)
            .set_crypto_provider(crypto.clone())
            .set_dtls_cert(cert.clone())
            .clear_codecs()
            .enable_opus(true, false)
            .enable_bwe(bwe_kbps.map(|kbps| Bitrate::kbps(u64::from(kbps)))),
    );
    config
        .codec_config()
        .add_h264(H264_PT.into(), Some(H264_RTX_PT.into()), true, H264_PROFILE);
    config
}

/// H.264 constrained baseline, level 3.1: enough for 1280×720 at 30 fps.
pub(crate) const H264_PROFILE: u32 = 0x42e01f;
const H264_PT: u8 = 108;
const H264_RTX_PT: u8 = 109;

fn kbps(bitrate: Bitrate) -> u64 {
    bitrate.as_u64() / 1000
}

/// A UDP host candidate and a passive TCP one on the public address, and the same pair on
/// the LAN address when there is one.
pub(crate) fn candidates(
    public: SocketAddr,
    lan: Option<SocketAddr>,
) -> Vec<Result<Candidate, str0m::error::IceError>> {
    [Some(public), lan]
        .into_iter()
        .flatten()
        .flat_map(|addr| {
            [
                Candidate::host(addr, "udp"),
                Candidate::builder()
                    .tcp()
                    .host(addr)
                    .tcptype(TcpType::Passive)
                    .build(),
            ]
        })
        .collect()
}

/// The payload type of an RTP packet, whose header SRTP leaves in the clear. RTCP reads as a
/// number below the dynamic range, so it never matches a media type. RFC 7983 says which is RTP.
fn payload_type(packet: &[u8]) -> Option<Pt> {
    match packet {
        [128..=191, marker_and_type, ..] => Some((marker_and_type & 0x7f).into()),
        _ => None,
    }
}

/// RFC 7983: a first byte of 0 to 3 is STUN.
fn is_stun(packet: &[u8]) -> bool {
    packet.first().is_some_and(|byte| *byte < 4)
}

fn readiness(event: &Ready) -> (Token, bool, bool) {
    (
        event.token(),
        event.is_readable() || event.is_read_closed() || event.is_error(),
        event.is_writable(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    /// A room's highest bitrate in 20 ms frames, and the tag, nonce and marker of a seal.
    const SEALED_FRAME: u32 = 128_000 / 8 / 50 + 16;

    fn defaults() -> Config {
        let any = Ipv4Addr::LOCALHOST;
        Config::new(SocketAddr::from((any, 0)), any.into())
    }

    #[test]
    fn thirty_seconds_at_the_highest_room_bitrate_all_pass_the_audio_budget() {
        let start = Instant::now();
        let mut budget = audio_budget(defaults().audio_kbps, start);
        for frame in 0..1500 {
            let at = start + Duration::from_millis(frame * 20);
            assert!(budget.take(SEALED_FRAME, at), "frame {frame} dropped");
        }
    }

    #[test]
    fn voice_bitrate_overshoot_passes_the_audio_budget_and_a_flood_does_not() {
        let start = Instant::now();
        let mut budget = audio_budget(defaults().audio_kbps, start);
        // Half again over the bitrate through a loud passage of 10 s.
        for frame in 0..500 {
            let at = start + Duration::from_millis(frame * 20);
            assert!(budget.take(SEALED_FRAME * 3 / 2, at), "frame {frame}");
        }
        // Then four times the room's top rate, which no client sends.
        let mut dropped = 0;
        for frame in 500..2000 {
            let at = start + Duration::from_millis(frame * 20);
            dropped += u32::from(!budget.take(SEALED_FRAME * 4, at));
        }
        assert!(dropped > 500, "only {dropped} of 1500 dropped");
    }

    const AT_30_FPS: Duration = Duration::from_millis(33);

    /// A layer of 3,000 bytes a second, broken by a frame over its budget.
    fn broken_layer(now: Instant) -> Inbound {
        let mut layer = Inbound::new(24, now);
        layer.arrived = true;
        assert!(!layer.admit(false, 4000, now));
        assert!(layer.broken);
        layer
    }

    #[test]
    fn a_broken_layer_saves_up_for_its_keyframe() {
        let start = Instant::now();
        let mut layer = broken_layer(start);
        // Deltas that would each spend what came in since the last, if they were charged.
        let mut at = start;
        for _ in 0..60 {
            at += AT_30_FPS;
            assert!(
                !layer.admit(false, 200, at),
                "a delta passed a broken layer"
            );
        }
        assert!(layer.admit(true, 2900, at), "the keyframe did not fit");
        assert!(!layer.broken);
        assert!(layer.fresh(at));
    }

    #[test]
    fn a_keyframe_that_does_not_fit_leaves_the_layer_broken() {
        let start = Instant::now();
        let mut layer = broken_layer(start);
        assert!(!layer.admit(true, 4000, start + AT_30_FPS));
        assert!(layer.broken);
        assert!(!layer.fresh(start + AT_30_FPS));
    }

    #[test]
    fn keyframe_requests_back_off_while_broken_and_wait_after_a_keyframe() {
        let start = Instant::now();
        let mut layer = broken_layer(start);
        assert!(layer.may_ask(start));
        layer.asked = Some(start);
        assert!(!layer.may_ask(start + video::KEYFRAME_EVERY));
        assert!(layer.may_ask(start + video::KEYFRAME_BROKEN_EVERY));

        let mut healthy = Inbound::new(2400, start);
        assert!(!healthy.may_ask(start), "no frame has arrived");
        assert!(healthy.admit(true, 100, start));
        healthy.arrived = true;
        assert!(!healthy.may_ask(start), "a keyframe just came");
        assert!(healthy.may_ask(start + video::KEYFRAME_EVERY));
    }

    #[test]
    fn the_payload_type_is_read_from_rtp_only() {
        let opus: Pt = 111.into();
        assert_eq!(payload_type(&[0x80, 111, 0, 1]), Some(opus));
        // The marker bit is not part of it.
        assert_eq!(payload_type(&[0x80, 0x80 | 111, 0, 1]), Some(opus));
        // A sender report, DTLS, and STUN.
        assert_ne!(payload_type(&[0x80, 200, 0, 6]), Some(opus));
        assert_eq!(payload_type(&[22, 254, 253]), None);
        assert_eq!(payload_type(&[0, 1, 0, 0]), None);
        assert_eq!(payload_type(&[0x80]), None);
    }

    fn left(call_id: &str) -> Event {
        Event::Left {
            call_id: call_id.to_string(),
            lost: Lost::Gone,
        }
    }

    #[test]
    fn events_over_a_full_queue_wait_their_turn_and_nothing_blocks() {
        let (sender, mut receiver) = async_mpsc::channel(1);
        let mut outbox = Outbox::new(sender);
        // `blocking_send` would stop the media thread here until the API read.
        for call_id in ["a", "b", "c"] {
            outbox.tell(left(call_id));
        }
        outbox.hint(left("dropped"));
        assert_eq!(receiver.try_recv().ok(), Some(left("a")));
        outbox.flush();
        assert_eq!(receiver.try_recv().ok(), Some(left("b")));
        outbox.tell(left("d"));
        assert_eq!(receiver.try_recv().ok(), Some(left("c")));
        outbox.flush();
        assert_eq!(receiver.try_recv().ok(), Some(left("d")));
        assert!(receiver.try_recv().is_err());
        assert!(outbox.waiting.is_empty());
    }

    #[test]
    fn events_for_a_gone_api_are_let_go() {
        let (sender, receiver) = async_mpsc::channel(1);
        let mut outbox = Outbox::new(sender);
        drop(receiver);
        outbox.tell(left("a"));
        assert!(outbox.waiting.is_empty());
    }

    #[test]
    fn a_missing_audio_packet_is_not_held_for_fifteen_later_ones() {
        let config = release_audio_promptly(RtcConfig::new());
        assert_eq!(config.reordering_size_audio(), 0);
        assert_eq!(
            config.reordering_timeout_audio(),
            Some(Duration::from_millis(40))
        );
    }
}

/// The media thread's steps driven by hand, to see what each one does on its own.
#[cfg(all(test, feature = "headless"))]
mod turn_tests {
    use std::net::{Ipv4Addr, UdpSocket as StdUdpSocket};
    use std::sync::mpsc;

    use super::*;
    use crate::client::{Client, Running};

    const LOOPBACK: Ipv4Addr = Ipv4Addr::LOCALHOST;
    const PATIENCE: Duration = Duration::from_secs(3);

    struct Rig {
        engine: Engine,
        buf: Vec<u8>,
        _commands: mpsc::SyncSender<Command>,
        _events: async_mpsc::Receiver<Event>,
    }

    impl Rig {
        fn new() -> Self {
            let bind = SocketAddr::from((LOOPBACK, 0));
            let mut config = Config::new(bind, LOOPBACK.into());
            config.bwe_start_kbps = None;
            let (commands, inbox) = mpsc::sync_channel(8);
            let (events, outbox) = async_mpsc::channel(1024);
            let alive = Arc::new(AtomicBool::new(true));
            let (engine, _waker) = Engine::bind(config, inbox, events, alive).expect("bind");
            Self {
                engine,
                buf: vec![0; DATAGRAM],
                _commands: commands,
                _events: outbox,
            }
        }

        /// Joins a client and runs whole turns until it and everyone else are connected.
        fn join(&mut self, user_id: &str, marker: u8) -> Running {
            let mut client = Client::new(SocketAddr::from((LOOPBACK, 0)), 3).expect("client");
            let offer = client.offer().to_string();
            let (reply, answer) = tokio::sync::oneshot::channel();
            let now = Instant::now();
            self.engine
                .join("r".to_string(), user_id.to_string(), &offer, reply, now);
            let answer = answer.blocking_recv().expect("reply").expect("answer");
            client.accept(&answer.sdp).expect("accept");
            let running = client.start(marker).expect("start");
            let deadline = Instant::now() + PATIENCE;
            while !self.engine.peers.values().all(|peer| peer.connected) {
                assert!(Instant::now() < deadline, "never connected");
                self.engine.read(&mut self.buf);
                self.engine.tick(Instant::now());
                std::thread::sleep(Duration::from_millis(1));
            }
            running
        }
    }

    #[test]
    fn a_forwarded_frame_leaves_before_the_turn_ends() {
        let mut rig = Rig::new();
        let speaker = rig.join("a", b'A');
        let listener = rig.join("b", b'B');
        // Packets from a listener would also move its `Rtc` along, so this one sends none.
        listener.stop();
        speaker.send_payload(Some(vec![0x55; 1000]));
        speaker.sending(true);
        let before = rig.engine.egress;
        let deadline = Instant::now() + PATIENCE;
        // Only the socket is read, with no `tick`, which is all that used to send these. RTCP
        // and STUN for the speaker are far smaller than this many frames.
        while rig.engine.egress - before < 30_000 {
            assert!(Instant::now() < deadline, "nothing left for the listener");
            rig.engine.read(&mut rig.buf);
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn the_socket_asks_for_more_buffer_than_the_default() {
        let rig = Rig::new();
        let socket = SockRef::from(&rig.engine.socket);
        // Windows gives what was asked. Linux caps it, at about 200 KiB on a stock kernel.
        let least = if cfg!(windows) {
            SOCKET_BUFFER
        } else {
            128 * 1024
        };
        assert!(socket.recv_buffer_size().expect("read back") >= least);
        assert!(socket.send_buffer_size().expect("read back") >= least);
    }

    #[test]
    fn a_read_stops_at_its_cap_and_says_more_is_waiting() {
        let mut rig = Rig::new();
        let sender = StdUdpSocket::bind((LOOPBACK, 0)).expect("bind");
        for _ in 0..READ_BATCH + 10 {
            sender
                .send_to(&[0x80; 32], rig.engine.public)
                .expect("send");
        }
        // Loopback delivers before `send_to` returns, on every platform these tests run on.
        assert!(rig.engine.read(&mut rig.buf), "more should be waiting");
        assert!(!rig.engine.read(&mut rig.buf), "the rest was under the cap");
    }
}
