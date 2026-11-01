//! One live socket: `ready`, the replay, then events, presence, and calls until something
//! closes it.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use db::{Caller, CommunityError, SessionError};
use tokio::sync::broadcast::error::RecvError;
use tokio::time::{Instant, MissedTickBehavior};

use super::call::{Moves, Reason, Refused, Session};
use super::frame::{Audience, Event, Incoming, Me as MeFrame, Out};
use super::hub::{access, Hub, Joined, Slot};

/// Why the server closed a socket. The code and reason are in `docs/calls.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Close {
    Idle,
    SessionEnded,
    Resync,
    Removed,
    Replaced,
    SlowDown,
    Restart,
    Unavailable,
    TextOnly,
    TooBig,
    /// The operator took the community offline.
    Suspended,
}

impl Close {
    fn frame(self) -> CloseFrame {
        let (code, reason) = match self {
            Self::Idle => (4000, "idle"),
            Self::SessionEnded => (4001, "session_ended"),
            Self::Resync => (4002, "resync"),
            Self::Removed => (4003, "removed"),
            Self::Replaced => (4008, "replaced"),
            Self::SlowDown => (4029, "slow_down"),
            Self::Restart => (1012, "restart"),
            Self::Unavailable => (1011, "unavailable"),
            Self::TextOnly => (1003, "text_only"),
            Self::TooBig => (1009, "too_big"),
            Self::Suspended => (4010, "suspended"),
        };
        CloseFrame {
            code,
            reason: reason.into(),
        }
    }
}

/// How a socket ends: with a close frame, or with the peer already gone.
enum End {
    Close(Close),
    Gone,
}

type Step = Result<(), End>;

pub(crate) struct Me {
    pub token: String,
    /// The signed-in device this socket belongs to.
    pub session: Session,
    pub user_id: String,
    pub role: String,
    /// Server-wide permissions, re-read with the session.
    pub permissions: i64,
    /// Empty for a person. A bot carries the scopes it was granted.
    pub scopes: Vec<String>,
}

impl Me {
    fn is_bot(&self) -> bool {
        self.role == "bot"
    }

    fn allows(&self, scope: &str) -> bool {
        self.scopes.iter().any(|have| have == scope)
    }

    /// A person sees the snapshot. A bot sees it only with that scope.
    fn snapshot<'a, T>(&self, items: &'a [T], scope: &str) -> &'a [T] {
        if self.is_bot() && !self.allows(scope) {
            &[]
        } else {
            items
        }
    }

    fn caller(&self) -> Caller {
        Caller {
            user_id: self.user_id.clone(),
            role: self.role.clone(),
            scopes: self.scopes.clone(),
        }
    }

    fn update(&mut self, identity: db::Identity) {
        self.role = identity.role;
        self.permissions = identity.permissions;
    }
    /// Bots receive only room and member topics covered by their explicit scopes.
    fn sees_topic(&self, topic: &str) -> bool {
        if !self.is_bot() {
            return true;
        }
        if topic.starts_with("room.") {
            return self.allows("rooms.read");
        }
        if topic.starts_with("member.") {
            return self.allows("members.read");
        }
        false
    }

    fn sees_durable(&self, event: &Event) -> bool {
        if self.is_bot() {
            return matches!(event.audience, Audience::Everyone) && self.sees_topic(&event.topic);
        }
        match event.audience {
            Audience::Users => event.users.iter().any(|id| id == &self.user_id),
            other => other.admits(self.permissions),
        }
    }

    /// Bots may receive member presence and public call occupancy, never signaling or messages.
    fn sees_ephemeral(&self, frame: &str) -> bool {
        let Some(kind) = frame_kind(frame) else {
            return !self.is_bot();
        };
        if !self.is_bot() {
            return true;
        }
        if kind.starts_with("room.") || matches!(kind, "call.joined" | "call.left" | "call.state") {
            return self.allows("rooms.read");
        }
        if kind.starts_with("member.") || kind == "presence" {
            return self.allows("members.read");
        }
        false
    }
}

/// The leading `"type"` of a frame this process serialises. Anything else is left alone.
fn frame_kind(frame: &str) -> Option<&str> {
    frame
        .strip_prefix("{\"type\":\"")
        .and_then(|rest| rest.split('"').next())
        .filter(|kind| !kind.is_empty())
}

/// Client frames that change a call. A bot cannot send them.
fn changes_call(kind: &str) -> bool {
    kind.starts_with("call.")
}

/// A request that succeeded.
enum Done {
    /// Sent as `ok`.
    Ok,
    /// Sent as `call.answer`.
    Answer(sfu::Answer),
}

/// What a request gets back: `ok` or `call.answer`, or `error` with this code.
type Answer = Result<Done, &'static str>;

pub(crate) async fn run(
    hub: Arc<Hub>,
    mut socket: WebSocket,
    slot: Slot,
    mut me: Me,
    since: Option<i64>,
) {
    let joined = hub.join(&me.user_id, me.is_bot(), me.session);
    let conn = joined.conn;
    tracing::info!(user_id = %me.user_id, conn, "live socket opened");
    if !me.is_bot() {
        hub.saw(&me.user_id);
    }
    let end = serve(&hub, &mut socket, &mut me, since, joined).await;
    let reason = match end {
        End::Close(Close::Removed) => Some(Reason::Removed),
        End::Close(Close::SessionEnded) => Some(Reason::SessionEnded),
        _ => None,
    };
    if let End::Close(close) = end {
        tracing::info!(user_id = %me.user_id, conn, ?close, "live socket closed");
        let frame = Message::Close(Some(close.frame()));
        let _ = tokio::time::timeout(Duration::from_secs(1), socket.send(frame)).await;
    } else {
        tracing::info!(user_id = %me.user_id, conn, "live socket gone");
    }
    hub.hang_up(&me.user_id, conn, reason);
    hub.leave(&me.user_id, conn);
    if !me.is_bot() {
        hub.saw(&me.user_id);
    }
    drop(slot);
}

async fn serve(
    hub: &Hub,
    socket: &mut WebSocket,
    me: &mut Me,
    since: Option<i64>,
    joined: Joined,
) -> End {
    let Joined {
        conn,
        mut kicked,
        mut presence,
        mut direct,
        online,
        calls,
    } = joined;
    // Subscribed before the outbox is read, so nothing falls between the replay and the stream.
    let mut events = hub.events.subscribe();
    let mut stop = hub.stopping();
    let limits = hub.limits;

    let bounds = match db::outbox_bounds(&hub.pool).await {
        Ok(bounds) => bounds,
        Err(error) => {
            tracing::warn!(%error, "outbox read failed");
            return End::Close(Close::Unavailable);
        }
    };
    let mut last = match since {
        Some(since) if since < bounds.pruned_through || since > bounds.head => {
            return resync(hub, socket).await;
        }
        Some(since) => since,
        None => bounds.head,
    };
    let online_frame = me.snapshot(&online, "members.read");
    let calls_frame = me.snapshot(&calls, "rooms.read");
    let ready = Out::Ready {
        seq: last,
        me: MeFrame {
            user_id: &me.user_id,
            role: &me.role,
        },
        online: online_frame,
        calls: calls_frame,
    };
    if let Err(end) = send(hub, socket, ready.text()).await {
        return end;
    }
    if since.is_some() {
        let rows = match db::outbox_after(&hub.pool, last, limits.replay_max + 1).await {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(%error, "outbox replay failed");
                return End::Close(Close::Unavailable);
            }
        };
        if rows.len() as i64 > limits.replay_max {
            return resync(hub, socket).await;
        }
        for row in rows {
            last = row.seq;
            if let Err(end) = deliver(hub, socket, me, &Event::from_row(row)).await {
                return end;
            }
        }
    }

    let mut seen = Instant::now();
    let mut recheck = tokio::time::interval_at(Instant::now() + limits.recheck, limits.recheck);
    recheck.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut bucket = Bucket::new(limits.messages_per_second, limits.message_burst);
    let mut moves = Moves::new(limits.call_move_burst, limits.call_move_every);
    let mut rekeys = Moves::new(limits.e2ee_rekey_burst, limits.e2ee_rekey_every);
    let mut asking = Asking {
        conn,
        bucket: &mut bucket,
        moves: &mut moves,
        rekeys: &mut rekeys,
    };

    loop {
        let step = tokio::select! {
            close = &mut kicked => Err(End::Close(close.unwrap_or(Close::Replaced))),
            () = stopped(&mut stop) => Err(End::Close(Close::Restart)),
            event = events.recv() => match event {
                Ok(event) if event.seq <= last => Ok(()),
                Ok(event) => {
                    last = event.seq;
                    deliver(hub, socket, me, &event).await
                }
                Err(RecvError::Lagged(_)) => return resync(hub, socket).await,
                Err(RecvError::Closed) => Err(End::Close(Close::Restart)),
            },
            frame = presence.recv() => match frame {
                Ok(frame) => send_visible(hub, socket, me, frame.as_ref()).await,
                Err(RecvError::Lagged(_)) => return resync(hub, socket).await,
                Err(RecvError::Closed) => Err(End::Close(Close::Restart)),
            },
            Some(frame) = direct.recv() => send_visible(hub, socket, me, frame.as_ref()).await,
            message = socket.recv() => {
                seen = Instant::now();
                receive(hub, socket, me, &mut asking, message).await
            }
            () = tokio::time::sleep_until(seen + limits.idle) => Err(End::Close(Close::Idle)),
            _ = recheck.tick() => {
                if hub.operator.current().await.mode == db::Mode::Offline {
                    Err(End::Close(Close::Suspended))
                } else {
                    if !me.is_bot() { hub.saw(&me.user_id); }
                    check(hub, me).await
                }
            }
        };
        if let Err(end) = step {
            return end;
        }
    }
}

/// Resolves once the hub is closing, including when it already was.
async fn stopped(stop: &mut tokio::sync::watch::Receiver<bool>) {
    let _ = stop.wait_for(|stopped| *stopped).await;
}

async fn resync(hub: &Hub, socket: &mut WebSocket) -> End {
    match send(hub, socket, Out::Resync.text()).await {
        Ok(()) => End::Close(Close::Resync),
        Err(end) => end,
    }
}

/// Sends one durable event if this socket may see it, and acts on events about this person.
async fn deliver(hub: &Hub, socket: &mut WebSocket, me: &mut Me, event: &Event) -> Step {
    let mine = event.subject.as_deref() == Some(me.user_id.as_str());
    if me.sees_durable(event) {
        if let Some(frame) = &event.frame {
            send(hub, socket, frame.clone()).await?;
        }
    }
    match event.topic.as_str() {
        "member.removed" | "member.banned" if mine => Err(End::Close(Close::Removed)),
        "member.role" | "member.roles" if mine => check(hub, me).await,
        // A role this person may hold changed what they may see.
        "role.updated" | "role.deleted" => check(hub, me).await,
        "session.ended" if mine => check(hub, me).await,
        _ => Ok(()),
    }
}

/// Re-reads the session. A database error keeps the socket; the next check tries again.
/// A bot token is checked with `authenticate_bot`, and its scopes are refreshed.
async fn check(hub: &Hub, me: &mut Me) -> Step {
    if me.is_bot() {
        return match db::authenticate_bot(&hub.pool, &me.token).await {
            Ok(bot) => {
                me.scopes = bot.scopes;
                Ok(())
            }
            Err(SessionError::Unauthorized) => Err(End::Close(Close::SessionEnded)),
            Err(SessionError::Database(error)) => {
                tracing::warn!(%error, "live session check failed");
                Ok(())
            }
        };
    }
    match db::current_session(&hub.pool, &me.token).await {
        Ok(identity) => {
            me.update(identity);
            Ok(())
        }
        Err(SessionError::Unauthorized) => Err(End::Close(Close::SessionEnded)),
        Err(SessionError::Database(error)) => {
            tracing::warn!(%error, "live session check failed");
            Ok(())
        }
    }
}

/// Per-socket state for client requests.
struct Asking<'a> {
    conn: u64,
    bucket: &'a mut Bucket,
    moves: &'a mut Moves,
    /// End-to-end encryption key packages and `invalid`s.
    rekeys: &'a mut Moves,
}

async fn receive(
    hub: &Hub,
    socket: &mut WebSocket,
    me: &mut Me,
    asking: &mut Asking<'_>,
    message: Option<Result<Message, axum::Error>>,
) -> Step {
    let message = match message {
        None => return Err(End::Gone),
        Some(Ok(message)) => message,
        Some(Err(error)) => {
            if is_too_big(&error) {
                frame_refused(&me.user_id, asking.conn, "bad_frame");
                return Err(End::Close(Close::TooBig));
            }
            return Err(End::Gone);
        }
    };
    let text = match message {
        Message::Text(text) => text,
        Message::Binary(_) => return Err(End::Close(Close::TextOnly)),
        Message::Ping(_) | Message::Pong(_) => return Ok(()),
        Message::Close(_) => return Err(End::Gone),
    };
    match asking.bucket.take() {
        Take::Allowed => {}
        Take::Warn => {
            let warning = Out::Error {
                reference: None,
                code: "slow_down",
            };
            return send(hub, socket, warning.text()).await;
        }
        Take::Dropped => return Ok(()),
        Take::Exceeded => {
            frame_refused(&me.user_id, asking.conn, "slow_down");
            return Err(End::Close(Close::SlowDown));
        }
    }
    let Some(incoming) = Incoming::parse(text.as_str()) else {
        frame_refused(&me.user_id, asking.conn, "bad_frame");
        let reply = Out::Error {
            reference: None,
            code: "bad_frame",
        };
        return send(hub, socket, reply.text()).await;
    };
    if me.is_bot() && changes_call(&incoming.kind) {
        frame_refused(&me.user_id, asking.conn, "forbidden");
        let reply = Out::Error {
            reference: incoming.reference.as_ref(),
            code: "forbidden",
        };
        return send(hub, socket, reply.text()).await;
    }
    if !crate::operator::read_only_frame_allowed(&incoming.kind)
        && hub.operator.current().await.mode != db::Mode::Normal
    {
        frame_refused(&me.user_id, asking.conn, "read_only");
        let reply = Out::Error {
            reference: incoming.reference.as_ref(),
            code: "read_only",
        };
        return send(hub, socket, reply.text()).await;
    }
    let room_id = incoming.room_id.as_deref();
    let sdp = incoming.sdp.as_deref();
    let e2ee = incoming.e2ee_version() >= u64::from(super::e2ee::VERSION);
    let answer = match incoming.kind.as_str() {
        "ping" => return send(hub, socket, Out::Pong.text()).await,
        // Answered only when refused: the frames it causes are the answer.
        "call.e2ee" => match call_e2ee(hub, me, asking, &incoming) {
            Ok(()) => return Ok(()),
            Err(code) => Err(code),
        },
        "call.join" => call_join(hub, me, asking, room_id, sdp, e2ee).await?,
        "call.leave" => call_leave(hub, me, asking),
        "call.kick" => call_kick(hub, me, room_id, incoming.user_id.as_deref()).await?,
        "call.moderate" => call_moderate(hub, me, &incoming).await?,
        "call.move" => call_move(hub, me, room_id, incoming.user_id.as_deref()).await?,
        "call.share_end" => call_share_end(hub, me, room_id).await?,
        "call.offer" => call_offer(hub, me, asking, sdp).await,
        "call.state" => call_state(hub, me, &incoming),
        "call.quality" => call_quality(hub, me, incoming.video.as_deref()),
        "typing" => typing(hub, me, incoming.conversation_id.as_deref()).await,
        _ => Err("unknown_type"),
    };
    let reference = incoming.reference.as_ref();
    let reply = match &answer {
        Ok(Done::Ok) => Out::Ok { reference },
        Ok(Done::Answer(answer)) => Out::CallAnswer {
            reference,
            call_id: &answer.call_id,
            sdp: &answer.sdp,
        },
        Err(code) => {
            note_refusal(
                me,
                asking.conn,
                &incoming.kind,
                hub.call_of(&me.user_id).as_deref(),
                code,
            );
            Out::Error { reference, code }
        }
    };
    send(hub, socket, reply.text()).await
}

fn frame_refused(user_id: &str, conn: u64, reason: &'static str) {
    tracing::warn!(user_id, conn, reason, "frame refused");
}

fn note_refusal(me: &Me, conn: u64, kind: &str, call_id: Option<&str>, code: &'static str) {
    const CALL: &[&str] = &[
        "bad_sdp",
        "share_taken",
        "not_found",
        "slow_down",
        "media_unavailable",
    ];
    if kind.starts_with("call.") && CALL.contains(&code) {
        let call_id = call_id.unwrap_or("");
        tracing::warn!(call_id, reason = code, "call refused");
        return;
    }
    if matches!(
        code,
        "bad_frame" | "forbidden" | "unknown_type" | "slow_down"
    ) {
        frame_refused(&me.user_id, conn, code);
    }
}

/// Relays typing to the other people in the conversation. The frame is not stored.
async fn typing(hub: &Hub, me: &Me, conversation_id: Option<&str>) -> Answer {
    let Some(conversation_id) = conversation_id else {
        return Err("bad_frame");
    };
    match db::typing_recipients(&hub.pool, &me.caller(), conversation_id).await {
        Ok(ids) => {
            let frame = serde_json::json!({
                "type": "typing",
                "conversation_id": conversation_id,
                "user_id": &me.user_id,
            });
            hub.relay(&ids, &frame.to_string());
            Ok(Done::Ok)
        }
        Err(error) => Err(typing_code(&error)),
    }
}

fn typing_code(error: &db::MessageError) -> &'static str {
    match error {
        db::MessageError::NotFound => "not_found",
        db::MessageError::Forbidden | db::MessageError::Banned | db::MessageError::Hierarchy => {
            "forbidden"
        }
        db::MessageError::Invalid => "bad_frame",
        db::MessageError::TooLarge | db::MessageError::UnsupportedMedia => "bad_frame",
        db::MessageError::Scanning | db::MessageError::Blocked => "forbidden",
        db::MessageError::Unavailable => "unavailable",
        db::MessageError::Store(error) => {
            tracing::warn!(%error, "typing check failed");
            "unavailable"
        }
        db::MessageError::Database(error) => {
            tracing::warn!(%error, "typing check failed");
            "unavailable"
        }
        // The errors of encrypted groups and posts. A typing frame is neither.
        _ => "bad_frame",
    }
}

/// Re-reads the session before a call request. An ended session closes the socket.
async fn fresh(hub: &Hub, me: &mut Me) -> Result<Answer, End> {
    match db::current_session(&hub.pool, &me.token).await {
        Ok(identity) => {
            me.update(identity);
            Ok(Ok(Done::Ok))
        }
        Err(SessionError::Unauthorized) => Err(End::Close(Close::SessionEnded)),
        Err(SessionError::Database(error)) => {
            tracing::warn!(%error, "live session check failed");
            Ok(Err("unavailable"))
        }
    }
}

async fn room_exists(hub: &Hub, me: &Me, room_id: &str) -> Answer {
    match db::room_exists(&hub.pool, &me.caller(), room_id).await {
        Ok(true) => Ok(Done::Ok),
        Ok(false) => Err("not_found"),
        Err(error) => {
            tracing::warn!(%error, "room check failed");
            Err("unavailable")
        }
    }
}

/// What the person may do in the room's call. A room they cannot see is `not_found`, no
/// connect is `forbidden`, a timeout is `timed_out`.
async fn call_access(hub: &Hub, me: &Me, room_id: &str) -> Result<db::CallAccess, &'static str> {
    match db::call_access(&hub.pool, &me.user_id, room_id).await {
        Ok(Some(found)) if found.timed_out => Err("timed_out"),
        Ok(Some(found)) if !db::perm::has(found.permissions, db::perm::CONNECT) => Err("forbidden"),
        Ok(Some(found)) => Ok(found),
        Ok(None) => Err("not_found"),
        Err(error) => {
            tracing::warn!(%error, "call access check failed");
            Err("unavailable")
        }
    }
}

/// A member with connect may join a room they can see, up to its limit. Leaves the person's
/// other call first. With an SFU running the join must carry an offer, and the reply is the
/// answer. Without speak they join suppressed: nothing they send is forwarded.
async fn call_join(
    hub: &Hub,
    me: &mut Me,
    asking: &mut Asking<'_>,
    room_id: Option<&str>,
    sdp: Option<&str>,
    e2ee: bool,
) -> Result<Answer, End> {
    let Some(room_id) = room_id else {
        return Ok(Err("bad_frame"));
    };
    let media = match (&hub.media, sdp) {
        (None, None) => None,
        (Some(_), None) => return Ok(Err("bad_frame")),
        (None, Some(_)) => return Ok(Err(Refused::NoMedia.code())),
        (Some(media), Some(_)) if !media.running() => return Ok(Err(Refused::NoMedia.code())),
        (Some(media), Some(sdp)) => Some((media, sdp)),
    };
    if !asking.moves.take() {
        return Ok(Err("slow_down"));
    }
    if let Err(code) = fresh(hub, me).await? {
        return Ok(Err(code));
    }
    let found = match call_access(hub, me, room_id).await {
        Ok(found) => found,
        Err(code) => return Ok(Err(code)),
    };
    // A room that requires end-to-end encryption takes only apps that can do it. Without
    // media there is nothing to encrypt.
    if media.is_some() && found.e2ee_required && !e2ee {
        return Ok(Err("e2ee_required"));
    }
    if let Err(refused) = hub.call_check(&me.user_id, room_id, found.user_limit) {
        return Ok(Err(refused.code()));
    }
    let answer = match media {
        None => None,
        Some((media, sdp)) => match media.join(room_id, &me.user_id, sdp).await {
            Ok(answer) => Some(answer),
            Err(refused) => return Ok(Err(media_code(refused))),
        },
    };
    let joined = hub.call_join(
        &me.user_id,
        room_id,
        asking.conn,
        answer.as_ref(),
        found.user_limit,
        access(found.permissions),
        e2ee && answer.is_some(),
        me.session,
    );
    if let Err(refused) = joined {
        if let Some(answer) = &answer {
            hub.forget(&answer.call_id);
        }
        return Ok(Err(refused.code()));
    }
    // A delete that committed during the first check was followed before this seat existed.
    if let Err("not_found") = room_exists(hub, me, room_id).await {
        hub.end_room(room_id, Reason::RoomDeleted);
        return Ok(Err("not_found"));
    }
    Ok(Ok(answer.map_or(Done::Ok, Done::Answer)))
}

/// MLS for the caller's call: a key package, a commit, or `invalid`. Only from the socket that
/// holds their seat with media. Key packages and `invalid`s share a small rate of their own.
fn call_e2ee(
    hub: &Hub,
    me: &Me,
    asking: &mut Asking<'_>,
    incoming: &Incoming,
) -> Result<(), &'static str> {
    let Some(op) = incoming.op.as_deref() else {
        return Err("bad_frame");
    };
    if matches!(op, "key_package" | "invalid") && !asking.rekeys.take() {
        return Err("slow_down");
    }
    hub.call_e2ee(
        &me.user_id,
        asking.conn,
        op,
        incoming.epoch,
        incoming.data.as_deref(),
        incoming.welcome.as_deref(),
    )
}

fn media_code(refused: sfu::Refused) -> &'static str {
    match refused {
        sfu::Refused::BadSdp => "bad_sdp",
        sfu::Refused::NotFound => "not_found",
        sfu::Refused::ShareTaken => "share_taken",
        sfu::Refused::Unavailable => Refused::NoMedia.code(),
    }
}

/// Renegotiates the caller's media. The SFU answers, and a camera or share that started or
/// stopped is shown to everyone as `call.state`. A share someone else holds is
/// `share_taken`.
async fn call_offer(hub: &Hub, me: &Me, asking: &mut Asking<'_>, sdp: Option<&str>) -> Answer {
    let Some(sdp) = sdp else {
        return Err("bad_frame");
    };
    let Some(media) = &hub.media else {
        return Err(Refused::NoMedia.code());
    };
    if !asking.moves.take() {
        return Err("slow_down");
    }
    let Some(call_id) = hub.call_of(&me.user_id) else {
        return Err("not_found");
    };
    let answer = media.offer(&call_id, sdp).await.map_err(media_code)?;
    hub.call_media(&me.user_id, &answer);
    Ok(Done::Answer(answer))
}

/// What the caller's one video slot may carry. Not being in a call with media is
/// `not_found`.
fn call_quality(hub: &Hub, me: &Me, video: Option<&str>) -> Answer {
    let video = match video {
        Some("high") => sfu::Quality::High,
        Some("low") => sfu::Quality::Low,
        Some("off") => sfu::Quality::Off,
        _ => return Err("bad_frame"),
    };
    let (Some(media), Some(call_id)) = (&hub.media, hub.call_of(&me.user_id)) else {
        return Err("not_found");
    };
    media.quality(&call_id, video);
    Ok(Done::Ok)
}

/// Mute, deafen, and priority, shown to everyone. Priority counts only with
/// priority_speaker. Not being in a call is `not_found`.
fn call_state(hub: &Hub, me: &Me, incoming: &Incoming) -> Answer {
    let (Some(muted), Some(deafened)) = (incoming.muted, incoming.deafened) else {
        return Err("bad_frame");
    };
    let priority = incoming.priority.unwrap_or(false);
    if !hub.call_state(&me.user_id, muted, deafened, priority) {
        return Err("not_found");
    }
    Ok(Done::Ok)
}

/// Leaving with no call is not an error.
fn call_leave(hub: &Hub, me: &Me, asking: &mut Asking<'_>) -> Answer {
    if !asking.moves.take() {
        return Err("slow_down");
    }
    hub.call_leave(&me.user_id, Reason::Left);
    Ok(Done::Ok)
}

/// The database's answer to a moderation check, as a frame error code.
fn moderation_code(error: CommunityError, what: &str) -> &'static str {
    match error {
        CommunityError::Forbidden => "forbidden",
        CommunityError::Hierarchy => "hierarchy",
        CommunityError::NotFound => "not_found",
        CommunityError::Invalid => "bad_frame",
        error => {
            tracing::warn!(%error, what, "call moderation failed");
            "unavailable"
        }
    }
}

/// disconnect_members in the room and a rank above the person.
async fn call_kick(
    hub: &Hub,
    me: &mut Me,
    room_id: Option<&str>,
    user_id: Option<&str>,
) -> Result<Answer, End> {
    let (Some(room_id), Some(user_id)) = (room_id, user_id) else {
        return Ok(Err("bad_frame"));
    };
    if let Err(code) = fresh(hub, me).await? {
        return Ok(Err(code));
    }
    if !hub.in_call(room_id, user_id) {
        return Ok(Err("not_found"));
    }
    if let Err(error) = db::kick_from_call(&hub.pool, &me.caller(), room_id, user_id).await {
        return Ok(Err(moderation_code(error, "kick")));
    }
    hub.call_kick(room_id, user_id);
    Ok(Ok(Done::Ok))
}

/// Server mute needs mute_members and server deafen deafen_members in the room, and a rank
/// above the person. It lasts until undone or they leave calls altogether.
async fn call_moderate(hub: &Hub, me: &mut Me, incoming: &Incoming) -> Result<Answer, End> {
    let (Some(room_id), Some(user_id)) = (incoming.room_id.as_deref(), incoming.user_id.as_deref())
    else {
        return Ok(Err("bad_frame"));
    };
    let (server_muted, server_deafened) = (incoming.server_muted, incoming.server_deafened);
    if server_muted.is_none() && server_deafened.is_none() {
        return Ok(Err("bad_frame"));
    }
    if let Err(code) = fresh(hub, me).await? {
        return Ok(Err(code));
    }
    if !hub.in_call(room_id, user_id) {
        return Ok(Err("not_found"));
    }
    let checked = db::moderate_call(
        &hub.pool,
        &me.caller(),
        room_id,
        user_id,
        server_muted,
        server_deafened,
    )
    .await;
    if let Err(error) = checked {
        return Ok(Err(moderation_code(error, "moderate")));
    }
    if !hub.moderate(room_id, user_id, server_muted, server_deafened) {
        return Ok(Err("not_found"));
    }
    Ok(Ok(Done::Ok))
}

/// move_members in both rooms and a rank above the person. The person gets `call.moved` and
/// joins the room themselves, past its limit.
async fn call_move(
    hub: &Hub,
    me: &mut Me,
    room_id: Option<&str>,
    user_id: Option<&str>,
) -> Result<Answer, End> {
    let (Some(to_room), Some(user_id)) = (room_id, user_id) else {
        return Ok(Err("bad_frame"));
    };
    if let Err(code) = fresh(hub, me).await? {
        return Ok(Err(code));
    }
    let Some(from_room) = hub.room_of(user_id) else {
        return Ok(Err("not_found"));
    };
    if from_room == to_room {
        return Ok(Ok(Done::Ok));
    }
    let checked = db::move_in_call(&hub.pool, &me.caller(), &from_room, to_room, user_id).await;
    if let Err(error) = checked {
        return Ok(Err(moderation_code(error, "move")));
    }
    if !hub.call_move(user_id, to_room) {
        return Ok(Err("not_found"));
    }
    Ok(Ok(Done::Ok))
}

/// Ending the screen share in a room: disconnect_members there and a rank above the sharer.
async fn call_share_end(hub: &Hub, me: &mut Me, room_id: Option<&str>) -> Result<Answer, End> {
    let Some(room_id) = room_id else {
        return Ok(Err("bad_frame"));
    };
    if let Err(code) = fresh(hub, me).await? {
        return Ok(Err(code));
    }
    let Some(user_id) = hub.sharer(room_id) else {
        return Ok(Err("not_found"));
    };
    if let Err(error) = db::end_share(&hub.pool, &me.caller(), room_id, &user_id).await {
        return Ok(Err(moderation_code(error, "share end")));
    }
    hub.end_share(room_id, &user_id);
    Ok(Ok(Done::Ok))
}

fn is_too_big(error: &axum::Error) -> bool {
    use std::error::Error as _;
    let mut source = error.source();
    while let Some(inner) = source {
        if let Some(tungstenite) = inner.downcast_ref::<tokio_tungstenite::tungstenite::Error>() {
            return matches!(
                tungstenite,
                tokio_tungstenite::tungstenite::Error::Capacity(_)
            );
        }
        source = inner.source();
    }
    false
}

/// Forwards one ephemeral frame when this socket's scopes allow it.
async fn send_visible(hub: &Hub, socket: &mut WebSocket, me: &Me, frame: &str) -> Step {
    if me.sees_ephemeral(frame) {
        send(hub, socket, frame.to_string()).await
    } else {
        Ok(())
    }
}

/// A send that does not finish within the limit drops the socket. Nothing queues behind it
/// except the bounded broadcast backlog, and a socket that falls past that gets `resync`.
async fn send(hub: &Hub, socket: &mut WebSocket, text: String) -> Step {
    match tokio::time::timeout(hub.limits.send_timeout, socket.send(Message::text(text))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(End::Gone),
        Err(_) => {
            tracing::info!("live socket too slow; dropped");
            Err(End::Gone)
        }
    }
}

enum Take {
    Allowed,
    /// The first message over the rate. The client is told once.
    Warn,
    Dropped,
    /// A burst's worth dropped in a row.
    Exceeded,
}

/// Client messages: `rate` a second with a `burst` above that.
struct Bucket {
    rate: f64,
    burst: f64,
    tokens: f64,
    at: Instant,
    dropped: u32,
}

impl Bucket {
    fn new(rate: u32, burst: u32) -> Self {
        Self {
            rate: f64::from(rate.max(1)),
            burst: f64::from(burst.max(1)),
            tokens: f64::from(burst.max(1)),
            at: Instant::now(),
            dropped: 0,
        }
    }

    fn take(&mut self) -> Take {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.at).as_secs_f64();
        self.at = now;
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            self.dropped = 0;
            return Take::Allowed;
        }
        self.dropped += 1;
        if f64::from(self.dropped) >= self.burst {
            Take::Exceeded
        } else if self.dropped == 1 {
            Take::Warn
        } else {
            Take::Dropped
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Bucket, Close, Take};

    #[test]
    fn a_burst_passes_then_one_warning_then_a_close() {
        let mut bucket = Bucket::new(1, 3);
        for _ in 0..3 {
            assert!(matches!(bucket.take(), Take::Allowed));
        }
        assert!(matches!(bucket.take(), Take::Warn));
        assert!(matches!(bucket.take(), Take::Dropped));
        assert!(matches!(bucket.take(), Take::Exceeded));
    }

    #[test]
    fn close_codes_match_the_design() {
        assert_eq!(Close::SessionEnded.frame().code, 4001);
        assert_eq!(Close::Removed.frame().code, 4003);
        assert_eq!(Close::Resync.frame().code, 4002);
        assert_eq!(Close::Restart.frame().code, 1012);
    }

    fn bot(scopes: &[&str]) -> super::Me {
        super::Me {
            session: [0; 32],
            token: String::new(),
            user_id: "b".to_string(),
            role: "bot".to_string(),
            permissions: 0,
            scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
        }
    }

    #[test]
    fn a_bot_scope_gates_room_and_member_frames() {
        let rooms = bot(&["rooms.read"]);
        assert!(rooms.sees_topic("room.created"));
        assert!(!rooms.sees_topic("member.joined"));
        assert!(rooms.sees_ephemeral(r#"{"type":"call.joined","room_id":"r"}"#));
        assert!(!rooms.sees_ephemeral(r#"{"type":"presence","user_id":"u","online":true}"#));
        assert_eq!(rooms.caller().scopes, vec!["rooms.read".to_string()]);

        let members = bot(&["members.read"]);
        assert!(!members.sees_topic("room.deleted"));
        assert!(members.sees_topic("member.renamed"));
        assert!(members.sees_ephemeral(r#"{"type":"presence","user_id":"u","online":false}"#));
        assert!(!members.sees_ephemeral(r#"{"type":"call.left","room_id":"r"}"#));

        let unknown = bot(&["rooms.write"]);
        assert!(!unknown.sees_topic("room.created"));
        assert!(!unknown.sees_topic("member.removed"));
        assert!(!unknown.sees_topic("message.created"));
        assert!(!unknown.sees_topic("future.secret"));
        assert!(!unknown.sees_ephemeral(r#"{"type":"message.created"}"#));
        assert!(!unknown.sees_ephemeral(r#"{"type":"future.secret"}"#));

        let person = super::Me {
            session: [0; 32],
            token: String::new(),
            user_id: "u".to_string(),
            role: "member".to_string(),
            permissions: 0,
            scopes: Vec::new(),
        };
        assert!(person.sees_topic("room.created"));
        assert!(person.sees_ephemeral(r#"{"type":"presence","user_id":"u","online":true}"#));
        assert!(person.caller().scopes.is_empty());
        assert!(super::changes_call("call.join"));
        assert!(super::changes_call("call.offer"));
        assert!(super::changes_call("call.moderate"));
        assert!(super::changes_call("call.move"));
        assert!(super::changes_call("call.e2ee"));
        assert!(!super::changes_call("ping"));
    }
}
