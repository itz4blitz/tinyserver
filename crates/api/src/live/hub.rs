//! Who is connected, who is online, who is in which call, and the one task that reads the
//! outbox.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use db::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::{broadcast, mpsc, oneshot, watch, Notify};
use tokio::task::JoinHandle;

use super::call::{Access, Calls, Change, Moderation, Occupancy, Reason, Refused, Session};
use super::e2ee;
use super::frame::{Event, Out, SlotFrame, VideoSlotFrame};
use super::socket::Close;
use super::Limits;

/// Events a socket may fall behind by before it gets `resync`.
const BACKLOG: usize = 1024;
/// Outbox rows read per query.
const BATCH: i64 = 256;
/// Frames for one socket alone, such as `call.slots`. A socket this far behind misses some.
const DIRECT: usize = 64;

pub(crate) struct Hub {
    pub pool: PgPool,
    pub limits: Limits,
    pub events: broadcast::Sender<Arc<Event>>,
    /// Presence and call frames, already serialised. Every member gets every one.
    ephemeral: broadcast::Sender<Arc<str>>,
    people: Mutex<People>,
    open: AtomicUsize,
    drained: Notify,
    stop: watch::Sender<bool>,
    dispatcher: Mutex<Option<JoinHandle<()>>>,
    ready: watch::Sender<bool>,
    pub media: Option<sfu::Media>,
    /// People connected since `last_seen_at` was last written.
    seen: Mutex<HashSet<String>>,
    /// The operator's suspension, for what a socket may still do.
    pub operator: crate::Operator,
}

#[derive(Default)]
struct People {
    users: HashMap<String, Person>,
    next_conn: u64,
    calls: Calls,
    /// The latest `call.slots` frame for each SFU participant, seated or not yet.
    slots: HashMap<String, Arc<str>>,
    /// End-to-end encryption: the MLS delivery service for each room's call.
    e2ee: e2ee::Rooms,
}

impl People {
    /// Sends `frame` to socket `conn` of `user_id` alone. Dropped if that socket is behind.
    fn direct(&self, user_id: &str, conn: u64, frame: Arc<str>) {
        let Some(person) = self.users.get(user_id) else {
            return;
        };
        if let Some(found) = person.conns.iter().find(|entry| entry.id == conn) {
            let _ = found.direct.try_send(frame);
        }
    }

    /// Every socket of `user_id`. A socket that is behind misses the frame.
    fn fanout(&self, user_id: &str, frame: Arc<str>) {
        let Some(person) = self.users.get(user_id) else {
            return;
        };
        for conn in &person.conns {
            let _ = conn.direct.try_send(Arc::clone(&frame));
        }
    }

    /// Sends `frame` to the socket holding `user_id`'s seat.
    fn to_seat(&self, user_id: &str, frame: Arc<str>) {
        if let Some(conn) = self.calls.conn_of(user_id) {
            self.direct(user_id, conn, frame);
        }
    }

    /// Sends `call.e2ee` frames, each to the socket holding its person's seat, or keeps them
    /// for the next one while that seat has none.
    fn e2ee(&mut self, frames: Vec<e2ee::Outgoing>) {
        for outgoing in frames {
            let frame: Arc<str> = Arc::from(outgoing.frame);
            if !self.calls.hold(&outgoing.user_id, &frame) {
                self.to_seat(&outgoing.user_id, frame);
            }
        }
    }
}

/// Online while this exists. It outlives the last socket by the presence grace.
#[derive(Default)]
struct Person {
    bot: bool,
    conns: Vec<Conn>,
    epoch: u64,
}

struct Conn {
    id: u64,
    kick: oneshot::Sender<Close>,
    direct: mpsc::Sender<Arc<str>>,
}

/// What a socket gets when it joins the hub.
pub(crate) struct Joined {
    pub conn: u64,
    pub kicked: oneshot::Receiver<Close>,
    pub presence: broadcast::Receiver<Arc<str>>,
    /// Frames for this socket alone.
    pub direct: mpsc::Receiver<Arc<str>>,
    pub online: Vec<String>,
    pub calls: Vec<Occupancy>,
}

/// One reserved socket. Dropping it frees the place.
pub(crate) struct Slot {
    hub: Arc<Hub>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.hub.open.fetch_sub(1, Ordering::AcqRel);
        self.hub.drained.notify_waiters();
    }
}

impl Hub {
    pub fn start(
        pool: PgPool,
        limits: Limits,
        media: Option<(sfu::Media, mpsc::Receiver<sfu::Event>)>,
    ) -> Arc<Self> {
        let (media, media_events) = match media {
            Some((media, events)) => (Some(media), Some(events)),
            None => (None, None),
        };
        let (events, _) = broadcast::channel(BACKLOG);
        let (ephemeral, _) = broadcast::channel(BACKLOG);
        let (stop, _) = watch::channel(false);
        let hub = Arc::new(Self {
            operator: crate::Operator::new(pool.clone()),
            pool,
            limits,
            events,
            ephemeral,
            people: Mutex::new(People::default()),
            open: AtomicUsize::new(0),
            drained: Notify::new(),
            stop,
            dispatcher: Mutex::new(None),
            ready: watch::channel(false).0,
            media,
            seen: Mutex::new(HashSet::new()),
        });
        let task = tokio::spawn(dispatch(hub.clone(), hub.stop.subscribe()));
        *lock(&hub.dispatcher) = Some(task);
        if let Some(events) = media_events {
            tokio::spawn(follow_media(hub.clone(), events, hub.stop.subscribe()));
        }
        tokio::spawn(remember(hub.clone(), hub.stop.subscribe()));
        tokio::spawn(rekey(hub.clone(), hub.stop.subscribe()));
        hub
    }

    /// `user_id` is connected now. Written to `last_seen_at` with the next batch.
    pub fn saw(&self, user_id: &str) {
        lock(&self.seen).insert(user_id.to_string());
    }

    /// Also ends every call and stops the SFU. The `1012` close is how callers learn it.
    pub fn close(&self) {
        self.stop.send_replace(true);
        let mut people = lock(&self.people);
        people.calls.clear();
        people.slots.clear();
        people.e2ee.clear();
        if let Some(media) = &self.media {
            media.shutdown();
        }
    }

    /// `off` without an SFU, `up` while it runs, `down` once it stopped.
    pub fn media_status(&self) -> &'static str {
        match &self.media {
            None => "off",
            Some(media) if media.running() => "up",
            Some(_) => "down",
        }
    }

    /// Do not establish a socket cursor before the dispatcher has its starting cursor.
    pub async fn ready(&self) {
        let _ = self.ready.subscribe().wait_for(|ready| *ready).await;
    }

    pub fn closing(&self) -> bool {
        *self.stop.borrow()
    }

    pub fn stopping(&self) -> watch::Receiver<bool> {
        self.stop.subscribe()
    }

    pub fn sockets(&self) -> usize {
        self.open.load(Ordering::Acquire)
    }

    pub async fn closed(&self, timeout: Duration) -> bool {
        let task = lock(&self.dispatcher).take();
        tokio::time::timeout(timeout, async {
            if let Some(task) = task {
                let _ = task.await;
            }
            loop {
                let drained = self.drained.notified();
                if self.sockets() == 0 {
                    break;
                }
                drained.await;
            }
        })
        .await
        .is_ok()
    }

    pub fn reserve(self: &Arc<Self>) -> Option<Slot> {
        let before = self.open.fetch_add(1, Ordering::AcqRel);
        let slot = Slot { hub: self.clone() };
        (before < self.limits.sockets_total).then_some(slot)
    }

    /// Adds a socket for `user_id`. The first one brings the person online. Past the per-user
    /// cap, the oldest socket is told to close. If `session` holds their seat, the seat moves
    /// here, and what the socket reads after `ready` catches it up.
    pub fn join(&self, user_id: &str, bot: bool, session: Session) -> Joined {
        let (kick, kicked) = oneshot::channel();
        let (direct, inbox) = mpsc::channel(DIRECT);
        let mut people = lock(&self.people);
        people.next_conn += 1;
        let conn = people.next_conn;
        let arrived = !people.users.contains_key(user_id);
        let person = people.users.entry(user_id.to_string()).or_default();
        person.bot = bot;
        while person.conns.len() >= self.limits.sockets_per_user.max(1) {
            let oldest = person.conns.remove(0);
            let _ = oldest.kick.send(Close::Replaced);
        }
        person.conns.push(Conn {
            id: conn,
            kick,
            direct,
        });
        if arrived && !bot {
            self.announce(user_id, true);
        }
        if let Some(regained) = people.calls.regain(user_id, session, conn) {
            tracing::info!(conn, "call seat regained");
            let slots = regained
                .call_id
                .and_then(|call_id| people.slots.get(&call_id).cloned());
            for frame in slots.into_iter().chain(regained.frames) {
                people.direct(user_id, conn, frame);
            }
        }
        // Subscribed under the lock, so every presence frame after this snapshot reaches it.
        let presence = self.ephemeral.subscribe();
        let mut online: Vec<String> = people
            .users
            .iter()
            .filter(|(_, person)| !person.bot)
            .map(|(id, _)| id.clone())
            .collect();
        online.sort();
        Joined {
            conn,
            kicked,
            presence,
            direct: inbox,
            online,
            calls: people.calls.snapshot(),
        }
    }

    /// Removes a socket. When it was the person's last, they go offline after the grace.
    pub fn leave(self: &Arc<Self>, user_id: &str, conn: u64) {
        let epoch = {
            let mut people = lock(&self.people);
            let Some(person) = people.users.get_mut(user_id) else {
                return;
            };
            let had = person.conns.len();
            person.conns.retain(|entry| entry.id != conn);
            if person.conns.len() == had || !person.conns.is_empty() {
                return;
            }
            person.epoch += 1;
            person.epoch
        };
        let hub = self.clone();
        let user_id = user_id.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(hub.limits.presence_grace).await;
            let mut people = lock(&hub.people);
            let gone = people
                .users
                .get(&user_id)
                .is_some_and(|person| person.conns.is_empty() && person.epoch == epoch);
            if gone {
                let person = people.users.remove(&user_id);
                if person.is_some_and(|person| !person.bot) {
                    hub.announce(&user_id, false);
                }
            }
        });
    }

    fn announce(&self, user_id: &str, online: bool) {
        let frame = Out::Presence { user_id, online }.text();
        let _ = self.ephemeral.send(Arc::from(frame));
    }

    /// How many people `room_id` takes for `user_id`: its own limit, else the server's, and
    /// the server's for someone moved there. Never more than the server's.
    fn room_limit(&self, calls: &Calls, user_id: &str, room_id: &str, user_limit: i32) -> usize {
        let max = self.limits.call_room_max;
        match usize::try_from(user_limit) {
            Ok(limit) if limit > 0 && !calls.ticket(user_id, room_id) => limit.min(max),
            _ => max,
        }
    }

    /// Whether `call_join` would seat `user_id` in `room_id` now. Checked before media is
    /// set up, because an SFU join replaces the person's media in any other room.
    pub fn call_check(&self, user_id: &str, room_id: &str, user_limit: i32) -> Result<(), Refused> {
        if self.closing() {
            return Err(Refused::Closing);
        }
        let people = lock(&self.people);
        let limit = self.room_limit(&people.calls, user_id, room_id, user_limit);
        people
            .calls
            .check(user_id, room_id, limit, self.limits.call_server_max)
    }

    /// Seats `user_id` in `room_id` for socket `conn`, with the SFU's answer if the join had
    /// media. `e2ee`: their app can encrypt end to end. Refused once the hub is closing.
    #[allow(clippy::too_many_arguments)]
    pub fn call_join(
        &self,
        user_id: &str,
        room_id: &str,
        conn: u64,
        answer: Option<&sfu::Answer>,
        user_limit: i32,
        access: Access,
        e2ee: bool,
        session: Session,
    ) -> Result<(), Refused> {
        let call_id = answer.map(|answer| answer.call_id.as_str());
        let mut people = lock(&self.people);
        if self.closing() {
            return Err(Refused::Closing);
        }
        let limit = self.room_limit(&people.calls, user_id, room_id, user_limit);
        let again = people.calls.room_of(user_id) == Some(room_id);
        let changes = people.calls.join(
            user_id,
            room_id,
            conn,
            call_id.map(str::to_string),
            limit,
            self.limits.call_server_max,
            access,
            e2ee,
        )?;
        people.calls.set_session(user_id, session);
        self.tell(&mut people, changes);
        // A seat with media is in the room's group, or is why the room has none. Taking the
        // same seat again with new media is a leave and a join: the old keys went with it.
        if answer.is_some() {
            if again {
                let frames = people
                    .e2ee
                    .leave(room_id, user_id, std::time::Instant::now());
                people.e2ee(frames);
            }
            let frames = people.e2ee.join(room_id, user_id, e2ee);
            people.e2ee(frames);
        }
        if let Some(answer) = answer {
            let change =
                people
                    .calls
                    .set_media(user_id, &answer.call_id, answer.camera, answer.share);
            self.tell(&mut people, change);
        }
        // A new SFU participant starts unmoderated.
        if let Some(moderation) = people.calls.moderation(user_id) {
            if moderation.silenced || moderation.hushed {
                self.apply(Some(moderation));
            }
        }
        // Slots the SFU sent before the seat existed. The socket reads them after its answer.
        if let Some(frame) = call_id.and_then(|call_id| people.slots.get(call_id)) {
            people.direct(user_id, conn, frame.clone());
        }
        Ok(())
    }

    /// Drops an SFU participant that never got a seat.
    pub fn forget(&self, call_id: &str) {
        let mut people = lock(&self.people);
        people.slots.remove(call_id);
        if let Some(media) = &self.media {
            media.leave(call_id);
        }
    }

    /// `user_id`'s SFU participant, if they are in a call with media.
    pub fn call_of(&self, user_id: &str) -> Option<String> {
        lock(&self.people)
            .calls
            .call_of(user_id)
            .map(str::to_string)
    }

    /// Records mute, deafen, and priority, tells everyone, and tells the SFU. False if not in
    /// a call.
    pub fn call_state(&self, user_id: &str, muted: bool, deafened: bool, priority: bool) -> bool {
        let mut people = lock(&self.people);
        if people.calls.room_of(user_id).is_none() {
            return false;
        }
        let change = people.calls.set_state(user_id, muted, deafened, priority);
        if let (Some(media), Some(call_id)) = (&self.media, people.calls.call_of(user_id)) {
            media.state(call_id, muted, deafened);
        }
        self.tell(&mut people, change);
        true
    }

    /// Server mute or deafen `user_id` in `room_id`'s call, tell everyone, and tell the SFU.
    /// False if they are not in that call.
    pub fn moderate(
        &self,
        room_id: &str,
        user_id: &str,
        server_muted: Option<bool>,
        server_deafened: Option<bool>,
    ) -> bool {
        let mut people = lock(&self.people);
        let Some((change, moderation)) =
            people
                .calls
                .moderate(room_id, user_id, server_muted, server_deafened)
        else {
            return false;
        };
        self.apply(moderation);
        self.tell(&mut people, change);
        true
    }

    /// Tells `user_id` to move to `to_room`, whose limit they may pass for a while. False if
    /// they are not in a call.
    pub fn call_move(&self, user_id: &str, to_room: &str) -> bool {
        let mut people = lock(&self.people);
        if people.calls.room_of(user_id).is_none() {
            return false;
        }
        people.calls.issue_ticket(user_id, to_room);
        let frame: Arc<str> = Arc::from(Out::CallMoved { room_id: to_room }.text());
        people.to_seat(user_id, frame);
        true
    }

    /// The room whose call `user_id` is in.
    pub fn room_of(&self, user_id: &str) -> Option<String> {
        lock(&self.people)
            .calls
            .room_of(user_id)
            .map(str::to_string)
    }

    fn apply(&self, moderation: Option<Moderation>) {
        if let (Some(media), Some(moderation)) = (&self.media, moderation) {
            media.moderate(&moderation.call_id, moderation.silenced, moderation.hushed);
        }
    }

    /// Re-reads the permissions of everyone in a call, after a role or an override changed,
    /// and suppresses or frees them and their priority to match.
    fn refresh(self: &Arc<Self>) {
        let hub = self.clone();
        tokio::spawn(async move {
            let rooms = lock(&hub.people).calls.occupied();
            for (room_id, people) in rooms {
                let permissions = match db::room_permissions(&hub.pool, &room_id, &people).await {
                    Ok(permissions) => permissions,
                    Err(error) => {
                        tracing::warn!(%error, "call permissions not read");
                        continue;
                    }
                };
                let mut guard = lock(&hub.people);
                for (user_id, bits) in permissions {
                    if let Some((change, moderation)) =
                        guard.calls.set_access(&room_id, &user_id, access(bits))
                    {
                        hub.apply(moderation);
                        hub.tell(&mut guard, Some(change));
                    }
                }
                drop(guard);
            }
        });
    }

    /// Records whether `user_id`'s camera sends and whether they share after `answer`, and
    /// tells everyone if either changed.
    pub fn call_media(&self, user_id: &str, answer: &sfu::Answer) {
        let mut people = lock(&self.people);
        let change = people
            .calls
            .set_media(user_id, &answer.call_id, answer.camera, answer.share);
        self.tell(&mut people, change);
    }

    /// Who shares their screen in `room_id`'s call.
    pub fn sharer(&self, room_id: &str) -> Option<String> {
        lock(&self.people).calls.sharer(room_id).map(str::to_string)
    }

    /// Ends `user_id`'s share in `room_id`, tells everyone, and stops it in the SFU. False if
    /// they were not sharing there.
    pub fn end_share(&self, room_id: &str, user_id: &str) -> bool {
        let mut people = lock(&self.people);
        let Some((call_id, change)) = people.calls.end_share(room_id, user_id) else {
            return false;
        };
        if let (Some(media), Some(call_id)) = (&self.media, call_id) {
            media.end_share(&call_id);
        }
        self.tell(&mut people, Some(change));
        true
    }

    /// Ends `user_id`'s call, if they are in one.
    pub fn call_leave(&self, user_id: &str, reason: Reason) {
        let mut people = lock(&self.people);
        let change = people.calls.leave(user_id, reason);
        self.tell(&mut people, change);
    }

    /// Ends `user_id`'s call if it is in `room_id`. False if they were not there.
    pub fn call_kick(&self, room_id: &str, user_id: &str) -> bool {
        let mut people = lock(&self.people);
        if people.calls.room_of(user_id) != Some(room_id) {
            return false;
        }
        let change = people.calls.leave(user_id, Reason::Kicked);
        self.tell(&mut people, change);
        true
    }

    pub fn in_call(&self, room_id: &str, user_id: &str) -> bool {
        lock(&self.people).calls.room_of(user_id) == Some(room_id)
    }

    pub fn end_room(&self, room_id: &str, reason: Reason) {
        let mut people = lock(&self.people);
        let changes = people.calls.end_room(room_id, reason);
        self.tell(&mut people, changes);
    }

    /// Socket `conn` closed with `reason`. If it held its person's seat, they leave now, or
    /// after the presence grace when `reason` is `None` and no socket of theirs rejoins first.
    pub fn hang_up(self: &Arc<Self>, user_id: &str, conn: u64, reason: Option<Reason>) {
        let epoch = {
            let mut people = lock(&self.people);
            let Some(epoch) = people.calls.orphan(user_id, conn) else {
                return;
            };
            if let Some(reason) = reason {
                let change = people.calls.leave(user_id, reason);
                self.tell(&mut people, change);
                return;
            }
            epoch
        };
        let hub = self.clone();
        let user_id = user_id.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(hub.limits.presence_grace).await;
            let mut people = lock(&hub.people);
            let change = people.calls.expire(&user_id, epoch);
            hub.tell(&mut people, change);
        });
    }

    /// Acts on durable events that end calls or change what people in them may do.
    fn follow(self: &Arc<Self>, event: &Event) {
        match event.topic.as_str() {
            "room.deleted" => {
                if let Some(room_id) = &event.room {
                    self.end_room(room_id, Reason::RoomDeleted);
                }
            }
            "member.removed" => {
                if let Some(user_id) = &event.subject {
                    self.call_leave(user_id, Reason::Removed);
                }
            }
            "member.banned" => {
                if let Some(user_id) = &event.subject {
                    self.call_leave(user_id, Reason::Banned);
                }
            }
            "member.timeout" if event.timed_out => {
                if let Some(user_id) = &event.subject {
                    self.call_leave(user_id, Reason::TimedOut);
                }
            }
            "role.updated" | "role.deleted" | "member.roles" | "member.role" | "room.updated" => {
                self.refresh();
            }
            _ => {}
        }
    }

    /// Sends call changes, and drops the media of anyone who left. Called with `people`
    /// locked, so frames go out in registry order.
    fn tell(&self, people: &mut People, changes: impl IntoIterator<Item = Change>) {
        for change in changes {
            let frame = match &change {
                Change::Joined {
                    room_id,
                    participant,
                } => Out::CallJoined {
                    room_id,
                    participant,
                }
                .text(),
                Change::Left {
                    room_id,
                    user_id,
                    reason,
                    call_id,
                } => {
                    if let Some(call_id) = call_id {
                        people.slots.remove(call_id);
                        if let Some(media) = &self.media {
                            media.leave(call_id);
                        }
                    }
                    let frames = people
                        .e2ee
                        .leave(room_id, user_id, std::time::Instant::now());
                    people.e2ee(frames);
                    Out::CallLeft {
                        room_id,
                        user_id,
                        reason: reason.as_str(),
                    }
                    .text()
                }
                Change::State {
                    room_id,
                    participant,
                } => Out::CallState {
                    room_id,
                    participant,
                }
                .text(),
            };
            let _ = self.ephemeral.send(Arc::from(frame));
        }
    }

    /// One `call.e2ee` from socket `conn` of `user_id`. Only the socket holding a seat with
    /// media may send one.
    pub fn call_e2ee(
        &self,
        user_id: &str,
        conn: u64,
        op: &str,
        epoch: Option<u64>,
        data: Option<&str>,
        welcome: Option<&str>,
    ) -> Result<(), &'static str> {
        let mut people = lock(&self.people);
        let room_id = match people.calls.room_of(user_id) {
            Some(room_id)
                if people.calls.conn_of(user_id) == Some(conn)
                    && people.calls.call_of(user_id).is_some() =>
            {
                room_id.to_string()
            }
            _ => return Err(e2ee::Refused::NotFound.code()),
        };
        let now = std::time::Instant::now();
        let sent = match op {
            "key_package" => {
                let data = e2ee::decode(data).map_err(e2ee::Refused::code)?;
                people.e2ee.key_package(&room_id, user_id, &data, now)
            }
            "commit" => {
                let Some(epoch) = epoch else {
                    return Err(e2ee::Refused::BadFrame.code());
                };
                let data = e2ee::decode(data).map_err(e2ee::Refused::code)?;
                let welcome = match welcome {
                    Some(welcome) => {
                        Some(e2ee::decode(Some(welcome)).map_err(e2ee::Refused::code)?)
                    }
                    None => None,
                };
                people
                    .e2ee
                    .commit(&room_id, user_id, epoch, &data, welcome.as_deref(), now)
            }
            "invalid" => people.e2ee.invalid(&room_id, user_id, now),
            _ => return Err(e2ee::Refused::BadFrame.code()),
        };
        let frames = sent.map_err(e2ee::Refused::code)?;
        people.e2ee(frames);
        Ok(())
    }

    /// Acts on what the SFU reports.
    fn hear(&self, event: sfu::Event) {
        let mut people = lock(&self.people);
        match event {
            sfu::Event::Connected { call_id } => {
                tracing::debug!(%call_id, "media connected");
            }
            sfu::Event::Slots {
                call_id,
                audio,
                video,
            } => {
                let slots: Vec<SlotFrame<'_>> = audio
                    .iter()
                    .map(|slot| SlotFrame {
                        mid: &slot.mid,
                        user_id: &slot.user_id,
                    })
                    .collect();
                let video = video.as_ref().map(|slot| VideoSlotFrame {
                    mid: &slot.mid,
                    user_id: &slot.user_id,
                    kind: slot.kind.as_str(),
                });
                let frame: Arc<str> = Arc::from(
                    Out::CallSlots {
                        call_id: &call_id,
                        audio: &slots,
                        video,
                    }
                    .text(),
                );
                if let Some(user_id) = people.calls.holder(&call_id) {
                    people.to_seat(user_id, frame.clone());
                }
                people.slots.insert(call_id, frame);
            }
            sfu::Event::Speaking { room_id, user_ids } => {
                let frame: Arc<str> = Arc::from(
                    Out::CallSpeaking {
                        room_id: &room_id,
                        user_ids: &user_ids,
                    }
                    .text(),
                );
                for user_id in people.calls.people(&room_id) {
                    people.to_seat(user_id, frame.clone());
                }
            }
            sfu::Event::Left { call_id, lost } => {
                people.slots.remove(&call_id);
                let Some(user_id) = people.calls.holder(&call_id).map(str::to_string) else {
                    return;
                };
                let reason = match lost {
                    sfu::Lost::Timeout => Reason::MediaTimeout,
                    sfu::Lost::Gone => Reason::MediaLost,
                };
                let change = people.calls.leave(&user_id, reason);
                self.tell(&mut people, change);
            }
            sfu::Event::Load {
                egress_kbps,
                participants,
            } => {
                tracing::debug!(egress_kbps, participants, "media load");
            }
        }
    }
}

/// Writes who was connected to `last_seen_at` every `seen_every`, in one query, and once more
/// when the hub closes.
async fn remember(hub: Arc<Hub>, mut stop: watch::Receiver<bool>) {
    let mut ticks = tokio::time::interval(hub.limits.seen_every);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let last = tokio::select! {
            _ = stop.wait_for(|stopped| *stopped) => true,
            _ = ticks.tick() => false,
        };
        let users: Vec<String> = lock(&hub.seen).drain().collect();
        if let Err(error) = db::touch_seen(&hub.pool, &users).await {
            tracing::debug!(%error, "last seen not recorded");
        }
        if last {
            return;
        }
    }
}

/// Builds again any E2EE group whose batch of proposals got no commit in time.
async fn rekey(hub: Arc<Hub>, mut stop: watch::Receiver<bool>) {
    let mut ticks = tokio::time::interval(Duration::from_secs(1));
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = stop.wait_for(|stopped| *stopped) => return,
            _ = ticks.tick() => {}
        }
        let people = &mut *lock(&hub.people);
        let frames = people.e2ee.tick(std::time::Instant::now());
        people.e2ee(frames);
    }
}

/// Feeds SFU events to the hub until it closes or the SFU stops.
async fn follow_media(
    hub: Arc<Hub>,
    mut events: mpsc::Receiver<sfu::Event>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = stop.wait_for(|stopped| *stopped) => return,
            event = events.recv() => match event {
                Some(event) => hub.hear(event),
                None => {
                    tracing::warn!("media stopped");
                    return;
                }
            },
        }
    }
}

/// Reads outbox rows past the last one sent, in seq order, on every wake and every poll.
/// Ids are handed out in commit order, so a cursor in memory never skips a row.
async fn dispatch(hub: Arc<Hub>, mut stop: watch::Receiver<bool>) {
    let mut cursor = loop {
        match db::outbox_bounds(&hub.pool).await {
            Ok(bounds) => break bounds.head,
            Err(error) => tracing::warn!(%error, "outbox read failed"),
        }
        tokio::select! {
            _ = stop.changed() => return,
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    };
    hub.ready.send_replace(true);
    let mut listener: Option<PgListener> = None;
    let mut poll = tokio::time::interval(hub.limits.poll);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if *stop.borrow() {
            return;
        }
        if listener.is_none() {
            listener = listen(&hub.pool).await;
        }
        tokio::select! {
            _ = stop.changed() => return,
            _ = poll.tick() => {}
            woke = wake(listener.as_mut()) => {
                if let Err(error) = woke {
                    tracing::warn!(%error, "outbox listener lost");
                    listener = None;
                }
            }
        }
        cursor = drain(&hub, cursor).await;
    }
}

async fn listen(pool: &PgPool) -> Option<PgListener> {
    let mut listener = match PgListener::connect_with(pool).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::warn!(%error, "outbox listener connect failed");
            return None;
        }
    };
    match listener.listen(db::WAKE_CHANNEL).await {
        Ok(()) => Some(listener),
        Err(error) => {
            tracing::warn!(%error, "outbox listen failed");
            None
        }
    }
}

/// A notification, or a reconnect that may have missed one. Either way the outbox is read.
async fn wake(listener: Option<&mut PgListener>) -> Result<(), sqlx::Error> {
    match listener {
        Some(listener) => listener.try_recv().await.map(|_| ()),
        None => std::future::pending().await,
    }
}

async fn drain(hub: &Arc<Hub>, mut cursor: i64) -> i64 {
    loop {
        let rows = match db::outbox_after(&hub.pool, cursor, BATCH).await {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(%error, "outbox read failed");
                return cursor;
            }
        };
        let full = rows.len() as i64 == BATCH;
        for row in rows {
            cursor = row.seq;
            let event = Arc::new(Event::from_row(row));
            hub.follow(&event);
            let _ = hub.events.send(event);
        }
        if !full {
            return cursor;
        }
    }
}

/// What permissions in a room mean for someone in its call.
pub(crate) fn access(permissions: i64) -> Access {
    use db::perm;
    Access {
        suppressed: !perm::has(permissions, perm::SPEAK),
        priority_speaker: perm::has(permissions, perm::PRIORITY_SPEAKER),
    }
}

impl Hub {
    /// Sends one ephemeral frame to every socket of each user.
    pub(crate) fn relay(&self, user_ids: &[String], frame: &str) {
        let frame: Arc<str> = Arc::from(frame);
        let people = lock(&self.people);
        for user_id in user_ids {
            people.fanout(user_id, Arc::clone(&frame));
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}
