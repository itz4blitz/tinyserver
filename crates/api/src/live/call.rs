//! Who is in which room's call. Memory only; a restart ends every call.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::time::{Duration, Instant};

/// One signed-in device: a digest of its session token. A person's phone and desktop differ,
/// and the same device stays the same across sockets.
pub(crate) type Session = [u8; 32];

pub(crate) fn session_of(token: &str) -> Session {
    Sha256::digest(token.as_bytes()).into()
}

/// `call.e2ee` frames kept for a seat whose socket closed, to hand to the next one. Commits are
/// few in a grace; `call.join` and `call.leave` are limited per socket.
const HELD: usize = 32;

/// Why someone left a call. Sent as `call.left` `reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reason {
    Left,
    JoinedElsewhere,
    Disconnected,
    RoomDeleted,
    Removed,
    SessionEnded,
    Kicked,
    /// Media never connected.
    MediaTimeout,
    /// Media connected, then stopped.
    MediaLost,
    Banned,
    TimedOut,
    /// Someone with move_members moved them to another room.
    Moved,
}

impl Reason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Left => "left",
            Self::JoinedElsewhere => "joined_elsewhere",
            Self::Disconnected => "disconnected",
            Self::RoomDeleted => "room_deleted",
            Self::Removed => "removed",
            Self::SessionEnded => "session_ended",
            Self::Kicked => "kicked",
            Self::MediaTimeout => "media_timeout",
            Self::MediaLost => "media_lost",
            Self::Banned => "banned",
            Self::TimedOut => "timed_out",
            Self::Moved => "moved",
        }
    }

    /// The person goes on to another call, so moderation stays with them.
    fn keeps_moderation(self) -> bool {
        matches!(self, Self::JoinedElsewhere | Self::Moved)
    }
}

/// A change everyone is told about.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Change {
    Joined {
        room_id: String,
        participant: Participant,
    },
    Left {
        room_id: String,
        user_id: String,
        reason: Reason,
        /// The media participant to drop, if the seat had one.
        call_id: Option<String>,
    },
    State {
        room_id: String,
        participant: Participant,
    },
}

/// Why a join was refused. The `error` code is in `docs/calls.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refused {
    /// The room's limit, or the server's when it has none.
    Full,
    ServerFull,
    /// The server is shutting down.
    Closing,
    /// Media was asked for and is not running.
    NoMedia,
}

impl Refused {
    pub fn code(self) -> &'static str {
        match self {
            Self::Full => "room_full",
            Self::ServerFull => "server_full",
            Self::Closing => "unavailable",
            Self::NoMedia => "media_unavailable",
        }
    }
}

/// One room's call in `ready.calls`.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct Occupancy {
    pub room_id: String,
    pub participants: Vec<Participant>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct Participant {
    pub user_id: String,
    pub muted: bool,
    pub deafened: bool,
    pub camera: bool,
    pub sharing: bool,
    pub server_muted: bool,
    pub server_deafened: bool,
    /// No speak permission in the room: nothing they send is forwarded.
    pub suppressed: bool,
    /// Holding priority speaker now.
    pub priority: bool,
    /// Joined able to encrypt end to end.
    pub e2ee: bool,
}

/// What a person may do in the room they join, from their permissions there.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Access {
    /// They lack speak.
    pub suppressed: bool,
    /// They have priority_speaker.
    pub priority_speaker: bool,
}

/// What the SFU should do with a participant apart from their own mute and deafen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Moderation {
    pub call_id: String,
    /// Their audio is not forwarded.
    pub silenced: bool,
    /// They are sent no audio.
    pub hushed: bool,
}

/// What a device gets back with its seat.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Regained {
    pub call_id: Option<String>,
    /// `call.e2ee` frames that came while no socket held the seat, in order. The seat keeps them
    /// until `delivered`, so a socket that fails on the way hands them to the next one.
    pub frames: Vec<Arc<str>>,
}

/// Where a person sits. `conn` is the socket that joined; `None` once it closed.
struct Seat {
    room_id: String,
    conn: Option<u64>,
    epoch: u64,
    /// The device that joined. `None` until `set_session`.
    session: Option<Session>,
    /// `call.e2ee` frames for the next socket. A member who misses a commit cannot follow the
    /// group, so these are not dropped as other frames for a closed socket are.
    held: Vec<Arc<str>>,
    /// The device's new socket is waiting to hear whether `conn` is alive. What goes to `conn`
    /// meanwhile is kept in `held` too, in case it is not.
    contested: bool,
    /// Their SFU participant. `None` for a join without media.
    call_id: Option<String>,
    muted: bool,
    deafened: bool,
    /// Their camera m-line is sending, as the SFU's last answer said.
    camera: bool,
    /// They hold the room's screen share, as the SFU's last answer said.
    sharing: bool,
    access: Access,
    priority: bool,
    /// Joined with `e2ee`: their app can encrypt end to end.
    e2ee: bool,
}

/// A move someone with move_members made, waiting for the moved person's join.
struct Ticket {
    room_id: String,
    until: Instant,
}

/// How long a moved person has to join the room they were moved to past its limit.
const TICKET: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(crate) struct Calls {
    /// Room to participants, in join order.
    rooms: BTreeMap<String, Vec<String>>,
    seats: HashMap<String, Seat>,
    /// Server mute and server deafen, kept while a person moves between calls and dropped once
    /// they leave calls altogether.
    moderated: HashMap<String, (bool, bool)>,
    tickets: HashMap<String, Ticket>,
    epoch: u64,
}

impl Calls {
    /// Whether `join` would seat `user_id` in `room_id` now, with `limit` people in a room.
    pub fn check(
        &self,
        user_id: &str,
        room_id: &str,
        limit: usize,
        server_max: usize,
    ) -> Result<(), Refused> {
        if self.room_of(user_id) == Some(room_id) {
            return Ok(());
        }
        let here = self.rooms.get(room_id).map_or(0, Vec::len);
        if here >= limit {
            return Err(Refused::Full);
        }
        if !self.seats.contains_key(user_id) && self.seats.len() >= server_max {
            return Err(Refused::ServerFull);
        }
        Ok(())
    }

    /// Whether `user_id` was moved to `room_id` and may still join it past its limit.
    pub fn ticket(&self, user_id: &str, room_id: &str) -> bool {
        self.tickets
            .get(user_id)
            .is_some_and(|ticket| ticket.room_id == room_id && ticket.until > Instant::now())
    }

    /// Records that `user_id` was moved to `room_id`.
    pub fn issue_ticket(&mut self, user_id: &str, room_id: &str) {
        self.tickets
            .retain(|_, ticket| ticket.until > Instant::now());
        self.tickets.insert(
            user_id.to_string(),
            Ticket {
                room_id: room_id.to_string(),
                until: Instant::now() + TICKET,
            },
        );
    }

    /// Seats `user_id` in `room_id` for socket `conn` with SFU participant `call_id`, leaving
    /// any other call first. Joining the room they are already in only moves the seat to
    /// `conn` and `call_id`. A join on a move ticket leaves the other call as `moved`.
    #[allow(clippy::too_many_arguments)]
    pub fn join(
        &mut self,
        user_id: &str,
        room_id: &str,
        conn: u64,
        call_id: Option<String>,
        limit: usize,
        server_max: usize,
        access: Access,
        e2ee: bool,
    ) -> Result<Vec<Change>, Refused> {
        self.check(user_id, room_id, limit, server_max)?;
        let moved = self.ticket(user_id, room_id);
        self.tickets.remove(user_id);
        if let Some(seat) = self.seats.get_mut(user_id) {
            if seat.room_id == room_id {
                seat.conn = Some(conn);
                seat.call_id = call_id;
                seat.access = access;
                seat.held.clear();
                seat.contested = false;
                if seat.e2ee != e2ee {
                    seat.e2ee = e2ee;
                    return Ok(vec![self.state(user_id)]);
                }
                return Ok(Vec::new());
            }
        }
        let mut changes = Vec::new();
        let reason = if moved {
            Reason::Moved
        } else {
            Reason::JoinedElsewhere
        };
        changes.extend(self.leave(user_id, reason));
        self.epoch += 1;
        self.seats.insert(
            user_id.to_string(),
            Seat {
                room_id: room_id.to_string(),
                conn: Some(conn),
                epoch: self.epoch,
                session: None,
                held: Vec::new(),
                contested: false,
                call_id,
                muted: false,
                deafened: false,
                camera: false,
                sharing: false,
                access,
                priority: false,
                e2ee,
            },
        );
        self.rooms
            .entry(room_id.to_string())
            .or_default()
            .push(user_id.to_string());
        changes.push(Change::Joined {
            room_id: room_id.to_string(),
            participant: self.participant(user_id),
        });
        Ok(changes)
    }

    pub fn leave(&mut self, user_id: &str, reason: Reason) -> Option<Change> {
        let seat = self.seats.remove(user_id)?;
        if !reason.keeps_moderation() {
            self.moderated.remove(user_id);
        }
        if let Some(people) = self.rooms.get_mut(&seat.room_id) {
            people.retain(|person| person != user_id);
            if people.is_empty() {
                self.rooms.remove(&seat.room_id);
            }
        }
        Some(Change::Left {
            room_id: seat.room_id,
            user_id: user_id.to_string(),
            reason,
            call_id: seat.call_id,
        })
    }

    /// The room `user_id` is in.
    pub fn room_of(&self, user_id: &str) -> Option<&str> {
        self.seats.get(user_id).map(|seat| seat.room_id.as_str())
    }

    /// `user_id`'s SFU participant.
    pub fn call_of(&self, user_id: &str) -> Option<&str> {
        self.seats.get(user_id)?.call_id.as_deref()
    }

    /// The socket holding `user_id`'s seat.
    pub fn conn_of(&self, user_id: &str) -> Option<u64> {
        self.seats.get(user_id)?.conn
    }

    /// Who holds SFU participant `call_id`.
    pub fn holder(&self, call_id: &str) -> Option<&str> {
        self.seats
            .iter()
            .find(|(_, seat)| seat.call_id.as_deref() == Some(call_id))
            .map(|(user_id, _)| user_id.as_str())
    }

    /// Everyone in `room_id`'s call, in join order.
    pub fn people(&self, room_id: &str) -> &[String] {
        self.rooms.get(room_id).map_or(&[], Vec::as_slice)
    }

    /// Every room with a call, and who is in it.
    pub fn occupied(&self) -> Vec<(String, Vec<String>)> {
        self.rooms
            .iter()
            .map(|(room_id, people)| (room_id.clone(), people.clone()))
            .collect()
    }

    /// Records `user_id`'s mute, deafen, and priority; priority only with priority_speaker.
    /// `None` if they are not in a call or nothing changed.
    pub fn set_state(
        &mut self,
        user_id: &str,
        muted: bool,
        deafened: bool,
        priority: bool,
    ) -> Option<Change> {
        let seat = self.seats.get_mut(user_id)?;
        let priority = priority && seat.access.priority_speaker;
        if seat.muted == muted && seat.deafened == deafened && seat.priority == priority {
            return None;
        }
        seat.muted = muted;
        seat.deafened = deafened;
        seat.priority = priority;
        Some(self.state(user_id))
    }

    /// Server mute or deafen `user_id` if they are in `room_id`'s call. `None` changes nothing
    /// there. `None` back if they are not there; else the change, if any, and what the SFU
    /// should now do with them.
    pub fn moderate(
        &mut self,
        room_id: &str,
        user_id: &str,
        server_muted: Option<bool>,
        server_deafened: Option<bool>,
    ) -> Option<(Option<Change>, Option<Moderation>)> {
        if self.room_of(user_id) != Some(room_id) {
            return None;
        }
        let before = self.moderated.get(user_id).copied().unwrap_or_default();
        let after = (
            server_muted.unwrap_or(before.0),
            server_deafened.unwrap_or(before.1),
        );
        if after == (false, false) {
            self.moderated.remove(user_id);
        } else {
            self.moderated.insert(user_id.to_string(), after);
        }
        let change = (after != before).then(|| self.state(user_id));
        Some((change, self.moderation(user_id)))
    }

    /// New permissions in the room for someone in its call. The change, if any, and what the
    /// SFU should now do with them.
    pub fn set_access(
        &mut self,
        room_id: &str,
        user_id: &str,
        access: Access,
    ) -> Option<(Change, Option<Moderation>)> {
        let seat = self.seats.get_mut(user_id)?;
        if seat.room_id != room_id || seat.access == access {
            return None;
        }
        seat.access = access;
        if !access.priority_speaker {
            seat.priority = false;
        }
        Some((self.state(user_id), self.moderation(user_id)))
    }

    /// What the SFU should do with `user_id` apart from their own mute and deafen. `None`
    /// without media.
    pub fn moderation(&self, user_id: &str) -> Option<Moderation> {
        let seat = self.seats.get(user_id)?;
        let (server_muted, server_deafened) =
            self.moderated.get(user_id).copied().unwrap_or_default();
        Some(Moderation {
            call_id: seat.call_id.clone()?,
            silenced: server_muted || seat.access.suppressed,
            hushed: server_deafened,
        })
    }

    /// Records whether `user_id`'s camera sends and whether they share, if `call_id` is still
    /// their participant. `None` if it is not or nothing changed.
    pub fn set_media(
        &mut self,
        user_id: &str,
        call_id: &str,
        camera: bool,
        sharing: bool,
    ) -> Option<Change> {
        let seat = self.seats.get_mut(user_id)?;
        if seat.call_id.as_deref() != Some(call_id)
            || (seat.camera == camera && seat.sharing == sharing)
        {
            return None;
        }
        seat.camera = camera;
        seat.sharing = sharing;
        Some(self.state(user_id))
    }

    /// Who shares their screen in `room_id`'s call.
    pub fn sharer(&self, room_id: &str) -> Option<&str> {
        self.people(room_id)
            .iter()
            .find(|user_id| self.seats.get(*user_id).is_some_and(|seat| seat.sharing))
            .map(String::as_str)
    }

    /// Ends `user_id`'s share in `room_id`. Their SFU participant and the change, or `None`
    /// if they were not sharing there.
    pub fn end_share(&mut self, room_id: &str, user_id: &str) -> Option<(Option<String>, Change)> {
        let seat = self.seats.get_mut(user_id)?;
        if seat.room_id != room_id || !seat.sharing {
            return None;
        }
        seat.sharing = false;
        let call_id = seat.call_id.clone();
        Some((call_id, self.state(user_id)))
    }

    /// The socket `conn` closed. If it held the seat, returns an epoch for `expire`.
    pub fn orphan(&mut self, user_id: &str, conn: u64) -> Option<u64> {
        let seat = self.seats.get_mut(user_id)?;
        if seat.conn != Some(conn) {
            return None;
        }
        seat.conn = None;
        self.epoch += 1;
        seat.epoch = self.epoch;
        Some(seat.epoch)
    }

    /// Records which device holds `user_id`'s seat.
    pub fn set_session(&mut self, user_id: &str, session: Session) {
        if let Some(seat) = self.seats.get_mut(user_id) {
            seat.session = Some(session);
        }
    }

    /// Whether `session` is the device in `user_id`'s seat.
    pub fn seated(&self, user_id: &str, session: Session) -> bool {
        self.seats
            .get(user_id)
            .is_some_and(|seat| seat.session == Some(session))
    }

    /// Socket `conn`, holding `user_id`'s seat, was asked to prove it is alive.
    pub fn contest(&mut self, user_id: &str) {
        if let Some(seat) = self.seats.get_mut(user_id) {
            seat.contested = true;
        }
    }

    /// Socket `from` proved it, and keeps the seat with everything it was sent meanwhile.
    pub fn uncontest(&mut self, user_id: &str, from: u64) {
        if let Some(seat) = self.seats.get_mut(user_id) {
            seat.contested = false;
            if seat.conn == Some(from) {
                seat.held.clear();
            }
        }
    }

    /// Moves the seat to `conn`, the device's new socket, if `session` holds it and no socket
    /// does, or only `from`, which did not prove it is alive. A blip on the live socket leaves
    /// healthy media alone this way. Any other device takes the seat only with `call.join`.
    pub fn regain(
        &mut self,
        user_id: &str,
        session: Session,
        conn: u64,
        from: Option<u64>,
    ) -> Option<Regained> {
        let seat = self.seats.get_mut(user_id)?;
        if seat.session != Some(session) || (seat.conn.is_some() && seat.conn != from) {
            return None;
        }
        seat.conn = Some(conn);
        seat.contested = false;
        Some(Regained {
            call_id: seat.call_id.clone(),
            frames: seat.held.clone(),
        })
    }

    /// Socket `conn` sent what `regain` gave it.
    pub fn delivered(&mut self, user_id: &str, conn: u64) {
        if let Some(seat) = self.seats.get_mut(user_id) {
            if seat.conn == Some(conn) {
                seat.held.clear();
            }
        }
    }

    /// Keeps `frame` if no socket holds `user_id`'s seat, or while its socket is in doubt. True
    /// if it should not be sent now: false if a socket should have it, or they have no seat.
    pub fn hold(&mut self, user_id: &str, frame: &Arc<str>) -> bool {
        let Some(seat) = self
            .seats
            .get_mut(user_id)
            .filter(|seat| seat.conn.is_none() || seat.contested)
        else {
            return false;
        };
        if seat.held.len() < HELD {
            seat.held.push(Arc::clone(frame));
        }
        seat.conn.is_none()
    }

    /// Leaves if the seat is still the one `orphan` left, with no socket rejoined since.
    pub fn expire(&mut self, user_id: &str, epoch: u64) -> Option<Change> {
        let seat = self.seats.get(user_id)?;
        if seat.conn.is_some() || seat.epoch != epoch {
            return None;
        }
        self.leave(user_id, Reason::Disconnected)
    }

    pub fn end_room(&mut self, room_id: &str, reason: Reason) -> Vec<Change> {
        let people = self.rooms.get(room_id).cloned().unwrap_or_default();
        people
            .iter()
            .filter_map(|user_id| self.leave(user_id, reason))
            .collect()
    }

    pub fn clear(&mut self) {
        self.rooms.clear();
        self.seats.clear();
        self.moderated.clear();
        self.tickets.clear();
    }

    pub fn snapshot(&self) -> Vec<Occupancy> {
        self.rooms
            .iter()
            .map(|(room_id, people)| Occupancy {
                room_id: room_id.clone(),
                participants: people
                    .iter()
                    .map(|user_id| self.participant(user_id))
                    .collect(),
            })
            .collect()
    }

    fn participant(&self, user_id: &str) -> Participant {
        let seat = self.seats.get(user_id);
        let (server_muted, server_deafened) =
            self.moderated.get(user_id).copied().unwrap_or_default();
        Participant {
            user_id: user_id.to_string(),
            muted: seat.is_some_and(|seat| seat.muted),
            deafened: seat.is_some_and(|seat| seat.deafened),
            camera: seat.is_some_and(|seat| seat.camera),
            sharing: seat.is_some_and(|seat| seat.sharing),
            server_muted,
            server_deafened,
            suppressed: seat.is_some_and(|seat| seat.access.suppressed),
            priority: seat.is_some_and(|seat| seat.priority),
            e2ee: seat.is_some_and(|seat| seat.e2ee),
        }
    }

    fn state(&self, user_id: &str) -> Change {
        Change::State {
            room_id: self.room_of(user_id).unwrap_or_default().to_string(),
            participant: self.participant(user_id),
        }
    }
}

/// Joins and leaves one socket may make: a burst, then one more every `every`.
pub(crate) struct Moves {
    burst: f64,
    every: Duration,
    tokens: f64,
    at: Instant,
}

impl Moves {
    pub fn new(burst: u32, every: Duration) -> Self {
        let burst = f64::from(burst.max(1));
        Self {
            burst,
            every: every.max(Duration::from_millis(1)),
            tokens: burst,
            at: Instant::now(),
        }
    }

    pub fn take(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.at);
        self.at = now;
        self.tokens =
            (self.tokens + elapsed.as_secs_f64() / self.every.as_secs_f64()).min(self.burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Access, Calls, Change, Moderation, Moves, Participant, Reason, Refused};
    use super::{Regained, HELD};
    use std::sync::Arc;
    use std::time::Duration;

    const FREE: Access = Access {
        suppressed: false,
        priority_speaker: false,
    };

    fn person(user_id: &str) -> Participant {
        Participant {
            user_id: user_id.to_string(),
            muted: false,
            deafened: false,
            camera: false,
            sharing: false,
            server_muted: false,
            server_deafened: false,
            suppressed: false,
            priority: false,
            e2ee: false,
        }
    }

    fn joined(room_id: &str, user_id: &str) -> Change {
        Change::Joined {
            room_id: room_id.to_string(),
            participant: person(user_id),
        }
    }

    fn left(room_id: &str, user_id: &str, reason: Reason) -> Change {
        Change::Left {
            room_id: room_id.to_string(),
            user_id: user_id.to_string(),
            reason,
            call_id: None,
        }
    }

    fn join(calls: &mut Calls, user: &str, room: &str, conn: u64) -> Result<Vec<Change>, Refused> {
        calls.join(user, room, conn, None, 12, 32, FREE, false)
    }

    #[test]
    fn joining_another_room_leaves_the_first() {
        let mut calls = Calls::default();
        assert_eq!(join(&mut calls, "u", "a", 1), Ok(vec![joined("a", "u")]));
        assert_eq!(
            join(&mut calls, "u", "b", 1),
            Ok(vec![
                left("a", "u", Reason::JoinedElsewhere),
                joined("b", "u")
            ])
        );
        assert_eq!(calls.room_of("u"), Some("b"));
        assert_eq!(calls.snapshot().len(), 1);
    }

    #[test]
    fn the_same_session_regains_a_seat_and_stops_its_expiry() {
        let mut calls = Calls::default();
        assert!(join(&mut calls, "u", "a", 1).is_ok());
        calls.set_session("u", [1; 32]);
        let epoch = calls.orphan("u", 1).expect("seat held by 1");
        assert_eq!(calls.regain("u", [2; 32], 2, None), None, "another device");
        assert_eq!(calls.conn_of("u"), None);
        assert_eq!(
            calls.regain("u", [1; 32], 3, None),
            Some(Regained {
                call_id: None,
                frames: Vec::new()
            })
        );
        assert_eq!(calls.conn_of("u"), Some(3));
        assert_eq!(calls.expire("u", epoch), None);
        assert_eq!(calls.room_of("u"), Some("a"));
    }

    #[test]
    fn a_socket_still_up_loses_the_seat_only_to_a_claim_naming_it() {
        let mut calls = Calls::default();
        assert!(join(&mut calls, "u", "a", 1).is_ok());
        calls.set_session("u", [1; 32]);
        assert_eq!(calls.regain("u", [1; 32], 2, None), None, "socket 1 is up");
        assert_eq!(
            calls.regain("u", [1; 32], 2, Some(9)),
            None,
            "not socket 9's"
        );
        assert_eq!(calls.conn_of("u"), Some(1));
        assert!(calls.regain("u", [1; 32], 2, Some(1)).is_some());
        assert_eq!(calls.conn_of("u"), Some(2));
        assert_eq!(calls.regain("u", [1; 32], 3, Some(1)), None, "a late claim");
        assert_eq!(calls.conn_of("u"), Some(2));
    }

    #[test]
    fn held_frames_outlive_a_socket_that_fails_before_sending_them() {
        let mut calls = Calls::default();
        let frame: Arc<str> = Arc::from("frame");
        assert!(join(&mut calls, "u", "a", 1).is_ok());
        calls.set_session("u", [1; 32]);
        calls.orphan("u", 1);
        assert!(calls.hold("u", &frame));

        // Socket 2 got them and died before it could send.
        let first = calls
            .regain("u", [1; 32], 2, None)
            .expect("the same device");
        assert_eq!(first.frames.len(), 1);
        calls.orphan("u", 2);
        assert!(calls.hold("u", &frame), "and more came");

        let second = calls
            .regain("u", [1; 32], 3, None)
            .expect("the same device");
        assert_eq!(second.frames.len(), 2);
        calls.delivered("u", 2);
        assert_eq!(
            calls
                .regain("u", [1; 32], 4, Some(3))
                .map(|got| got.frames.len()),
            Some(2),
            "a stale socket clears nothing"
        );
        calls.delivered("u", 4);
        calls.orphan("u", 4);
        let last = calls
            .regain("u", [1; 32], 5, None)
            .expect("the same device");
        assert!(last.frames.is_empty());
    }

    #[test]
    fn frames_sent_while_a_socket_is_in_doubt_are_kept_for_whoever_wins() {
        let mut calls = Calls::default();
        let frame: Arc<str> = Arc::from("frame");
        assert!(join(&mut calls, "u", "a", 1).is_ok());
        calls.set_session("u", [1; 32]);

        calls.contest("u");
        assert!(!calls.hold("u", &frame), "socket 1 gets it as well");
        calls.uncontest("u", 1);
        assert!(!calls.hold("u", &frame));
        calls.contest("u");
        calls.uncontest("u", 1);

        calls.contest("u");
        assert!(!calls.hold("u", &frame));
        let regained = calls.regain("u", [1; 32], 2, Some(1)).expect("taken");
        assert_eq!(regained.frames.len(), 1);
        assert!(!calls.hold("u", &frame), "no longer in doubt");
    }

    #[test]
    fn a_seat_with_no_session_is_not_regained() {
        let mut calls = Calls::default();
        assert!(join(&mut calls, "u", "a", 1).is_ok());
        assert_eq!(calls.regain("u", [1; 32], 2, None), None);
        assert_eq!(calls.conn_of("u"), Some(1));
    }

    #[test]
    fn frames_for_a_closed_socket_wait_for_the_next_one_up_to_a_cap() {
        let mut calls = Calls::default();
        let frame: Arc<str> = Arc::from("frame");
        assert!(!calls.hold("u", &frame), "no seat");
        assert!(join(&mut calls, "u", "a", 1).is_ok());
        calls.set_session("u", [1; 32]);
        assert!(!calls.hold("u", &frame), "the socket is up");
        calls.orphan("u", 1);
        for _ in 0..HELD + 1 {
            assert!(calls.hold("u", &frame));
        }
        let regained = calls
            .regain("u", [1; 32], 2, None)
            .expect("the same device");
        assert_eq!(regained.frames.len(), HELD);
        assert!(!calls.hold("u", &frame), "the socket is back");
    }

    #[test]
    fn a_join_drops_what_was_held_for_the_old_media() {
        let mut calls = Calls::default();
        assert!(join(&mut calls, "u", "a", 1).is_ok());
        calls.set_session("u", [1; 32]);
        calls.orphan("u", 1);
        assert!(calls.hold("u", &Arc::from("frame")));
        assert!(join(&mut calls, "u", "a", 2).is_ok());
        calls.orphan("u", 2);
        let regained = calls
            .regain("u", [1; 32], 3, None)
            .expect("the same device");
        assert!(regained.frames.is_empty());
    }

    #[test]
    fn a_second_socket_in_the_same_room_takes_the_seat_without_a_change() {
        let mut calls = Calls::default();
        assert!(join(&mut calls, "u", "a", 1).is_ok());
        assert_eq!(join(&mut calls, "u", "a", 2), Ok(Vec::new()));
        assert_eq!(calls.orphan("u", 1), None);
        let epoch = calls.orphan("u", 2).expect("seat held by 2");
        assert!(join(&mut calls, "u", "a", 3).is_ok());
        assert_eq!(calls.expire("u", epoch), None);
        assert_eq!(calls.room_of("u"), Some("a"));
    }

    #[test]
    fn an_orphaned_seat_expires() {
        let mut calls = Calls::default();
        assert!(join(&mut calls, "u", "a", 1).is_ok());
        let epoch = calls.orphan("u", 1).expect("epoch");
        assert_eq!(
            calls.expire("u", epoch),
            Some(left("a", "u", Reason::Disconnected))
        );
        assert!(calls.snapshot().is_empty());
    }

    #[test]
    fn caps_count_people_not_moves() {
        let mut calls = Calls::default();
        let go = |calls: &mut Calls, user: &str, room: &str, conn: u64| {
            calls.join(user, room, conn, None, 2, 3, FREE, false)
        };
        assert!(go(&mut calls, "u", "a", 1).is_ok());
        assert!(go(&mut calls, "v", "a", 2).is_ok());
        assert_eq!(go(&mut calls, "w", "a", 3), Err(Refused::Full));
        assert!(go(&mut calls, "w", "b", 3).is_ok());
        assert_eq!(go(&mut calls, "x", "b", 4), Err(Refused::ServerFull));
        assert!(go(&mut calls, "u", "b", 1).is_ok());
        assert_eq!(Refused::Full.code(), "room_full");
    }

    #[test]
    fn the_default_room_cap_refuses_the_thirteenth() {
        let limits = crate::live::Limits::default();
        let mut calls = Calls::default();
        for n in 0..12 {
            let user_id = format!("u{n}");
            let joined = calls.join(
                &user_id,
                "a",
                n,
                None,
                limits.call_room_max,
                limits.call_server_max,
                FREE,
                false,
            );
            assert!(joined.is_ok(), "{user_id}");
        }
        assert_eq!(
            calls.join(
                "u12",
                "a",
                12,
                None,
                limits.call_room_max,
                limits.call_server_max,
                FREE,
                false
            ),
            Err(Refused::Full)
        );
    }

    #[test]
    fn a_move_ticket_names_the_leave_moved_and_keeps_moderation() {
        let mut calls = Calls::default();
        assert!(join(&mut calls, "u", "a", 1).is_ok());
        assert!(calls.moderate("a", "u", Some(true), None).is_some());
        calls.issue_ticket("u", "b");
        assert!(calls.ticket("u", "b"));
        assert!(!calls.ticket("u", "a"));
        let changes = join(&mut calls, "u", "b", 1).expect("join");
        assert_eq!(changes[0], left("a", "u", Reason::Moved));
        let Change::Joined { participant, .. } = &changes[1] else {
            panic!("joined");
        };
        assert!(participant.server_muted);
        assert!(!calls.ticket("u", "b"), "a ticket is used once");
        // Leaving calls altogether drops the server mute.
        assert!(calls.leave("u", Reason::Left).is_some());
        let changes = join(&mut calls, "u", "a", 1).expect("join");
        let Change::Joined { participant, .. } = &changes[0] else {
            panic!("joined");
        };
        assert!(!participant.server_muted);
    }

    #[test]
    fn moderation_and_suppression_reach_the_sfu() {
        let mut calls = Calls::default();
        let suppressed = Access {
            suppressed: true,
            priority_speaker: false,
        };
        let changes = calls
            .join(
                "u",
                "a",
                1,
                Some("c-1".to_string()),
                12,
                32,
                suppressed,
                false,
            )
            .expect("join");
        let Change::Joined { participant, .. } = &changes[0] else {
            panic!("joined");
        };
        assert!(participant.suppressed);
        assert_eq!(
            calls.moderation("u"),
            Some(Moderation {
                call_id: "c-1".to_string(),
                silenced: true,
                hushed: false
            })
        );
        assert_eq!(
            calls.moderate("b", "u", Some(true), None),
            None,
            "other room"
        );
        let (change, sfu) = calls
            .moderate("a", "u", None, Some(true))
            .expect("in the call");
        assert!(change.is_some());
        assert_eq!(sfu.map(|sfu| sfu.hushed), Some(true));
        let (change, _) = calls
            .moderate("a", "u", None, Some(true))
            .expect("in the call");
        assert_eq!(change, None, "nothing changed");
        let (change, sfu) = calls.set_access("a", "u", FREE).expect("changed");
        let Change::State { participant, .. } = change else {
            panic!("state");
        };
        assert!(!participant.suppressed);
        assert_eq!(sfu.map(|sfu| sfu.silenced), Some(false));
    }

    #[test]
    fn priority_needs_priority_speaker() {
        let mut calls = Calls::default();
        assert!(join(&mut calls, "u", "a", 1).is_ok());
        assert_eq!(calls.set_state("u", false, false, true), None);
        let speaker = Access {
            suppressed: false,
            priority_speaker: true,
        };
        assert!(calls.set_access("a", "u", speaker).is_some());
        let Some(Change::State { participant, .. }) = calls.set_state("u", false, false, true)
        else {
            panic!("state");
        };
        assert!(participant.priority);
        let (Change::State { participant, .. }, _) =
            calls.set_access("a", "u", FREE).expect("changed")
        else {
            panic!("state");
        };
        assert!(
            !participant.priority,
            "losing the permission drops priority"
        );
    }

    #[test]
    fn ending_a_room_empties_it() {
        let mut calls = Calls::default();
        assert!(join(&mut calls, "u", "a", 1).is_ok());
        assert!(join(&mut calls, "v", "a", 2).is_ok());
        assert_eq!(
            calls.end_room("a", Reason::RoomDeleted),
            vec![
                left("a", "u", Reason::RoomDeleted),
                left("a", "v", Reason::RoomDeleted)
            ]
        );
        assert!(calls.snapshot().is_empty());
    }

    #[test]
    fn a_seat_keeps_its_call_id_and_state_until_it_leaves() {
        let mut calls = Calls::default();
        let joined = calls.join("u", "a", 1, Some("c-1".to_string()), 12, 32, FREE, false);
        assert!(joined.is_ok());
        assert_eq!(calls.call_of("u"), Some("c-1"));
        assert_eq!(calls.holder("c-1"), Some("u"));
        assert_eq!(
            calls.join("u", "a", 2, Some("c-2".to_string()), 12, 32, FREE, false),
            Ok(Vec::new())
        );
        assert_eq!(calls.holder("c-1"), None);
        assert_eq!(calls.conn_of("u"), Some(2));
        assert!(calls.set_state("u", true, false, false).is_some());
        assert_eq!(calls.set_state("u", true, false, false), None);
        assert!(calls.snapshot()[0].participants[0].muted);
        assert_eq!(
            calls.set_media("u", "c-1", true, false),
            None,
            "a stale call id"
        );
        assert_eq!(
            calls.set_media("u", "c-2", true, false),
            Some(Change::State {
                room_id: "a".to_string(),
                participant: Participant {
                    muted: true,
                    camera: true,
                    ..person("u")
                },
            })
        );
        assert_eq!(calls.set_media("u", "c-2", true, false), None);
        assert!(calls.snapshot()[0].participants[0].camera);
        assert_eq!(calls.sharer("a"), None);
        assert!(calls.set_media("u", "c-2", true, true).is_some());
        assert_eq!(calls.sharer("a"), Some("u"));
        assert!(calls.snapshot()[0].participants[0].sharing);
        assert_eq!(calls.end_share("b", "u"), None, "another room");
        let ended = calls.end_share("a", "u").expect("ended");
        assert_eq!(ended.0.as_deref(), Some("c-2"));
        assert_eq!(calls.end_share("a", "u"), None);
        assert_eq!(calls.sharer("a"), None);
        assert_eq!(
            calls.leave("u", Reason::MediaLost),
            Some(Change::Left {
                room_id: "a".to_string(),
                user_id: "u".to_string(),
                reason: Reason::MediaLost,
                call_id: Some("c-2".to_string()),
            })
        );
    }

    #[test]
    fn moves_allow_a_burst_then_refuse() {
        let mut moves = Moves::new(2, Duration::from_secs(60));
        assert!(moves.take());
        assert!(moves.take());
        assert!(!moves.take());
    }
}
