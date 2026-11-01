//! `GET /v1/live`: one WebSocket per signed-in device, or per bot token. Durable events come
//! from the outbox in seq order, presence, call occupancy, and call signaling from memory. The
//! frames are in `docs/calls.md`. A bot is not a member. It receives room events when it has
//! `rooms.read` and member events when it has `members.read`.

mod call;
mod e2ee;
pub(crate) mod frame;
mod hub;
mod socket;

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{rejection::WebSocketUpgradeRejection, WebSocketUpgrade};
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use db::{PgPool, SessionError};
use serde::Serialize;

use crate::session::{presented, Presented};
use hub::Hub;

/// Caps and timers for live sockets. `Default` is what the server runs.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// A further socket for the same person closes their oldest with `4008 replaced`.
    pub sockets_per_user: usize,
    /// A socket over this is refused with 503 before the upgrade.
    pub sockets_total: usize,
    /// The largest frame or message a client may send. Bigger closes the socket.
    pub frame_bytes: usize,
    /// No traffic from the client for this long closes the socket.
    pub idle: Duration,
    /// How often a socket re-reads its session.
    pub recheck: Duration,
    /// How long a person stays online after their last socket closes.
    pub presence_grace: Duration,
    /// How long a socket holding a call seat has to answer a ping when the same session opens
    /// another. No answer moves the seat to the new socket.
    pub seat_proof: Duration,
    /// A send that takes longer drops the socket.
    pub send_timeout: Duration,
    /// More missed events than this on resume gets `resync` instead.
    pub replay_max: i64,
    /// The dispatcher reads the outbox this often even without a wake.
    pub poll: Duration,
    /// Client messages a second, and the burst above that.
    pub messages_per_second: u32,
    pub message_burst: u32,
    /// People in one room's call. A join past it gets `full`.
    pub call_room_max: usize,
    /// People in calls on the server. A join past it gets `server_full`.
    pub call_server_max: usize,
    /// Joins and leaves one socket may make at once, and how often it gets one more back.
    pub call_move_burst: u32,
    pub call_move_every: Duration,
    /// How often who was connected is written to `last_seen_at`, in one batch.
    pub seen_every: Duration,
    /// End-to-end encryption key packages and `invalid`s one socket may send at once, and how
    /// often it gets one more back.
    pub e2ee_rekey_burst: u32,
    pub e2ee_rekey_every: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            sockets_per_user: 3,
            sockets_total: 1024,
            frame_bytes: 64 * 1024,
            idle: Duration::from_secs(60),
            recheck: Duration::from_secs(60),
            presence_grace: Duration::from_secs(10),
            seat_proof: Duration::from_secs(3),
            send_timeout: Duration::from_secs(10),
            replay_max: 1000,
            poll: Duration::from_secs(5),
            messages_per_second: 20,
            message_burst: 40,
            call_room_max: 12,
            call_server_max: 32,
            call_move_burst: 5,
            call_move_every: Duration::from_secs(3),
            seen_every: Duration::from_secs(30),
            e2ee_rekey_burst: 5,
            e2ee_rekey_every: Duration::from_secs(2),
        }
    }
}

/// The live socket hub. Cheap to clone.
#[derive(Clone)]
pub struct Live {
    hub: Arc<Hub>,
}

impl Live {
    /// Starts the outbox dispatcher, without media. A join with an offer gets
    /// `media_unavailable`. Call it inside a Tokio runtime.
    pub fn start(pool: PgPool, limits: Limits) -> Self {
        Self {
            hub: Hub::start(pool, limits, None),
        }
    }

    /// Starts the outbox dispatcher and follows the SFU from `sfu::start`.
    pub fn with_media(
        pool: PgPool,
        limits: Limits,
        media: sfu::Media,
        events: tokio::sync::mpsc::Receiver<sfu::Event>,
    ) -> Self {
        Self {
            hub: Hub::start(pool, limits, Some((media, events))),
        }
    }

    /// `off` without an SFU, `up` while it runs, `down` once it stopped.
    pub fn media(&self) -> &'static str {
        self.hub.media_status()
    }

    /// The SFU stopped by itself. Calls cannot start again until the process does.
    pub fn media_failed(&self) -> bool {
        self.hub.media_failed()
    }

    /// The media transports up now. Empty without an SFU or once it stopped.
    pub fn transports(&self) -> Vec<&'static str> {
        self.hub
            .media
            .as_ref()
            .map(sfu::Media::transports)
            .unwrap_or_default()
    }

    /// Closes every socket with `1012 restart`, refuses new ones, and stops the dispatcher
    /// and the SFU.
    pub fn close(&self) {
        self.hub.close();
    }

    /// Waits until every socket and the dispatcher have stopped. False if `timeout` passed first.
    pub async fn closed(&self, timeout: Duration) -> bool {
        self.hub.closed(timeout).await
    }

    /// Sockets open now.
    pub fn sockets(&self) -> usize {
        self.hub.sockets()
    }
}

pub(crate) fn routes<S: Clone + Send + Sync + 'static>(live: Live) -> Router<S> {
    Router::new()
        .route("/v1/live", get(upgrade))
        .with_state(live)
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
}

fn refuse(status: StatusCode, error: &'static str) -> Response {
    (status, Json(ErrorBody { error })).into_response()
}

/// The session is checked before the upgrade, so a bad token is a plain 401.
/// `Authorization: Bot <token>` opens the same socket when the bot token is live.
async fn upgrade(
    State(live): State<Live>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let hub = live.hub;
    let me = match presented(&headers) {
        Some(Presented::Bearer(token)) => {
            let identity = match db::current_session(&hub.pool, token).await {
                Ok(identity) => identity,
                Err(error) => return session_unavailable(error),
            };
            socket::Me {
                token: token.to_string(),
                session: call::session_of(token),
                user_id: identity.user_id,
                role: identity.role,
                permissions: identity.permissions,
                scopes: Vec::new(),
            }
        }
        Some(Presented::Bot(token)) => {
            let bot = match db::authenticate_bot(&hub.pool, token).await {
                Ok(bot) => bot,
                Err(error) => return session_unavailable(error),
            };
            socket::Me {
                token: token.to_string(),
                session: call::session_of(token),
                user_id: bot.id,
                role: "bot".to_string(),
                permissions: 0,
                scopes: bot.scopes,
            }
        }
        None => return live_refused("unauthorized"),
    };
    let Ok(since) = parse_since(query.as_deref()) else {
        return live_refused("invalid");
    };
    let ws = match ws {
        Ok(ws) => ws,
        Err(rejection) => {
            tracing::warn!(reason = "invalid", "live refused");
            return rejection.into_response();
        }
    };
    if hub.closing() {
        return live_refused("restarting");
    }
    if tokio::time::timeout(Duration::from_secs(5), hub.ready())
        .await
        .is_err()
    {
        return live_refused("unavailable");
    }
    let Some(slot) = hub.reserve() else {
        return live_refused("busy");
    };
    let frame = hub.limits.frame_bytes;
    ws.max_message_size(frame)
        .max_frame_size(frame)
        .on_upgrade(move |socket| socket::run(hub, socket, slot, me, since))
}

fn live_refused(reason: &'static str) -> Response {
    tracing::warn!(reason, "live refused");
    let status = match reason {
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "invalid" => StatusCode::BAD_REQUEST,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    refuse(status, reason)
}

fn session_unavailable(error: SessionError) -> Response {
    match error {
        SessionError::Unauthorized => live_refused("unauthorized"),
        SessionError::Database(error) => {
            tracing::warn!(%error, "live session check failed");
            refuse(StatusCode::INTERNAL_SERVER_ERROR, "unavailable")
        }
    }
}

/// `since` is a seq the client already has. Other parameters are ignored.
fn parse_since(query: Option<&str>) -> Result<Option<i64>, ()> {
    let Some(query) = query else {
        return Ok(None);
    };
    let mut since = None;
    for pair in query.split('&') {
        if let Some(value) = pair.strip_prefix("since=") {
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(());
            }
            since = Some(value.parse::<i64>().map_err(|_| ())?);
        }
    }
    Ok(since)
}

#[cfg(test)]
mod tests {
    use super::parse_since;

    #[test]
    fn since_is_a_non_negative_seq() {
        assert_eq!(parse_since(None), Ok(None));
        assert_eq!(parse_since(Some("")), Ok(None));
        assert_eq!(parse_since(Some("since=0")), Ok(Some(0)));
        assert_eq!(parse_since(Some("x=1&since=42")), Ok(Some(42)));
        assert!(parse_since(Some("since=")).is_err());
        assert!(parse_since(Some("since=-1")).is_err());
        assert!(parse_since(Some("since=1e3")).is_err());
        assert!(parse_since(Some("since=99999999999999999999")).is_err());
    }
}
