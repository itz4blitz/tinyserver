//! End-to-end encrypted calls through the real live socket: DAVE clients (`davey`, what
//! tinyclient runs) make one MLS group with the server only relaying, and the server keeps
//! none of it. Alone in its own test binary, so it can hold the one global log subscriber and
//! read every line the server writes.

mod common;

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU16;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use api::{Limits, Live};
use axum::http::{Method, StatusCode};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::live::*;
use common::{first_owner, join, run, Call, TestDb};
use davey::{DaveSession, ProposalsOperationType};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sfu::client::{Client, Running};
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

const LOOPBACK: Ipv4Addr = Ipv4Addr::LOCALHOST;
const SLOTS: usize = 11;

/// What the subscriber wrote, for every test in this binary.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl Logs {
    fn lock(&self) -> MutexGuard<'_, Vec<u8>> {
        self.0.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.lock()).into_owned()
    }
}

impl std::io::Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.lock().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn logs() -> &'static Logs {
    static LOGS: OnceLock<Logs> = OnceLock::new();
    LOGS.get_or_init(|| {
        let logs = Logs::default();
        let writer = logs.clone();
        // Everything the server writes, at every level. The WebSocket library prints whole
        // frames at trace, and the test's own clients (davey, the client half of tungstenite)
        // run in this process; neither is the server's own logging.
        let filter = tracing_subscriber::EnvFilter::new(
            "trace,tungstenite=debug,tokio_tungstenite=debug,davey=off,openmls=off",
        );
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .init();
        logs
    })
}

fn hashed(label: &[u8], id: &str) -> u64 {
    let digest = Sha256::new()
        .chain_update(label)
        .chain_update(id.as_bytes())
        .finalize();
    u64::from_be_bytes(digest[..8].try_into().expect("8 bytes"))
}

fn mls_user(user_id: &str) -> u64 {
    hashed(b"tiny-e2ee-v1 user ", user_id)
}

fn mls_room(room_id: &str) -> u64 {
    hashed(b"tiny-e2ee-v1 room ", room_id)
}

fn media_config() -> sfu::Config {
    let mut config = sfu::Config::new(SocketAddr::from((LOOPBACK, 0)), LOOPBACK.into());
    config.connect_timeout = Duration::from_millis(1500);
    config.consent_timeout = Duration::from_secs(5);
    config
}

async fn serve_media(db: &TestDb) -> Server {
    let (media, events) = sfu::start(media_config()).expect("start the SFU");
    let live = Live::with_media(db.runtime.clone(), Limits::default(), media, events);
    serve_live(db, live).await
}

/// Reads frames until `pick` returns something, skipping the rest.
async fn until<T>(ws: &mut Ws, mut pick: impl FnMut(&Value) -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let message = tokio::time::timeout_at(deadline, ws.next())
            .await
            .expect("a matching frame in time")
            .expect("socket open")
            .expect("frame");
        let Message::Text(text) = message else {
            assert!(!message.is_close(), "closed: {message:?}");
            continue;
        };
        let value: Value = serde_json::from_str(text.as_str()).expect("json");
        if let Some(found) = pick(&value) {
            return found;
        }
    }
}

async fn send(ws: &mut Ws, value: Value) {
    ws.send(Message::text(value.to_string()))
        .await
        .expect("send");
}

/// One app in a call: its socket, its media, and its side of the group.
struct App {
    ws: Ws,
    user_id: String,
    room_id: String,
    _running: Running,
    session: Option<DaveSession>,
    /// The last `off`, with whom it named.
    off: Option<Vec<String>>,
    /// How many `start`s came.
    starts: usize,
    /// The epoch of each `commit` frame that came.
    commits: Vec<u64>,
    /// Commits this app sent.
    committed: usize,
    /// Every `call.joined` seen after the join.
    joined: Vec<Value>,
    /// Every MLS payload sent or received, base64, to look for in the logs and the database.
    payloads: Vec<String>,
    errors: Vec<Value>,
}

/// Joins `room_id` with media, offering end-to-end encryption if `e2ee`, and waits for media.
async fn dial(mut ws: Ws, user_id: &str, room_id: &str, e2ee: bool) -> Result<App, Value> {
    let mut client = Client::new(SocketAddr::from((LOOPBACK, 0)), SLOTS).expect("client");
    let mut request =
        json!({"type": "call.join", "ref": "j", "room_id": room_id, "sdp": client.offer()});
    if e2ee {
        request["e2ee"] = json!(1);
    }
    send(&mut ws, request).await;
    let answer = until(&mut ws, |value| {
        (value["type"] == "call.answer" || value["type"] == "error").then(|| value.clone())
    })
    .await;
    if answer["type"] == "error" {
        return Err(answer);
    }
    client
        .accept(answer["sdp"].as_str().expect("sdp"))
        .expect("accept the answer");
    let running = client.start(b'x').expect("client thread");
    let deadline = Instant::now() + WAIT;
    while !running.connected() {
        assert!(Instant::now() < deadline, "media did not connect");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(App {
        ws,
        user_id: user_id.to_string(),
        room_id: room_id.to_string(),
        _running: running,
        session: None,
        off: None,
        starts: 0,
        commits: Vec::new(),
        committed: 0,
        joined: Vec::new(),
        payloads: Vec::new(),
        errors: Vec::new(),
    })
}

impl App {
    fn ready(&self) -> bool {
        self.session.as_ref().is_some_and(DaveSession::is_ready)
    }

    fn code(&self) -> Option<String> {
        self.session
            .as_ref()
            .and_then(DaveSession::voice_privacy_code)
            .map(str::to_string)
    }

    fn epoch(&self) -> Option<u64> {
        self.session
            .as_ref()
            .and_then(DaveSession::epoch)
            .map(|epoch| epoch.as_u64())
    }

    async fn e2ee(&mut self, op: &str, mut fields: Value) {
        fields["type"] = json!("call.e2ee");
        fields["op"] = json!(op);
        for key in ["data", "welcome"] {
            if let Some(payload) = fields[key].as_str() {
                self.payloads.push(payload.to_string());
            }
        }
        send(&mut self.ws, fields).await;
    }

    /// Reads what has come for a moment and acts on it as tinyclient does.
    async fn turn(&mut self, roster: &[u64]) {
        let Ok(Some(Ok(Message::Text(text)))) =
            tokio::time::timeout(Duration::from_millis(20), self.ws.next()).await
        else {
            return;
        };
        let frame: Value = serde_json::from_str(text.as_str()).expect("json");
        match frame["type"].as_str() {
            Some("call.joined") => self.joined.push(frame),
            Some("error") => self.errors.push(frame),
            Some("call.e2ee") => self.mls(&frame, roster).await,
            _ => {}
        }
    }

    async fn mls(&mut self, frame: &Value, roster: &[u64]) {
        assert_eq!(frame["room_id"], self.room_id.as_str());
        if let Some(data) = frame["data"].as_str() {
            self.payloads.push(data.to_string());
        }
        let data = || {
            STANDARD
                .decode(frame["data"].as_str().expect("data"))
                .expect("b64")
        };
        match frame["op"].as_str().expect("op") {
            "start" => {
                self.starts += 1;
                self.off = None;
                let sender = STANDARD
                    .decode(frame["external_sender"].as_str().expect("sender"))
                    .expect("b64");
                let mut session = DaveSession::new(
                    NonZeroU16::new(1).expect("one"),
                    mls_user(&self.user_id),
                    mls_room(&self.room_id),
                    None,
                )
                .expect("session");
                session.set_external_sender(&sender).expect("sender");
                let package = session.create_key_package().expect("key package");
                self.session = Some(session);
                self.e2ee("key_package", json!({"data": STANDARD.encode(package)}))
                    .await;
            }
            "off" => {
                self.session = None;
                let users = frame["user_ids"].as_array().expect("user_ids");
                self.off = Some(
                    users
                        .iter()
                        .map(|user| user.as_str().expect("id").to_string())
                        .collect(),
                );
            }
            "proposals" => {
                let payload = data();
                let session = self.session.as_mut().expect("session");
                let made = session
                    .process_proposals(ProposalsOperationType::APPEND, &payload[1..], Some(roster))
                    .expect("proposals")
                    .expect("a commit");
                let mut fields = json!({
                    "epoch": frame["epoch"],
                    "data": STANDARD.encode(&made.commit),
                });
                if let Some(welcome) = &made.welcome {
                    fields["welcome"] = json!(STANDARD.encode(welcome));
                }
                self.committed += 1;
                self.e2ee("commit", fields).await;
            }
            "commit" => {
                self.commits.push(frame["epoch"].as_u64().expect("epoch"));
                let session = self.session.as_mut().expect("session");
                session.process_commit(&data()).expect("commit applies");
            }
            "welcome" => {
                let session = self.session.as_mut().expect("session");
                session.process_welcome(&data()).expect("welcome applies");
            }
            other => panic!("unknown op {other}"),
        }
    }
}

/// Lets every app act until `done`, or panics after 10 s.
async fn settle(apps: &mut [&mut App], done: impl Fn(&[&mut App]) -> bool) {
    let roster: Vec<u64> = apps.iter().map(|app| mls_user(&app.user_id)).collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done(apps) {
        assert!(Instant::now() < deadline, "the apps did not settle");
        for app in apps.iter_mut() {
            app.turn(&roster).await;
        }
    }
}

/// Every app has keys, the same code, and is at `epoch`.
fn agreed(epoch: u64) -> impl Fn(&[&mut App]) -> bool {
    move |apps| {
        let code = apps[0].code();
        apps.iter()
            .all(|app| app.ready() && app.code() == code && app.epoch() == Some(epoch))
    }
}

/// No MLS payload reached the logs or any table.
async fn nothing_kept(db: &TestDb, payloads: &[String]) {
    let logs = logs().text();
    let mut stored = String::new();
    for table in [
        "outbox",
        "audit_log",
        "rooms",
        "users",
        "memberships",
        "sessions",
        "idempotency_keys",
    ] {
        let rows: Option<String> =
            sqlx::query_scalar(&format!("SELECT string_agg(t::text, ' ') FROM {table} t"))
                .fetch_one(&db.owner)
                .await
                .expect("dump");
        stored.push_str(&rows.unwrap_or_default());
    }
    assert!(!payloads.is_empty());
    assert!(
        logs.contains("e2ee commit accepted"),
        "the server's own lines are captured"
    );
    for payload in payloads {
        // A run of the payload long enough not to turn up by chance.
        let piece = &payload[payload.len() / 2..(payload.len() / 2 + 24).min(payload.len())];
        if let Some(line) = logs.lines().find(|line| line.contains(piece)) {
            panic!("an MLS payload reached the logs: {line}");
        }
        assert!(
            !stored.contains(piece),
            "an MLS payload reached the database"
        );
    }
}

struct Cast {
    server: Server,
    rest: axum::Router,
    owner: (String, String),
    members: Vec<(String, String)>,
    lobby: String,
}

async fn cast(db: &TestDb) -> Cast {
    let owner = first_owner(db).await;
    let (one, one_token) = join(db, &owner, "one@tinyserver.test", "member").await;
    let (two, two_token) = join(db, &owner, "two@tinyserver.test", "member").await;
    let lobby = room(db, &owner, "Lobby").await;
    let server = serve_media(db).await;
    Cast {
        server,
        rest: api::router(db.runtime.clone(), None),
        owner: (owner.user_id, owner_token(db).await),
        members: vec![(one.user_id, one_token), (two.user_id, two_token)],
        lobby: lobby.id,
    }
}

async fn socket(cast: &Cast, who: &(String, String)) -> Ws {
    connect(&cast.server, &who.1, None).await.0
}

#[tokio::test]
async fn three_apps_agree_on_a_code_the_server_cannot_know() {
    logs();
    run(|db| async move {
        let cast = cast(&db).await;
        let ws = socket(&cast, &cast.owner).await;
        let mut a = dial(ws, &cast.owner.0, &cast.lobby, true).await.expect("a");
        settle(&mut [&mut a], |apps| apps[0].session.is_some()).await;
        let ws = socket(&cast, &cast.members[0]).await;
        let mut b = dial(ws, &cast.members[0].0, &cast.lobby, true)
            .await
            .expect("b");
        settle(&mut [&mut a, &mut b], agreed(1)).await;
        let first = a.code().expect("code");
        assert!(a
            .joined
            .iter()
            .any(|joined| joined["user_id"] == b.user_id.as_str() && joined["e2ee"] == true));

        let ws = socket(&cast, &cast.members[1]).await;
        let mut c = dial(ws, &cast.members[1].0, &cast.lobby, true)
            .await
            .expect("c");
        settle(&mut [&mut a, &mut b, &mut c], agreed(2)).await;
        let second = a.code().expect("code");
        assert_ne!(first, second, "a join is a new epoch and a new code");
        // a and b were both asked to add c; one commit won and each member saw only that one.
        for app in [&a, &b] {
            let epochs: HashSet<u64> = app.commits.iter().copied().collect();
            assert_eq!(epochs.len(), app.commits.len(), "one commit an epoch");
        }

        send(&mut c.ws, json!({"type": "call.leave", "ref": "l"})).await;
        settle(&mut [&mut a, &mut b], agreed(3)).await;
        assert_ne!(
            a.code().expect("code"),
            second,
            "a leave is a new epoch too"
        );
        assert!(a.errors.is_empty() && b.errors.is_empty() && c.errors.is_empty());

        let payloads: Vec<String> = [&a, &b, &c]
            .iter()
            .flat_map(|app| app.payloads.clone())
            .collect();
        nothing_kept(&db, &payloads).await;
    })
    .await;
}

#[tokio::test]
async fn only_the_seat_holder_speaks_mls_and_only_as_itself() {
    logs();
    run(|db| async move {
        let cast = cast(&db).await;
        let ws = socket(&cast, &cast.owner).await;
        let mut a = dial(ws, &cast.owner.0, &cast.lobby, true).await.expect("a");
        settle(&mut [&mut a], |apps| apps[0].starts == 1).await;

        // Their other device does not hold the seat; nor does anyone outside.
        let other_device = db::open_session(&db.runtime, &cast.owner.0)
            .await
            .expect("a second session")
            .token;
        let mut other = connect(&cast.server, &other_device, None).await.0;
        let kp = json!({"type": "call.e2ee", "ref": "k", "op": "key_package", "data": "AAAA"});
        let reply = answer(&mut other, kp.clone()).await;
        assert_eq!(
            reply,
            json!({"type": "error", "ref": "k", "code": "not_found"})
        );
        let mut outsider = socket(&cast, &cast.members[0]).await;
        let reply = answer(&mut outsider, kp).await;
        assert_eq!(reply["code"], "not_found");

        // A key package for someone else is refused.
        let mut forged = DaveSession::new(
            NonZeroU16::new(1).expect("one"),
            mls_user(&cast.members[0].0),
            mls_room(&cast.lobby),
            None,
        )
        .expect("session");
        let package = STANDARD.encode(forged.create_key_package().expect("kp"));
        let forged = json!({"type": "call.e2ee", "ref": "f", "op": "key_package", "data": package});
        let reply = answer(&mut a.ws, forged).await;
        assert_eq!(
            reply,
            json!({"type": "error", "ref": "f", "code": "bad_e2ee"})
        );

        for bad in [
            json!({"type": "call.e2ee", "ref": "b"}),
            json!({"type": "call.e2ee", "ref": "b", "op": "nonsense"}),
            json!({"type": "call.e2ee", "ref": "b", "op": "commit", "data": "AAAA"}),
        ] {
            let reply = answer(&mut a.ws, bad).await;
            assert_eq!(reply["code"], "bad_frame");
        }

        // Key packages have a small rate of their own: 5 at once, two used already, and one
        // more back every 2 s.
        let mut codes = Vec::new();
        for n in 0..6 {
            let bogus = json!({"type": "call.e2ee", "ref": n, "op": "key_package", "data": "AAAA"});
            let reply = answer(&mut a.ws, bogus).await;
            let code = reply["code"].as_str().expect("code").to_string();
            if code == "slow_down" {
                break;
            }
            codes.push(code);
        }
        assert!(
            (3..=4).contains(&codes.len()) && codes.iter().all(|code| code == "bad_e2ee"),
            "{codes:?} before slow_down"
        );
    })
    .await;
}

#[tokio::test]
async fn an_app_that_cannot_encrypt_turns_the_call_to_transport_only_unless_required() {
    logs();
    run(|db| async move {
        let cast = cast(&db).await;
        let (status, capabilities) = Call::new(Method::GET, "/v1/capabilities")
            .token(&cast.owner.1)
            .send(&cast.rest)
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(capabilities["features"]
            .as_array()
            .expect("features")
            .contains(&json!("e2ee_calls")));

        let ws = socket(&cast, &cast.owner).await;
        let mut a = dial(ws, &cast.owner.0, &cast.lobby, true).await.expect("a");
        settle(&mut [&mut a], |apps| apps[0].starts == 1).await;

        let ws = socket(&cast, &cast.members[0]).await;
        let mut b = dial(ws, &cast.members[0].0, &cast.lobby, false)
            .await
            .expect("b");
        let b_id = cast.members[0].0.clone();
        // `call.joined` is broadcast and `off` is sent only to the seat. The socket
        // forwards whichever is ready, so `off` can arrive first. Keep reading until both
        // are in: stopping at `off` drops a join that is still queued.
        let roster = [mls_user(&a.user_id)];
        let deadline = Instant::now() + Duration::from_secs(10);
        let saw_b = |app: &App| {
            app.joined
                .iter()
                .any(|joined| joined["user_id"] == b_id.as_str() && joined["e2ee"] == false)
        };
        while a.off.is_none() || !saw_b(&a) {
            assert!(
                Instant::now() < deadline,
                "off={:?} joined={:?}",
                a.off,
                a.joined
            );
            a.turn(&roster).await;
        }
        assert_eq!(a.off.as_deref(), Some(&[b_id.clone()][..]));
        assert!(a.session.is_none());
        assert!(saw_b(&a), "joined {:?}", a.joined);

        // b leaves: everyone left can encrypt, so the group is made again.
        let reply = answer(&mut b.ws, json!({"type": "call.leave", "ref": "l"})).await;
        assert_eq!(reply["type"], "ok");
        settle(&mut [&mut a], |apps| apps[0].starts == 2).await;

        // The room requires it now: an app that cannot is refused, one that can is let in.
        let (status, room) = Call::new(Method::PATCH, &format!("/v1/rooms/{}", cast.lobby))
            .token(&cast.owner.1)
            .json(json!({"e2ee_required": true}))
            .send(&cast.rest)
            .await;
        assert_eq!(status, StatusCode::OK, "{room}");
        assert_eq!(room["e2ee_required"], true);
        let updated = until(&mut b.ws, |value| {
            (value["type"] == "room.updated").then(|| value["room"].clone())
        })
        .await;
        assert_eq!(updated["e2ee_required"], true);
        let (_, rooms) = Call::new(Method::GET, "/v1/rooms")
            .token(&cast.members[0].1)
            .send(&cast.rest)
            .await;
        let listed = rooms
            .as_array()
            .expect("rooms")
            .iter()
            .find(|room| room["id"] == cast.lobby.as_str())
            .expect("lobby")
            .clone();
        assert_eq!(listed["e2ee_required"], true);

        let refused = dial(b.ws, &b_id, &cast.lobby, false)
            .await
            .err()
            .expect("refused");
        assert_eq!(
            refused,
            json!({"type": "error", "ref": "j", "code": "e2ee_required"})
        );
        let ws = socket(&cast, &cast.members[0]).await;
        let mut b = dial(ws, &b_id, &cast.lobby, true)
            .await
            .expect("b with e2ee");
        settle(&mut [&mut a, &mut b], |apps| {
            apps.iter().all(|app| app.ready()) && apps[0].code() == apps[1].code()
        })
        .await;
    })
    .await;
}
