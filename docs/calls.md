# Calls

Design for voice and video in tinyserver. Slices 1 to 6, the live socket, call occupancy, audio through the SFU over UDP, ICE-TCP with hardening, camera video, and screen share, are built, and so is end-to-end encryption for audio; relay mode is not. Read it with [`README.md`](../README.md), [`deploy/README.md`](../deploy/README.md), and [`crates/sfu/src/lib.rs`](../crates/sfu/src/lib.rs).

## Recommendation

A room is a voice channel. People drop in and out; nobody rings anyone. Signaling, presence, and room changes travel over one authenticated WebSocket at `GET /v1/live`, through the cloudflared tunnel that already carries HTTPS. Media goes straight to a str0m SFU inside the tinyserver process: one thread, one UDP port, and ICE-TCP on the same port number. The compose file publishes that one port and nothing else. A host that cannot open a port (CGNAT, or an owner who wants the address hidden) turns on a relay mode later: the SFU allocates on Cloudflare Realtime TURN and never shows its own address. tinyclient adds str0m to `mediad` and keeps its existing Opus, jitter buffer, and audio device code.

| Decision | Choice |
| --- | --- |
| Realtime channel | WebSocket at `/v1/live`, bearer session or bot token, JSON frames |
| Durable fan-out | `outbox` row in the same transaction as the change, `LISTEN app_wake`, one in-process broadcast |
| Ephemeral state | Presence, occupancy, speaking, signaling. Memory only |
| Media | WebRTC through str0m `=0.24.1` in `crates/sfu`, ICE-lite, DTLS-SRTP. DTLS ends at the SFU. |
| End to end | MLS between the apps (DAVE's design), the server only relaying it, when every app in the call can |
| Audio | The SFU forwards Opus. It does not encode. The client target is mono, 32 kb/s, in-band FEC, DTX off (the speech gate ends a spurt). `enable_opus(true, false)` is Opus with `useinbandfec=1` and RED off. A missing audio packet is not held for later packets: `release_audio_promptly` sets reorder size 0 and a 40 ms timeout. |
| Video | H.264 constrained baseline. Camera simulcast in two layers. One share per room, in one layer |
| What each listener receives | Every other person's audio, and at most one H.264 picture. While someone else holds the room's share, that picture is the share. Otherwise it is one layer, `h` or `l`, of one other live camera: the newest floor holder who still has a camera, else the first other camera in join order. Never the listener's own picture. |
| Network | Publish one UDP port and the same TCP port. HTTP stays behind the tunnel |
| No-port mode | Cloudflare Realtime TURN, minted by tinyserver, in a later slice |
| Persistence | None for media or call state. A restart ends every call; clients rejoin |

## Constraints that shaped this

| Fact | Source | Consequence |
| --- | --- | --- |
| A cloudflared public hostname proxies HTTP(S), and TCP only as a stream over WebSocket that needs `cloudflared` on the client. UDP is not a public hostname service type. | [Cloudflare Tunnel protocols](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/routing-to-tunnel/protocols/) | Signaling fits the tunnel. Media does not. |
| WebSockets work on every Cloudflare plan. Idle ones are closed, and Cloudflare restarts can drop them. | [Cloudflare WebSockets](https://developers.cloudflare.com/network/websockets/) | App-level ping every 25 s. Clients must reconnect and resume. |
| Spectrum (Cloudflare's UDP proxy) needs an Enterprise plan with Spectrum as a paid add-on for custom TCP/UDP applications. | [Spectrum protocols per plan](https://developers.cloudflare.com/spectrum/protocols-per-plan/) | Not a default for a community server. |
| Cloudflare Realtime TURN: UDP 3478 and 443, TCP 3478 and 80, TLS 5349 and 443. $0.05/GB from edge to TURN client, 1,000 GB free a month. Per allocation, roughly 5–10 kpps and 50–100 Mbps. Credentials come from a REST call with a `ttl` and can be revoked. No TCP allocations (RFC 6062). Permissions to private IPs are refused. | [TURN](https://developers.cloudflare.com/realtime/turn/), [credentials](https://developers.cloudflare.com/realtime/turn/generate-credentials/), [FAQ](https://developers.cloudflare.com/realtime/turn/faq/) | Works as a hidden-origin relay with only outbound traffic from the host. One allocation per participant. |
| str0m 0.24.1 (October 2026) is sans-IO with no internal threads. Its author runs it as a production SFU. It does ICE over UDP and TCP, simulcast, VP8, VP9, H.264, AV1, Opus, TWCC bandwidth estimation, NACK, PLI, and RED. It does not do TURN itself: relay candidates are sockets the application adds. Releases still break the API every few weeks. | [str0m README](https://github.com/algesten/str0m), [changelog](https://github.com/algesten/str0m/blob/main/CHANGELOG.md), [crates.io](https://crates.io/crates/str0m) | Pin an exact version. Keep str0m types inside `crates/sfu`. SVC is not a listed feature; use simulcast. |
| tinyclient's `mediad` is a sync, single-UDP-socket, 20 ms loop with cpal audio, libopus (`unsafe-libopus`), FEC/PLC, and a jitter buffer. It speaks Discord voice: plain RTP with AEAD, not SRTP, and no ICE or DTLS. No WebRTC crate is in its `Cargo.lock`. It has no video and no echo cancellation. | tinyclient `docs/architecture.md`, `crates/mediad` | The client needs a WebRTC transport. str0m fits its "no async runtime" rule. Audio code carries over. |
| tinyclient's product rule: one video surface, either the active speaker or one share, and one hardware H.264 encode for a share. | tinyclient `docs/product/06-calls.md` | The SFU forwards at most one H.264 picture per listener, chosen as in the decision table. It does not encode. |

## Realtime channel

### Why WebSocket

Signaling needs both directions on one ordered stream. SSE would need a second HTTP path for every client message. The tunnel carries WebSocket upgrades on every plan. tinyclient already ships `tungstenite` (sync) in `mediad`, so `chatd` can use the same crate without an async runtime.

### Connect

```
GET /v1/live?since=<seq>
Authorization: Bearer <session token>
Authorization: Bot <bot token>
Upgrade: websocket
```

| Rule | Value |
| --- | --- |
| Auth | `Authorization: Bearer <session token>` or `Authorization: Bot <bot token>`. No query-string token. Checked before the upgrade. No token, a bad token, or an unknown scheme is `401 {"error":"unauthorized"}`. A database error on that check is `500 {"error":"unavailable"}`. The socket re-reads the session every 60 s (a bot token is re-read with `authenticate_bot`). It also re-reads at once on `session.ended`, on `member.role` or `member.roles` for itself, and on any `role.updated` or `role.deleted`. `call.join`, `call.kick`, `call.moderate`, `call.move`, and `call.share_end` re-read before they act. An ended session closes with `4001 session_ended`. A database error on a later re-read keeps the socket; the next re-read tries again. A database error on one of those five call requests returns `error` `unavailable` and does not close the socket. |
| Membership | A kick emits `member.removed` to every member socket. That person's sockets close with `4003 removed`, and their call ends at once with `call.left` `removed`. Their sessions are deleted, and `session.ended` is not emitted. A ban emits `member.banned` (ban_members only) and then `member.removed` if they were a member. The banned person's sockets close on `member.banned` with `4003` even when they do not receive that frame. The call ends as `banned`. |
| Session end | Sign-out of one session, sign-out everywhere, and a provider back-channel logout emit `session.ended` with `user_id`. The frame is not sent to clients. Every socket of that user re-checks its own token. A socket whose token is gone closes with `4001`. A socket whose token is still valid stays. Sign-out everywhere deletes every session, so every socket of that user closes. |
| Query | `since=<seq>`, a non-negative integer. Anything else is `400 {"error":"invalid"}`. Other parameters are ignored. |
| Frame size | 64 KiB, for one frame and one message. SDP with simulcast fits. Bigger closes the socket with `1009 too_big`. Binary frames close it with `1003 text_only`. |
| Client message rate | 20 a second, burst 40. A message over that is dropped, and the first one dropped gets `error {code: "slow_down"}`. 40 dropped in a row closes with `4029 slow_down`. |
| Sockets per user | 3. A fourth closes the oldest with `4008 replaced`. |
| Sockets in all | 1,024. Over that, `503 {"error":"busy"}` before the upgrade. |
| Not ready | If the outbox dispatcher has no starting cursor within 5 s, `503 {"error":"unavailable"}` before the upgrade. |
| Keepalive | The server answers a text `ping` with `pong`. It does not send its own ping and it does not check a 25 s interval. No received frame for 60 s closes with `4000 idle`. WebSocket protocol ping and pong count as traffic. A client should send `ping` about every 25 s so a tunnel idle timeout and this 60 s timer both stay clear. |
| Slow consumer | Nothing is buffered without a cap. A send that does not finish in 10 s drops the socket with no close code. Durable events and the ephemeral broadcast (`presence`, `call.joined`, `call.left`, `call.state`) each hold 1,024. Falling behind either one sends `resync` and closes with `4002`. Frames for one socket (`call.slots`, `call.speaking`, `call.moved`, `call.e2ee`, `typing`) use a queue of 64. A frame that does not fit is dropped. That does not close the socket and does not send `resync`. |
| Shutdown | SIGINT or SIGTERM empties the call registry and shuts the SFU down, then closes every socket with `1012 restart`. No `call.left` is sent. A new upgrade while that flag is set is `503 {"error":"restarting"}`. The process then waits up to 5 s for sockets to finish. |
| Encoding | JSON text frames, one object each. The server writes `type` first. A client frame parses as long as it is one JSON object with a string `type`; field order does not matter. A `type` this server does not take returns `error` `unknown_type`. What a client does with an unknown server `type` is not specified by tinyserver. |

Close codes:

| Code | Reason | Client does |
| --- | --- | --- |
| `1009` | `too_big` | Fix the sender |
| `1011` | `unavailable` | The database did not answer. Reconnect with backoff and `since`. |
| `1012` | `restart` | Reconnect with backoff and `since` |
| `4000` | `idle` | Reconnect with `since` |
| `4001` | `session_ended` | Sign in again |
| `4002` | `resync` | Refetch rooms, members, and invites, then reconnect without `since` |
| `4003` | `removed` | Stop. The account is no longer a member: removed or banned. |
| `4008` | `replaced` | Another device of the same person took the place. Do not reconnect in a loop. |
| `4010` | `suspended` | The operator took the community offline. Read `GET /v1/status` for the notice, and reconnect only once it says `normal`. |
| `4029` | `slow_down` | Reconnect after a pause |

Browsers cannot set the `Authorization` header on a WebSocket, so a hostile page cannot open this socket with someone's session. No `Origin` check is needed.

### Bots

`Authorization: Bot <token>` upgrades the same route when the token is live. A bot is not a member. It is absent from `ready.online` and from `presence`. It does not mark `last_seen_at`.

| Scope | Durable | Ephemeral |
| --- | --- | --- |
| `rooms.read` | `room.*` | `ready.calls`, `call.joined`, `call.left`, `call.state` |
| `members.read` | `member.*` except `member.banned`, which is not `Audience::Everyone` | `ready.online`, `presence` |

Anything else is dropped, including invites, bans, messages, `call.slots`, `call.speaking`, `call.moved`, and `call.e2ee`. A bot `call.*` frame is `forbidden`. The three-socket cap applies to the bot id. A revoked token closes with `4001` on the next re-check.

### Two kinds of events

| Kind | Examples | Path | Has `seq` | Survives reconnect |
| --- | --- | --- | --- | --- |
| Durable | server settings; roles; categories and the layout; rooms; members joined, renamed, avatar changed, given roles, timed out, removed, banned; invites; text channels; direct conversations; messages, edits, deletes, and reactions; direct read cursors | `outbox` row, written in the transaction that made the change | Yes | Yes, replayed from `since` |
| Ephemeral | presence, occupancy, speaking, call signaling | Memory, sent straight to the sockets that need it | No | No. `ready` carries a fresh snapshot |

Call occupancy is ephemeral. `call.joined` and `call.left` never touch the outbox and carry no `seq`, so `since` does not replay them. Every `ready`, with or without `since`, carries the whole of `calls`; the client replaces its occupancy with it and applies `call.joined` and `call.left` after that. The socket subscribes to call frames under the same lock that takes the snapshot, so nothing between the two is lost or counted twice. Durable and ephemeral frames take separate paths, so `room.deleted` and that room's `call.left` frames can arrive in either order.

### Outbox and LISTEN/NOTIFY

The `outbox` table and the `outbox_wake` trigger are in `0001_init.sql`; `0003_live.sql` adds the rest.

1. Every room, member, invite, and session write in `crates/db` calls `app_emit(topic, payload)` in the same transaction, just before it commits: `topic` like `room.renamed`, `payload` the public shape of the change. `app_accept_invite` emits from inside the function, because the invite row is behind RLS for the person accepting. A rolled-back change never emits. The runtime role cannot insert, update, or delete outbox rows itself. `app_emit_to` takes the same advisory lock and can store an `audience` array. A null audience is every member socket. An empty audience means the same. A non-empty audience is delivered only to those user ids (`Audience::Users`). Since `0013_private_channel_events.sql`, both functions narrow a null audience on an event about a channel that can be restricted to the members who can view it, so no emit site has to remember to.
2. `app_emit` takes one transaction-scoped advisory lock before it inserts, so ids are handed out in commit order. Without it, a transaction that took id 10 could commit after one that took id 11, and a reader at 11 would never see 10. The lock is held only from the emit to the commit.
3. The commit fires `pg_notify('app_wake', topic)`.
4. One dispatcher task holds a `sqlx::postgres::PgListener` on `app_wake`. It starts at the newest id and keeps that cursor in memory. On a wake, on a listener reconnect, and every 5 s regardless, it reads `WHERE id > cursor ORDER BY id` and sends each row into a `tokio::sync::broadcast` channel of 1,024. Because of step 2 a cursor never skips a row, so `sent_at` is not used. The listener takes one connection, so the runtime pool has six.
5. Each socket task subscribes to that channel before it reads the outbox, and filters per recipient by the permissions it read with the session. Invite events go to people with create_invites or manage_invites, matching the invite RLS; `member.banned` to people with ban_members. A `member.role` or `member.roles` event for the socket's own user, and any `role.updated` or `role.deleted`, re-read that socket's permissions, so new roles start invite events on the same socket. `session.ended` is never sent; it makes that user's sockets re-check their session.
6. The row `id` is the event `seq`. Seqs only grow, but have gaps: rolled-back ids, and events the socket may not see. A client reconnects with `?since=<last seq>`. `ready.seq` is then `since`, and the server replays rows with `id > since` that the caller may see, then streams. Without `since`, `ready.seq` is the newest seq, and the client should fetch REST state after `ready`.
7. `resync`, then close with `4002`, when `since` is below the highest pruned seq, above the newest seq (a seq this server never issued), more than 1,000 events back, or when the socket falls more than 1,024 events behind live.
8. The existing 10-minute sweep (`app_sweep`) deletes outbox rows older than 24 h. It deletes every id up to the newest old row, so what is kept is one unbroken run, and it records the highest deleted id in `outbox_pruned`. A client may resume up to 24 h back.

One process means one dispatcher, so there is no ordering problem across nodes.

Audit rows stay out of the live channel. `GET /v1/audit` is unchanged.

### Server to client

| `type` | Fields | When |
| --- | --- | --- |
| `ready` | `seq`, `me {user_id, role}`, `online [user_id]`, `calls [{room_id, participants [participant]}]` | First frame after connect. Snapshot of everything ephemeral. `online` is sorted, includes the caller, and excludes bots. `calls` lists rooms with anyone in them, sorted by `room_id`, participants in join order. A participant is `{user_id, muted, deafened, camera, sharing, server_muted, server_deafened, suppressed, priority, e2ee}`; later versions may add fields. `e2ee` is whether they joined able to encrypt end to end. |
| `server.updated` | `seq`, `server` | Durable. `server` is `GET /v1/server`. |
| `role.created`, `role.updated` | `seq`, `role` | Durable. `role` is a `GET /v1/roles` row. |
| `role.deleted` | `seq`, `role_id` | Durable |
| `roles.reordered` | `seq`, `role_ids` | Durable. Most senior first. |
| `category.created`, `category.updated` | `seq`, `category {id, name, position}` | Durable |
| `category.deleted` | `seq`, `category_id` | Durable. Its rooms are uncategorized now. |
| `layout.updated` | `seq`, `categories`, `rooms` | Durable. The order after `PUT /v1/layout`. |
| `room.created`, `room.updated`, `room.renamed` | `seq`, `room {id, name, created_by, created_at, category_id, position, topic, user_limit, bitrate_kbps, ptt_only, video_enabled, overrides}` | Durable. `GET /v1/rooms` without `my_permissions`, which differs per person: refetch. Any change sends `room.updated`; a name change also sends `room.renamed` first, for clients from before `room.updated`. An override change sends `room.updated`. |
| `room.deleted` | `seq`, `room_id` | Durable. Ends that room's call: everyone in it gets `call.left {reason: "room_deleted"}`. |
| `member.joined`, `member.role`, `member.roles`, `member.renamed` | `seq`, `member {user_id, display_name, role, owner, roles}` | Durable. No email; `GET /v1/members` has it. `role` is the legacy string or the owner flag. Setting the owner flag emits `member.role` and does not emit `member.roles`. Replacing role ids emits `member.roles`, and emits `member.role` as well only when the legacy string changed. Setting the legacy role emits both. `member.renamed` is a display name change. |
| `member.avatar` | `seq`, `member` | Durable. The caller's avatar was set or cleared. `member` is the same member object as `member.renamed`. |
| `member.timeout` | `seq`, `member`, `timeout_until` | Durable. `timeout_until` is `null` when a timeout ended. A timeout that starts ends their call with `timed_out`. |
| `member.removed` | `seq`, `user_id` | Durable. After a kick or a ban. |
| `member.banned` | `seq`, `user_id`, `ban` | Durable. ban_members only. The banned person's sockets close on this event with `4003` even when they do not receive the frame. `ban` is a `GET /v1/bans` row. |
| `invite.created`, `invite.accepted` | `seq`, `invite {id, role, created_by, created_by_name, expires_at, accepted_at, accepted_by, label, max_uses, uses, role_ids}` | Durable. create_invites or manage_invites only. The same shape as `GET /v1/invites`. |
| `invite.revoked` | `seq`, `invite_id` | Durable. create_invites or manage_invites only. Invites the sweep deletes after they expire send nothing. |
| `presence` | `user_id`, `online` | First socket opens, or last socket closes after a 10 s grace. Several sockets for one person are one presence. |
| `call.joined` | `room_id`, and the participant's fields | Ephemeral. Someone joined a room's call. Sent to every member, including the person who joined, so the sidebar shows who is in which room. `suppressed` is true when they may not speak there; `server_muted` and `server_deafened` follow them from a move. |
| `call.left` | `room_id`, `user_id`, `reason` | Ephemeral. Someone left a room's call. `reason` is below. Sent to every member. |
| `call.state` | `room_id`, and the participant's fields | Ephemeral. Someone in a call changed their mute, deafen, or priority, their camera started or stopped, their share started or ended, a moderator server muted or deafened them, or a role or override changed whether they may speak (`suppressed`) or hold priority. `priority` is true while they hold the priority-speaker key and have priority_speaker in the room. While anyone with `priority` speaks, listeners turn everyone else down by 12 dB; that is client-side. `camera` is true while their offer's camera m-line is sending (`sendonly` or `sendrecv`). `sharing` is true while they hold the room's share. Sent to every member, the sharer included, so a sharer whose share an owner or admin ended learns it here. A sharer who leaves gets `call.left` and no `call.state`. |
| `call.moved` | `room_id` | To the socket that holds the seat. Leave and join `room_id` with a new offer, keeping your own mute and deafen; no confirmation. The next join of that room may pass its `user_limit` for 30 s, still capped by `TINYSERVER_CALL_ROOM_MAX`. The leave is `moved` only when that join is the ticket's room. A join of some other room is `joined_elsewhere`. If that seat socket is already in the disconnect grace, `call.moved` is dropped and the ticket still exists. |
| `ok` | `ref` | `call.join` without media, `call.leave`, `call.kick`, `call.share_end`, `call.state`, `call.quality`, `call.moderate`, `call.move`, or `typing` succeeded. `ref` is omitted when the request had none. On the requester's socket this frame is written before the `call.joined`, `call.left`, or `call.state` that request caused. Other sockets are sent those occupancy frames when the registry changes, which is before the reply is written. |
| `call.answer` | `ref`, `call_id`, `sdp` | Reply to a `call.join` with an offer, in place of `ok`, and to `call.offer`. Same socket order as `ok`: the requester sees `call.answer` before the occupancy frame; other sockets can see `call.joined` or `call.state` first. `call_id` names this media session; a rejoin gets a new one. |
| `call.slots` | `call_id`, `audio [{mid, user_id}]`, `video` | Which receive slot carries whom. The whole map, sent to the socket in the call whenever it changes, and once after `call.answer`. `video` is `{mid, user_id, kind}` while the video slot carries someone, else `null`. `kind` is `camera` or `share`. It becomes `null` as soon as that source stops or the slot is planned empty. It names a new source only when the SFU writes that source's first keyframe, so reset the video decoder then. |
| `call.speaking` | `room_id`, `user_ids` | Who is audible and speaking, in SFU join order, at most every 250 ms. Omitted when self-muted, server-muted, or suppressed. Sent to the seat socket of each person in that room's call, through the queue of 64. A person's other sockets do not get it. |
| `call.e2ee` | `room_id`, `op`, and the op's fields | End-to-end encryption for the call this socket holds a seat in: `start`, `off`, `proposals`, `commit`, `welcome`. See [End-to-end encryption](#end-to-end-encryption). |
| `resync` | none | The server closes with `4002` next. Client refetches rooms and members, then reconnects with no `since`. |
| `error` | `ref`, `code` | A client request failed. `ref` is echoed when the request had one. |
| `pong` | none | Reply to `ping` |

### Client to server

| `type` | Fields | Meaning |
| --- | --- | --- |
| `ping` | none | Keepalive |
| `call.join` | `ref`, `room_id`, `sdp`, `e2ee` | Join the room's call. Leaves any other call first. `sdp` is the offer. With an SFU configured it is required (`bad_frame` without it). With no SFU, a join with it gets `media_unavailable` and a join without it takes a seat with no audio. If the handle is present but the SFU task has stopped, an offer is `media_unavailable`. `e2ee` is optional: the highest end-to-end encryption version the app speaks, `1` today. |
| `call.e2ee` | `op`, and the op's fields | MLS for the caller's call: `key_package`, `commit`, `invalid`. No `ok`: the frames that follow answer it. See [End-to-end encryption](#end-to-end-encryption). |
| `call.leave` | `ref` | Hang up. Not being in a call is not an error. Closing the socket that joined does the same after a 10 s grace, unless the same session opens a new socket first. |
| `call.kick` | `ref`, `room_id`, `user_id` | disconnect_members in that room, and a rank above the person. Removes them from that room's call. Audited. |
| `call.share_end` | `ref`, `room_id` | disconnect_members in that room, and a rank above the sharer. An owner skips the rank check and can end their own share. A member cannot; they stop theirs with `call.offer`. Ends the screen share in that room's call. Audited as `call.share_end`. The sharer's m-line keeps sending until they renegotiate; the SFU drops it, and the share stays ended until an offer sets that m-line `inactive` and a later one starts it again. |
| `call.offer` | `ref`, `sdp` | Renegotiate the caller's media session, for example to start a camera or a share. The reply is `call.answer`. A camera or share that started or stopped is shown to everyone as `call.state`. A share someone else in the room holds is `share_taken`. Counts against the join and leave rate, so a client sending them fast gets `slow_down`. |
| `call.state` | `ref`, `muted`, `deafened`, `priority` | `muted` and `deafened` are required; `priority` is optional and false when left out. Shown to everyone as `call.state`, and in `ready`. The SFU stops forwarding a muted publisher's audio, and sends a deafened listener nothing, video included. Mute does not stop a camera. `priority` counts only with priority_speaker in the room; send `true` while the priority push-to-talk key is held. |
| `call.moderate` | `ref`, `room_id`, `user_id`, `server_muted`, `server_deafened` | At least one of the two booleans. `server_muted` needs mute_members in the room, `server_deafened` deafen_members, and both a rank above the person, who must be in that room's call. The SFU stops forwarding a server-muted person's audio and sends a server-deafened person no audio; video is untouched. It lasts until undone or until they leave calls altogether: a move or a join elsewhere keeps it. Audited as `call.server_mute`. |
| `call.move` | `ref`, `user_id`, `room_id` | `call.move` to the room the person is already in re-reads the session and returns `ok`. It does not check `move_members` and does not issue a ticket. A move to another room checks that the room exists (`not_found`), `move_members` in both rooms (`forbidden`), and rank (`hierarchy`), audits `call.move`, sends `call.moved` to the socket that holds the seat, and lets their next join of that room pass the room's `user_limit` for 30 s, still capped by `TINYSERVER_CALL_ROOM_MAX`. If that seat socket is already in the disconnect grace, `call.moved` is dropped and the ticket still exists. The next successful join clears the ticket. The leave is `moved` only when that join is the ticket's room. A join of some other room is `joined_elsewhere`. |
| `call.quality` | `ref`, `video` (`high`, `low`, `off`) | What the caller's one video slot may carry: `low` caps it at `l`, `off` is audio only. It starts at `high`. Not saved; a rejoin starts at `high` again. |

`ref` is any JSON value. It is echoed in the `ok` or `error` reply. Requests without one get replies without one.

Error codes:

| Code | Meaning |
| --- | --- |
| `not_found` | `call.join`: no such room, a room the caller cannot see (no view_channel), or `room_id` is not a room id. `call.kick` and `call.moderate`: that person is not in that room's call. `call.move`: that person is not in a call, or the room does not exist. `call.share_end`: nobody shares in that room's call. For kick, moderate, and share-end this is decided before the permission check, so a caller without the bit can still learn it. Occupancy is already sent to every member. `call.offer`, `call.state`, and `call.quality`: the caller is not in a call with media, or in a call at all. |
| `forbidden` | `call.join` without connect in that room. `call.kick`, `call.share_end`, `call.moderate`, or `call.move` without the permission it needs. A member may not send `call.share_end` even for their own share; they stop it with `call.offer`. |
| `hierarchy` | `call.kick`, `call.share_end`, `call.moderate`, or `call.move` against someone whose rank is not strictly below the caller's. An owner skips this and can act on anyone, including themselves. Anyone else cannot act on an owner, on an equal or higher role, or on themselves. |
| `timed_out` | `call.join` from someone in a timeout |
| `room_full` | The room's call has as many people as its `user_limit`, or 12 when it has none (`TINYSERVER_CALL_ROOM_MAX`). Earlier versions sent `full`. |
| `server_full` | 32 people are in calls on the server |
| `slow_down` | With a `ref`: too many joins and leaves from this socket. Without one: too many messages. |
| `unavailable` | The database did not answer, or the server is shutting down. Try again. |
| `unknown_type` | A `type` this server does not take, such as `call.candidate` before slice 7 |
| `bad_frame` | Not a JSON object with a `type`, or a request without a field it needs. A `call.moderate` with neither `server_muted` nor `server_deafened`. A `call.join` without `sdp` while media runs. A `call.quality` whose `video` is not `high`, `low`, or `off`. A `call.share_end` without `room_id`. |
| `bad_sdp` | The SFU could not parse or accept the offer, or the offer is over the caps: 32 KiB, more audio m-lines than the room cap, more than 3 video m-lines, or any other kind of m-line |
| `media_unavailable` | A `call.join` or `call.offer` with an offer on a server without media, or with the SFU stopped |
| `e2ee_required` | A `call.join` without `e2ee` to a room whose `e2ee_required` is on, while media runs. No seat is taken. |
| `bad_e2ee` | A `call.e2ee` key package or commit that is not what it must be. See [End-to-end encryption](#end-to-end-encryption). |
| `share_taken` | A `call.join` or `call.offer` whose offer starts a share while someone else in the room holds it. Nothing changes: no seat for a join, and the caller's media as it was for an offer. str0m flips an m-line when it makes the offer, so the client sets the share m-line back to `inactive` locally and does not send that offer again. |

`call.left` reasons:

| Reason | When |
| --- | --- |
| `left` | `call.leave` |
| `joined_elsewhere` | The same person joined another room |
| `disconnected` | The socket holding the seat closed, the same session opened no new socket, and none of the person's sockets joined again within 10 s |
| `room_deleted` | The room was deleted |
| `removed` | The person was removed from the community |
| `session_ended` | The socket that joined closed with `4001 session_ended` |
| `kicked` | Someone with disconnect_members sent `call.kick` |
| `moved` | Someone with move_members moved them, and they joined the other room |
| `banned` | They were banned |
| `timed_out` | A timeout started |
| `media_timeout` | The SFU participant was not connected within 15 s of the SFU accepting the join (`sfu::Config::connect_timeout`). That is slightly before `call.answer` is written. |
| `media_lost` | After connect, the SFU heard nothing for 30 s (`sfu::Config::consent_timeout`), or the RTC closed, died, or failed a poll. |

```json
{"type":"ready","seq":41,"me":{"user_id":"9d1e…","role":"member"},"online":["5a0c…","9d1e…"],"calls":[]}
{"type":"room.renamed","seq":42,"room":{"id":"0b6f…","name":"Lobby","created_by":"5a0c…","created_at":"2026-10-24T12:00:00Z"}}
{"type":"presence","user_id":"5a0c…","online":false}
```

```json
{"type":"ready","seq":41,"me":{"user_id":"9d1e…","role":"member"},"online":["5a0c…","9d1e…"],"calls":[{"room_id":"0b6f…","participants":[{"user_id":"5a0c…"}]}]}
{"type":"call.join","ref":"7","room_id":"0b6f…"}
{"type":"ok","ref":"7"}
{"type":"call.joined","room_id":"0b6f…","user_id":"9d1e…"}
{"type":"call.kick","ref":"8","room_id":"0b6f…","user_id":"9d1e…"}
{"type":"error","ref":"8","code":"forbidden"}
{"type":"call.left","room_id":"0b6f…","user_id":"9d1e…","reason":"left"}
```

With media, a join carries the offer:

```json
{"type":"call.join","ref":"7","room_id":"0b6f…","sdp":"v=0\r\n…"}
{"type":"call.answer","ref":"7","call_id":"c-3f2a…","sdp":"v=0\r\n…"}
{"type":"call.joined","room_id":"0b6f…","user_id":"9d1e…"}
{"type":"call.slots","call_id":"c-3f2a…","audio":[{"mid":"aB3","user_id":"5a0c…"}],"video":null}
{"type":"call.speaking","room_id":"0b6f…","user_ids":["5a0c…"]}
{"type":"call.state","ref":"9","muted":true,"deafened":false}
{"type":"ok","ref":"9"}
{"type":"call.state","room_id":"0b6f…","user_id":"9d1e…","muted":true,"deafened":false}
```

`call.slots` and `call.joined` may arrive in either order after `call.answer`. Mids are whatever the client's offer named them.

A camera starts with a renegotiation:

```json
{"type":"call.offer","ref":"10","sdp":"v=0\r\n…"}
{"type":"call.answer","ref":"10","call_id":"c-3f2a…","sdp":"v=0\r\n…"}
{"type":"call.state","room_id":"0b6f…","user_id":"9d1e…","muted":false,"deafened":false,"camera":true}
```

and the others in the room get it in their video slot:

```json
{"type":"call.slots","call_id":"c-77b0…","audio":[{"mid":"aB3","user_id":"9d1e…"}],"video":{"mid":"vR1","user_id":"9d1e…","kind":"camera"}}
{"type":"call.quality","ref":"11","video":"low"}
{"type":"ok","ref":"11"}
```

A share starts the same way, with the share m-line `sendonly`. A second sharer is refused, and an owner or admin may end it:

```json
{"type":"call.offer","ref":"12","sdp":"v=0\r\n…"}
{"type":"call.answer","ref":"12","call_id":"c-3f2a…","sdp":"v=0\r\n…"}
{"type":"call.state","room_id":"0b6f…","user_id":"9d1e…","muted":false,"deafened":false,"camera":true,"sharing":true}
{"type":"call.slots","call_id":"c-77b0…","audio":[{"mid":"aB3","user_id":"9d1e…"}],"video":{"mid":"vR1","user_id":"9d1e…","kind":"share"}}
{"type":"call.offer","ref":"4","sdp":"v=0\r\n…"}
{"type":"error","ref":"4","code":"share_taken"}
{"type":"call.share_end","ref":"5","room_id":"0b6f…"}
{"type":"ok","ref":"5"}
{"type":"call.state","room_id":"0b6f…","user_id":"9d1e…","muted":false,"deafened":false,"camera":true,"sharing":false}
```

### Messages

Text channels and direct messages are built, on this same socket. A voice room is still only a call. It has no message rows. Text is a `channels` row, optionally in a category, or a `directs` row between two members.

Durable topics:

| `type` | Who | Payload besides `seq` |
| --- | --- | --- |
| `channel.created`, `channel.updated` | Every member socket | `channel` |
| `channel.deleted` | Every member socket | `channel_id` |
| `direct.created` | The two people | `direct` with `id`, `low_user_id`, `high_user_id`, `created_at` |
| `message.created`, `message.edited`, `message.deleted` | Every member socket for a channel. The two people for a direct | `conversation_id`, `kind` (`channel` or `direct`), `message` |
| `reaction.updated` | Same split as messages | `message_id`, `conversation_id`, `kind`, `emoji`, `on`, `user_id`, `count` |
| `read.updated` | Both people in a direct | `conversation_id`, `kind` `direct`, `user_id`, `message_id` |

A channel read is stored and emits nothing. Channel message, reaction and channel events go to every member socket when every member can view the channel, and only to the members who can view it when it can be restricted (`0013_private_channel_events.sql`). The audience is fixed when the event is emitted. `channel.deleted` carries only the id and goes to every member. Direct events set the audience to those two user ids. Bots do not receive these topics.

`message.created` and the other message writes are HTTP (`POST /v1/conversations/{id}/messages` and the routes next to it). The frame carries ids and the message object, not file bytes. A message body is at most 4,000 characters. The 64 KiB socket frame cap still applies.

A client may send `{"type":"typing","conversation_id":"…"}`, with an optional `ref`. Success is `ok`. The relay is `{"type":"typing","conversation_id":"…","user_id":"…"}` to the other people, through the per-socket queue of 64, and it is not stored. It uses the same per-socket message rate as other client frames. The sender needs to be a member, not banned, and not in a timeout, and needs speak in that channel. A direct goes to the other person. A channel goes to other members who can view it. Errors: `bad_frame` with no `conversation_id`, `not_found` when the conversation is missing or not visible, `forbidden` for no speak, a timeout, a ban, or a non-member, `unavailable` when the database does not answer. There is no `timed_out` code on typing.

## Signaling and SDP

The client is always the offerer. The server never sends an offer, so there is no glare and no rollback.

| Offer contents | Why |
| --- | --- |
| One `sendonly` audio m-line | The client's microphone. Opus, `useinbandfec=1`, `usedtx=0`. DTX is off; the speech gate ends a spurt. RFC 6464 audio level extension. If there are several, the SFU takes the first. |
| One video m-line for the camera, `inactive` until the camera starts | H.264 simulcast: `a=rid:h send`, `a=rid:l send`, `a=simulcast:send h;l`. `sendonly` while the camera runs. `sendrecv` also counts as on. The first video m-line that is not `recvonly` is the camera. Rid `l` is the low layer. No rid, rid `h`, or any other rid is the high layer. |
| One video m-line for the share, `inactive` until a share starts | The second video m-line that is not `recvonly`. H.264, one layer. A rid on it is ignored. `sendonly` or `sendrecv` is sharing. A missing direction is `sendrecv`, so it starts a share. Write `a=inactive` until the share starts. |
| `N - 1` `recvonly` audio m-lines, where `N` is the room cap | One per possible other speaker. Mapped by `call.slots`. With the default cap, 11. The SFU uses as many as are offered; others in a bigger room are not heard until a slot frees. |
| One `recvonly` video m-line | The single video slot. Mapped by `call.slots` `video`. If there are several, the SFU uses the first. |

The SFU enables Opus, and H.264 constrained baseline level 3.1 (`profile-level-id=42e01f`) in packetization mode 1 with RTX, and nothing else. Opus is `enable_opus(true, false)`: in-band FEC on, RED off, `usedtx` omitted (default 0). str0m offers `transport-cc` on that Opus payload. NACK, PLI, FIR, RTX, and REMB are on the H.264 payload, not on Opus. Nothing in the SFU adapts the Opus bitrate. Before str0m parses an offer, the SFU's handle checks it against the caps under `bad_sdp`: the room cap bounds the audio m-lines (one microphone and `N - 1` slots), 3 video m-lines hold the camera, the share, and the video slot, and other kinds, such as a data channel, are refused. An 11-slot offer from the headless client, with its three video m-lines, is about 14 KB.

Starting or stopping a camera or share is a `call.offer` that flips that m-line's direction: `sendonly` or `sendrecv` to start, `inactive` to stop. The answer tells the API whether the camera m-line is now sending and whether the caller holds the room's share, and a change goes to everyone as `call.state`. A `call.join` whose offer already has the camera or share sending (`sendonly` or `sendrecv`) is shown the same way after `call.joined`. Nobody joining or leaving changes SDP; only `call.slots` changes.

A share starts when its m-line goes from not sending to sending and nobody else in the room holds the share. The SFU reads that from the offer's text, the second video m-line that is not `recvonly`, before str0m takes the offer, so `share_taken` leaves the caller's media untouched. An offer whose share m-line was already sending starts nothing: a share an owner or admin ended stays ended through a camera renegotiation. Send a frame on a new m-line only once its answer is accepted; the SFU drops what comes before it takes the offer, and could keep the tail of a frame cut that way.

The answer carries the SFU's ICE-lite credentials, its DTLS fingerprint, and its candidates: one UDP host candidate and one TCP passive candidate (`tcptype passive`) on the public address, and the same pair on `TINYSERVER_LAN_IP` when it is set. The client does not trickle. The SFU learns the client's NAT address from its first STUN check (a peer-reflexive candidate). ICE credentials are random per join and only travel over the authenticated socket.

## SFU

### Shape

`crates/sfu` owns one OS thread. It holds the UDP socket, the ICE-TCP listener and its streams, and one `str0m::Rtc` per participant. It never touches Postgres or the session table. The API crate never sees an RTP packet, and no str0m type crosses the crate boundary.

```
 tokio runtime (api crate)                       sfu thread (std, mio)
 ┌───────────────────────────┐   Command (mpsc)  ┌────────────────────────────┐
 │ /v1/live socket tasks     │ ────────────────▶ │ rooms: room_id → Call      │
 │ dispatcher (outbox)       │                   │ Call: user_id → Rtc, slots │
 │ call registry (who/where) │ ◀──────────────── │ UDP :50000, TCP :50000     │
 └───────────────────────────┘   Event (mpsc)    └────────────────────────────┘
```

| Piece | Choice |
| --- | --- |
| Loop | `mio::Poll` over the UDP socket, the TCP listener, accepted TCP streams, and a `mio::Waker`. Timeout is the earliest `Output::Timeout` across all `Rtc`s, capped at 50 ms. Each `Rtc` is drained until it returns a timeout, as str0m requires, after every input and every frame written to it. |
| Packet routing | A per-source packet budget first. Then look up the protocol and source address in a map. Unknown address: only STUN (a first byte of 0 to 3, RFC 7983) gets past, routed by ICE username, and everything else is dropped before str0m sees it. `Rtc::accepts` decides. Every datagram and TCP frame is handed to str0m with the public address as its destination, because the socket binds every interface and, in Docker, never sees the address the client used. Traffic to the LAN candidate is answered the same way. |
| Slots | Each listener's `recvonly` m-lines are its slots. A joiner takes the first free slot in every other listener's map and fills its own with everyone already there, in join order. A leaver's slot frees and goes to anyone in the room without one. Every change sends that listener's whole map. |
| Forwarding | Each audio frame from a publisher that is neither muted nor silenced is written to every other connected listener in the room that is neither deafened nor hushed, on that listener's slot for the publisher, with the publisher's RTP time and the receive time as wallclock. str0m packetizes it again, so each slot has its own SSRC and sequence numbers. Each camera or share frame goes to every listener whose video slot carries that layer of that camera, or that share. |
| Video slot | Each listener's one `recvonly` video m-line carries the room's share, or one layer of one camera. Planned every 50 ms and on every change: the share, for everyone but its sharer; else the newest holder of the floor (below) with a camera on, other than the listener; with nobody yet, the first other camera in join order. A listener never gets their own camera or share, so the speaker gets the previous speaker and the sharer keeps the camera they would have had. A self-deafened listener gets no picture. Server mute, self mute, and server deafen do not empty it. |
| Switching | A new source, another camera or another layer, is wanted first and shown later: the slot keeps the old one until the wanted one sends a keyframe, then switches on that keyframe and sends `call.slots`. The SFU asks for it with PLI at once and again every 300 ms until it comes. If the old source is gone, the slot is empty until then. A PLI or FIR from a listener goes to the publisher of what its slot shows. Video RTP time is rewritten at 90 kHz when the picture source changes: a new source starts at the last time written plus the wall time since, and keeps that offset. |
| Timestamps | Frame mode keeps one SSRC and one run of sequence numbers per slot, because str0m packetizes each write again. Audio keeps the publisher's RTP time. The receive time is the wallclock. A slot that `call.slots` points at someone else then carries that person's RTP clock, so the client resets that slot's jitter buffer. Video RTP time is rewritten at 90 kHz when the picture source changes; that rewrite is in the switching row. |
| ICE-TCP | Passive only (RFC 6544), on the same port number as UDP. str0m 0.24.1 takes TCP candidates with `tcptype` and pairs by protocol, and emits each `Transmit` with its protocol; it does no socket work and no framing, so the SFU owns both: RFC 4571 framing (a 2-byte length), frames of 1 to 2,000 bytes, and anything else closes the stream. A stream's first frame must be STUN, or it is closed; until a participant's `Rtc` accepts one, the stream is unverified. A stream is verified to the participant whose STUN it carried first, and closes when they leave. Each `Transmit` over TCP goes to the stream from that address, framed; a stream with 32 KiB of audio or of anything else waiting drops the packet, as UDP would, and audio is written first at the next frame boundary. One stream reads at most 64 KiB a turn of the loop. |
| Media API | str0m frame mode, as in its `chat` example. Switching which speaker feeds the one video slot is a frame-level choice. The SFU never looks inside a frame beyond str0m's keyframe flag. RTP mode is a later optimisation. |
| Simulcast | str0m delivers each layer as `MediaData` with its `rid`. Per listener, pick `h` or `l` from its bandwidth estimate, its `call.quality`, and the upload budget: `h` from 1,600 kb/s up, back to `l` below 1,300 kb/s. With no estimate yet, `l`. A layer with no frame within budget for 1 s is skipped for the other, though never up to `h` past a `low` cap. |
| Bandwidth estimate | str0m's TWCC estimator on each listener's `Rtc` (`RtcConfig::enable_bwe`), starting at 1,000 kb/s, read from `Event::EgressBitrateEstimate(BweKind::Twcc)`. `Rtc.bwe().set_desired_bitrate` asks str0m to probe with padding. The target follows the picture that listener should receive: 2,500 kb/s while the slot wants the share, 2,000 kb/s while it wants a camera and the cap still allows `h`, and 350 kb/s otherwise (camera capped at `l`, or no picture). A REMB on the video slot still caps the TWCC estimate; the lower one wins. With no estimate, the camera layer stays `l`. |
| Active speaker | From the RFC 6464 audio level on each publisher's audio: an average weighted 0.4 to the newest packet, speaking above -50 dBov, held 1.5 s after the last loud packet. No decoding. It feeds `call.speaking`, and the floor. |
| Floor | Who the video follows. The first speaker takes it at once. A challenger takes it after 800 ms as the loudest speaker, and while the holder still speaks must be 6 dB louder, so two people talking do not flap the video. Holders are kept newest first, for "the previous speaker". |
| Share | One per room, held by whoever started it first. While it runs, every listener's video slot but the sharer's carries it, and their cameras stop at the share's first keyframe. It is one layer, so a listener's bandwidth estimate does not choose it: `call.quality` `high` and `low` both get it, `off` does not. If the upload budget withholds the share, those listeners' slots stay empty until the share ends or the budget fits again. It ends when the sharer's offer stops the m-line, when they leave, or on `call.share_end`; slots go back to a camera on that camera's next keyframe, asked for with PLI. |
| Crypto | `str0m` feature `aws-lc-rs` only. The crate does not enable `rust-crypto`. |
| Version | `str0m = "=0.24.1"`, the newest release on crates.io in October 2026, without default features (no example web server) and with `aws-lc-rs`. Bump on purpose, with the call tests. |
| DTLS certificate | One per process, made at start. Every answer carries its fingerprint. |
| Call ids | `c-` and 16 hex digits, unique among live participants. Only an id, not a secret. |

### Commands and events

As built, behind `sfu::Media` (a cloneable handle) and `sfu::Event`:

| API → SFU | | SFU → API | |
| --- | --- | --- | --- |
| `join(room_id, user_id, offer)` | Returns `Answer {call_id, sdp, camera, share}`, or `BadSdp` / `ShareTaken` / `Unavailable`. Replaces the person's participant in any room, without a `Left`. Caps are the API's. | `Connected {call_id}` | ICE and DTLS are up. Logged only. |
| `offer(call_id, offer)` | Renegotiate. `NotFound` for an unknown call id, `ShareTaken` for a share someone else holds. `camera` says whether the camera m-line sends now, `share` whether they hold the room's share. | `Slots {call_id, audio [{mid, user_id}], video}` | Sent as `call.slots`. `video` is `Option<VideoSlot {mid, user_id, kind}>`, `kind` `Camera` or `Share`. |
| `state(call_id, muted, deafened)` | The participant's own mute and deafen | `Speaking {room_id, user_ids}` | At most every 250 ms, to each seat socket |
| `moderate(call_id, silenced, hushed)` | Moderation: a `silenced` publisher's audio is not forwarded (server mute, or no speak), a `hushed` listener is sent no audio (server deafen). Video is untouched. | | |
| `quality(call_id, Quality)` | `High`, `Low`, or `Off`, from `call.quality` | | |
| `end_share(call_id)` | From `call.share_end`. Stops forwarding the share until the m-line stops and starts again. | | |
| `leave(call_id)` | Drops the participant. No `Left` follows. | `Left {call_id, lost}` | `Timeout`: not connected within 15 s of the SFU accepting the join. `Gone`: no packet for 30 s, or the RTC closed, died, or failed a poll. |
| `shutdown()` | Graceful stop | `Load {egress_kbps, participants}` | Every 5 s. Logged at debug. |

There is no `EndRoom`: a deleted room ends each person's seat, and each one sends `leave`. Commands go through a bounded `std::sync::mpsc` channel of 1,024 with `try_send`, and wake the loop with the `Waker`; a full queue refuses a join with `media_unavailable`. Events go back through a bounded `tokio::sync::mpsc` channel of 1,024. `Speaking` and `Load` use `try_send` and are dropped when it is full; the rest wait for room.

The API keeps the newest `call.slots` frame for every live call id, so slots the SFU sends before the seat exists reach the socket right after its `call.answer`. An SFU event for a call id no seat holds is stale and ignored.

### Who may join

The API decides. The SFU trusts a join that reaches it. On `call.join`:

1. `room_id` is required (`bad_frame`). With an SFU handle present, `sdp` is required (`bad_frame` without it). With no SFU handle, `sdp` is `media_unavailable`. If the handle is present but the SFU task has stopped, `sdp` is `media_unavailable`. These checks do not spend the join rate.
2. The socket is under its join and leave rate: 5 at once, then one more every 3 s. Over it is `slow_down` and spends nothing else. `call.leave` and `call.offer` share this bucket. `call.kick`, `call.moderate`, `call.move`, `call.share_end`, `call.state`, and `call.quality` do not.
3. The session is re-read. An ended session closes the socket with `4001`. A database error is `unavailable`.
4. The room exists and they can see it: `app_call_access` plus `view_channel`. Missing room, bad id, or no view is `not_found`. No `connect` is `forbidden`. A timeout is `timed_out`. No `speak` still joins, with `suppressed` true. The SFU then forwards none of their audio. Camera and share are not stopped by `suppressed`. They do not take the floor by speaking, because a silenced publisher's audio level is not read, but a live camera of theirs can still be the join-order picture. `video`, `share_screen`, and `voice_activity` are not checked. `ptt_only` and `video_enabled` are not checked.
5. While an SFU is running, a room with `e2ee_required` refuses a join whose `e2ee` is not a number of 1 or more, with `e2ee_required`, and takes no seat. MLS is specified in End-to-end encryption.
6. Caps, before the SFU join, because that join replaces this person's media in any room. The room allows its `user_limit` when that is 1 to 99, otherwise `TINYSERVER_CALL_ROOM_MAX` (default 12), and never more than that server cap. One more is `room_full`. People in calls on the server, default 32 (`TINYSERVER_CALL_SERVER_MAX`), one more is `server_full`. Someone already in a call does not count a second time. Rejoining the room they are already in does not re-check the cap. A `call.move` ticket lets them pass that room's `user_limit` for 30 s and still stops at the server room cap. Shutting down is `unavailable`.
7. With media, the SFU accepts the offer. `bad_sdp` or `share_taken` takes no seat. A full SFU command queue is `media_unavailable` (see the SFU command section).
8. The seat is taken. If they were in another room, that call ends as `joined_elsewhere`, or as `moved` when the ticket was for this room. Joining the room they are already in sends no `call.joined` or `call.left`. The seat moves to this socket and, with media, to the new call id. The SFU drops the old participant and does not emit `Left`. If `e2ee` changed, everyone gets `call.state`. A camera or share that the new answer turns on or off is `call.state` as well.
9. The room is read again. A delete that committed in between ends the seat with `room_deleted` and the join returns `not_found`. A seat refused after the SFU join drops that participant.

`call.leave` with no seat is `ok` and still spends the rate. A `call.offer` from someone not in a media call spends the rate and then returns `not_found`.

### Call registry

`crates/api/src/live/call.rs` holds who is in which call. It sits under the same lock as presence, and every change is sent to every socket from inside that lock, so frames go out in the order the registry changed.

| Rule | Behaviour |
| --- | --- |
| One call per person | A seat belongs to the person, not the socket. Any of their sockets may leave it. |
| The socket that joined | Holds the seat. A later `call.join` to the same room from another of their sockets takes the seat over without a frame. |
| That socket closes | The person leaves with `disconnected` after the presence grace (10 s), unless the same session opens a new socket or one of their sockets joins again first. A close with `4003` or `4001` leaves at once with `removed` or `session_ended`. |
| The same session opens a new socket | The seat moves to it with no `call.join`, the old socket closed or not, so a blip on the live socket does not touch media. It keeps its SFU participant. Nobody is told; `ready` lists the person in the call, and that socket then gets the latest `call.slots` and any `call.e2ee` frames that came while no socket held the seat. Another device of the same person does not take the seat this way, only with `call.join`. A seat whose app really died now lasts until the SFU's consent timeout rather than the grace, unless a `call.join` replaces it. |
| Room deleted | The dispatcher sees `room.deleted` and ends the room's call with `room_deleted`. |
| Member removed | The dispatcher sees `member.removed` and ends their call with `removed`, even if the socket that joined had already gone. |
| Kick | `call.kick` re-reads the caller's session. The target must be in that room's call. disconnect_members in the room and the hierarchy are checked in Postgres with the `call.kick` audit row (`subject` `call`, `subject_user_id`, `detail {room_id, user_id}`) in one transaction, then the person leaves with `kicked`. |
| Share end | `call.share_end` re-reads the caller's session. Someone in that room's call must hold the share. The same checks as a kick, with the `call.share_end` audit row, then everyone gets `call.state` with `sharing` false and the SFU stops forwarding it. |
| Server mute and deafen | `call.moderate`, checked the same way with mute_members or deafen_members. Kept per person in memory and dropped when they leave calls altogether; a move or a join elsewhere keeps it. Sent to the SFU as `moderate(call_id, silenced, hushed)` with every new participant. |
| Suppressed and priority | From speak and priority_speaker in the room when they join. A `role.updated`, `role.deleted`, `member.role`, `member.roles`, or `room.updated` event re-reads the permissions of everyone in a call and sends `call.state` to anyone whose `suppressed` changed or who lost priority. |
| Move | `call.move` to the room the person is already in re-reads the session and returns `ok`. It does not check `move_members` and does not issue a ticket. A move to another room checks that the room exists (`not_found`), `move_members` in both rooms (`forbidden`), and rank (`hierarchy`), audits `call.move`, sends `call.moved` to the socket that holds the seat, and lets their next join of that room pass the room's `user_limit` for 30 s, still capped by `TINYSERVER_CALL_ROOM_MAX`. If that seat socket is already in the disconnect grace, `call.moved` is dropped and the ticket still exists. The next successful join clears the ticket. The leave is `moved` only when that join is the ticket's room. A join of some other room is `joined_elsewhere`. |
| Ban and timeout | The dispatcher sees `member.banned` and ends their call with `banned`. Their sockets close on that event with `4003` even when they do not receive the frame. A following `member.removed`, if they were a member, does not emit a second `call.left`. A `member.timeout` that starts a timeout ends their call with `timed_out`. |
| Audit | Joins, leaves, and starting a share are not audited. `call.kick`, `call.share_end`, `call.server_mute`, and `call.move` are. |
| Restart | `Live::close` empties the registry before the sockets close with `1012`. |

Removal, room deletion, and a deleted session all reach the API as outbox events or session checks. Every `call.left`, whatever its reason, sends `leave` for that seat's call id, so leaving, a kick, a removal, a deleted room, and a dropped socket after its grace all stop that person's media. A `Left` from the SFU ends the seat with `media_timeout` or `media_lost`. One user is in at most one call.

### Limits

| Limit | Default | Setting |
| --- | --- | --- |
| People in one room's call | The room's `user_limit` (1 to 99), else 12, and never above 12. One more gets `room_full`. | `TINYSERVER_CALL_ROOM_MAX`, the room's `user_limit` |
| People in calls on the server | 32. The 33rd gets `server_full`. Moving between rooms does not count twice. | `TINYSERVER_CALL_SERVER_MAX` |
| Joins and leaves per socket | 5 at once, then one every 3 s. Over it gets `slow_down`. | |
| Audio from each publisher | Up to 192 kb/s of payload is forwarded, from a bucket of two seconds; frames over it are dropped and counted. The bytes are the RTP payload after DTLS, so a DAVE frame counts at its ciphertext size. Clients encode at the room's `bitrate_kbps`, 16 to 128, so this is an abuse limit: the top room bitrate, a seal on every frame, and Opus running half again over it all pass. | `sfu::Config::audio_kbps` |
| Packets from one source address | 1,000 a second, burst 2,000, before str0m sees them, UDP datagrams and TCP frames alike. At most 4,096 sources tracked; a new one past that is dropped. Slice 6 doubled it from 500 and 1,000: a publisher with camera, microphone, and a 2.5 Mb/s share sends about 550 a second (285 share, 210 camera, 50 audio, and RTCP), more in a keyframe burst, and a test at 500 lost share frames. Unknown sources still get only STUN through. | `sfu::Config::packets_per_second`, `packet_burst` |
| TCP streams | 256 open, 16 from one IP address, 64 unverified. A stream over any of them is closed as soon as it is accepted. | `sfu::Config::tcp_streams`, `tcp_per_source`, `tcp_unverified` |
| TCP backlog | 32 KiB of audio and 32 KiB of everything else waiting on one stream. Past either, a packet is dropped, as UDP would lose it. Audio is written first, at the next frame boundary. | |
| TCP handshake and idle | A stream with no STUN for a participant within 5 s is closed. A stream with no whole frame for 30 s is closed; half-open ones end this way. | `sfu::Config::tcp_handshake`, `tcp_idle` |
| Offers | 32 KiB. Audio m-lines up to the room cap, video up to 3, no other kinds. Over that is `bad_sdp`, checked before the media thread sees it. | `sfu::Config::offer_bytes`, `offer_audio` (set from `TINYSERVER_CALL_ROOM_MAX`), `offer_video` |
| Renegotiation | `call.offer` shares the join and leave rate: 5 at once, then one every 3 s. | |
| Consent | A connected participant the SFU hears nothing from for 30 s leaves with `media_lost`. str0m clients send STUN consent checks every few seconds even in silence. | `sfu::Config::consent_timeout` |
| Camera | Simulcast `h` 1280×720 at about 1.2 Mb/s, `l` 640×360 at about 350 kb/s. 30 fps. What a client should send. | |
| Camera from each publisher | Up to 2,400 kb/s of `h` and 700 kb/s of `l` is forwarded, each from a bucket of one second. A frame over it is dropped, and so is the rest of that layer until a keyframe fits. Those frames spend nothing, so the bucket fills for it. The SFU asks for a keyframe every 1 s while the layer is dropped, against every 300 ms otherwise, and not within 300 ms of one arriving. A layer dropped for 1 s gives way to the other. | `sfu::Config::camera_high_kbps`, `camera_low_kbps` |
| Share | One per room. H.264 constrained baseline, one layer, up to 1920×1080 at 5–15 fps and about 2.5 Mb/s, with a content hint of `detail` or `text` so the encoder keeps text sharp over frame rate. What a client should send. The SDP says level 3.1 (`42e01f`) for all H.264, which tops out at 1280×720; the SFU never reads a stream's own SPS, so a share encoded at level 4.0 for 1080p passes through, and receivers decode it as hardware decoders do. | |
| Share from each publisher | Up to 5,000 kb/s forwarded, from a bucket of one second, so a large keyframe of text fits. Over it, as for a camera layer. | `sfu::Config::share_kbps` |
| Server upload budget | Unset means no budget. A non-positive `TINYSERVER_MEDIA_EGRESS_KBPS` fails startup; it is not "unset". When set, one plan covers every room: 50 kb/s for each audio stream forwarded to a connected listener who is not self-deafened, then 2,500 kb/s for each listener who would watch a share, then 1,200 or 350 kb/s for each listener who would watch only a camera. Shares are kept only when audio plus those shares fit. If they do not, nobody in a room that has a share receives that share, and they do not fall back to a camera. Camera-only listeners, including a sharer, then all get `h` if that fits in what remains, else all get `l`, else no picture. A plan, not a measurement. | `TINYSERVER_MEDIA_EGRESS_KBPS`, `sfu::Config::egress_kbps` |
| Bandwidth estimate | Starts at 1,000 kb/s for each listener. `None` turns TWCC off; layers then follow REMB, `call.quality`, and the budget. | `sfu::Config::bwe_start_kbps` |
| Join without media | `Connected` must arrive within 15 s or the seat is freed with `media_timeout` | `sfu::Config::connect_timeout` |
| End-to-end encryption | Key packages and `invalid`s: 5 at once, then one every 2 s, per socket. MLS payloads up to 32 KiB. A batch of proposals with no commit in 10 s builds the group again. | `Limits::e2ee_rekey_burst`, `e2ee_rekey_every` |

What a host uploads, on those nominal rates. Eight people, cameras on, everyone connected and not self-deafened: each listener has 7 audio streams and one 1,200 kb/s picture, 1.55 Mb/s each, 12.4 Mb/s in all. Audio only is 8 × 7 × 50 kb/s = 2.8 Mb/s. One of them sharing: seven share viewers at 2,500 kb/s and the sharer at 1,200 kb/s, plus the same 2.8 Mb/s of audio, 21.5 Mb/s. Home upload is usually the first limit, which is why the budget exists.

### Restart

Calls live in memory and end on restart. That is the trade for keeping RTP out of Postgres.

| Step | Behaviour |
| --- | --- |
| SIGTERM | The API empties the call registry and sends `shutdown` to the SFU. Every live socket closes with `1012 restart`, and the process exits. No `call.left` is sent. |
| Crash | Sockets drop. Nothing in Postgres needs cleaning. |
| Client | Reconnects `/v1/live` with backoff (0.5 s, 1, 2, 4, cap 10). If it was in a call and the user did not hang up, `mediad` sends a fresh `call.join` to the same room. Target: back in the call within 5 s of the server answering. |
| Durable events | Replayed from `since`. Nothing is lost. |

The SFU task ending does not empty the call registry and does not emit `call.left`. `GET /health` can report media `down` while seats remain. Further joins with an offer get `media_unavailable`. A join without an offer is `bad_frame` while the SFU handle still exists. Sockets stay up. People leave by `call.leave`, by the socket grace, or by a later moderation or membership event.

## End-to-end encryption

Built. DTLS-SRTP protects media between each app and the SFU, and the SFU decrypts it there: transport encryption to a server the community runs. A call is also end-to-end encrypted when every app in it can do it. The apps make one MLS group (RFC 9420) per room's call and encrypt each Opus frame with keys only the group holds; the SFU forwards ciphertext it cannot read. The design is Discord's DAVE protocol, version 1, and tinyclient runs it with the same `davey` crate it uses for Discord calls. tinyserver plays the part Discord's voice gateway plays: MLS delivery service and external sender. It relays and orders the group's messages and proposes who joins and leaves; it never holds a group secret.

### Design

| Piece | Choice |
| --- | --- |
| Group | MLS, ciphersuite `MLS_128_DHKEMP256_AES128GCM_SHA256_P256`, plaintext handshake messages, ratchet tree in the Welcome. One group per room's call, made by the apps. |
| Identities | A member's credential is an MLS basic credential holding 8 bytes, their **MLS id**: the first 8 bytes, big-endian, of SHA-256 over `tiny-e2ee-v1 user ` and their user id. The group id is the same over `tiny-e2ee-v1 room ` and the room id. Each app signs with a long-term P-256 identity key it makes and keeps itself, so the key a person shows in one call is the key they show in the next. |
| External sender | tinyserver's own P-256 key, made at start, kept in memory, never written or logged; credential `tinyserver`. It signs Add and Remove proposals and nothing else. A restart makes a new one; calls end with a restart anyway. |
| Frame encryption | DAVE's: per-sender keys from the MLS exporter, a hash ratchet, AES-128-GCM with an 8-byte tag, a nonce and a `0xFAFA` marker at the end of each frame. RTP headers and the RFC 6464 audio level extension stay in the clear, so the SFU still forwards each frame and still finds who speaks. Opus silence frames are sent as they are. |
| Rekey | Every join and every leave is a commit and a new epoch. Who left cannot read what is said after; who joined cannot read what was said before. |
| Call code | DAVE's voice privacy code: 30 digits from the epoch authenticator. Everyone in the call sees the same one; it changes with every epoch. A server that put itself in the middle would show the two halves different codes. |
| Safety numbers | DAVE's pairwise verification code: 45 digits from both members' identity keys and MLS ids. tinyclient lets a person mark someone verified and warns loudly when a verified person's key changes. |
| Who decides | The room's call is end-to-end encrypted only if every app with media in it joined with `e2ee`. One that cannot turns it to transport only for everyone, and everyone is told who. `e2ee_required` on a room refuses apps that cannot. |

### Frames

`call.join` takes `e2ee`, the highest version the app speaks (`1`); absent or `0` is none. Participant objects in `ready`, `call.joined` and `call.state` carry `e2ee`, whether that person joined with it. Every other frame is `call.e2ee`, sent to and from the socket that holds the person's seat only, in order. All payloads are standard base64 of TLS-serialised MLS structures, at most 32 KiB decoded.

Server to client, each `{"type": "call.e2ee", "room_id", "op", ...}`:

| `op` | Fields | Meaning |
| --- | --- | --- |
| `start` | `version` (1), `external_sender` | Drop any group state, make a fresh session with this external sender, and answer `key_package`. A joiner with `e2ee` in a room that is encrypted gets it right after `call.answer`; everyone gets it when the room turns encrypted again, and when the server builds the group again. |
| `off` | `user_ids` | Transport only now: drop any group state. `user_ids` are the people in the call who cannot encrypt. A joiner with `e2ee` in a room that is transport only gets it right after `call.answer`; everyone with `e2ee` gets it when someone who cannot joins, and again with the new list when the list changes. |
| `proposals` | `epoch`, `data` | DAVE's opcode 27 payload: one byte, `0` for append, then a TLS vector of external proposal messages, removes first, then adds. Commit to them and send `commit`. Sent to every member that stays. |
| `commit` | `epoch`, `data` | The commit that won for that batch, from `epoch` to `epoch + 1`. Sent to every member that stays, the committer included. |
| `welcome` | `epoch`, `data` | You were added; the group is at `epoch` now. Sent to each person the commit added. |

A joiner with `e2ee` always gets exactly one of `start` and `off` after its `call.answer`; it may come before or after the `call.joined` frames. An app without `e2ee` never gets `call.e2ee`.

Client to server, each `{"type": "call.e2ee", "op", ...}`, with an optional `ref`. Nothing answers success; the frames that follow are the answer. A failure is `error {ref, code}`.

| `op` | Fields | Meaning |
| --- | --- | --- |
| `key_package` | `data` | After `start`, or to start again. It must validate, be for the ciphersuite above, and hold the sender's own MLS id: otherwise `bad_e2ee`. A member that sends one has started again: its old leaf is removed and it is added anew. |
| `commit` | `epoch`, `data`, `welcome` | The answer to `proposals`. Taken from a member the batch went to, for that batch's epoch, if it is a commit in the clear for this room's group at that epoch from the sender's own leaf, and carries a Welcome when the batch adds anyone: otherwise `bad_e2ee`. The first one wins. Later ones for that epoch, and any for another epoch, are dropped without a word. |
| `invalid` | none | The app could not apply a commit or a welcome. Its leaf is removed, and it sends a fresh `key_package`. |

Error codes: `e2ee_required` (a `call.join` without `e2ee` in a room that requires it, while media runs; no seat is taken); `bad_e2ee` (above); `not_found` (`call.e2ee` from a socket that does not hold a seat with media, or while the call is transport only); `bad_frame` (no `op`, an unknown `op`, no `data`, a `commit` without `epoch`, or over 32 KiB); `slow_down` (key packages and `invalid`s: 5 at once, then one every 2 s, per socket).

```json
{"type":"call.join","ref":"7","room_id":"0b6f…","sdp":"v=0…","e2ee":1}
{"type":"call.answer","ref":"7","call_id":"c-3f2a…","sdp":"v=0…"}
{"type":"call.e2ee","room_id":"0b6f…","op":"start","version":1,"external_sender":"AEEE…"}
{"type":"call.e2ee","op":"key_package","data":"AAEAAkBB…"}
{"type":"call.e2ee","room_id":"0b6f…","op":"proposals","epoch":0,"data":"AP0B…"}
{"type":"call.e2ee","op":"commit","epoch":0,"data":"AAEAAQgr…","welcome":"AAJAQQSq…"}
{"type":"call.e2ee","room_id":"0b6f…","op":"commit","epoch":0,"data":"AAEAAQgr…"}
```

### The delivery service

[`crates/api/src/live/e2ee.rs`](../crates/api/src/live/e2ee.rs), under the same lock as the call registry, so its frames go out in the order the registry changed. Memory only; it is dropped when the room's call empties.

| Rule | Behaviour |
| --- | --- |
| Mode | Encrypted while everyone with media in the call joined with `e2ee`, else transport only. Each change sends `start` or `off` to everyone with `e2ee` and drops the group. |
| The tree | The server keeps who sits in which leaf. Removes blank a leaf; adds take the leftmost blank leaf in the order of the batch, else a new one at the end; blank leaves at the end are trimmed. That is how openmls applies a commit, and the tests check it against real groups. |
| The first group | With no group yet, the first person in join order with a key package holds it: the pending group of one their app made at epoch 0. As soon as anyone else's key package waits, the proposals to add them go to that person only, and its commit makes the group, at epoch 1. If that person leaves first, the next one holds it. |
| Batches | With a group, whenever no batch is out and there is work, one batch goes to every member that stays: Remove proposals for leaves of people who left or sent `invalid`, then Add proposals for every waiting key package. The first valid commit wins; members get `commit`, the people added get `welcome`, and the next batch goes out if more work came meanwhile. |
| Leaving | Any way out of the call (leave, kick, move, ban, timeout, `media_lost`, a deleted room, a rejoin with a new offer) removes the person: a waiting key package is dropped, a leaf is removed with the next batch, and someone being added when they left is removed right after. When nobody who holds the group is left, it is gone, and the people waiting make a new one. |
| Stuck | A batch with no winning commit after 10 s builds the group again: `start` to everyone. Calls never wait on it; the apps hold their microphone back until they have keys again. |

### What it protects, and what it does not

| The server, or someone who controls it, | |
| --- | --- |
| cannot | Hear or decode audio in an encrypted call. It forwards ciphertext; it has no group secret, and its own key signs proposals only. |
| cannot | Join a call unseen. Anyone it adds is a member every app lists, with an identity key and a safety number. |
| cannot | Impersonate someone a person verified without the warning that their key changed. |
| can | See everything around the audio: who is in which call and when, who speaks and how loudly (the RFC 6464 level is in the clear, so the SFU can pick the speaker), packet sizes and timing (Opus is variable rate, so these say something about the speech), and every IP address. |
| can | Add a ghost member, or give someone a key that is not theirs, while nobody has verified that person: the external sender proposes members, as Discord's gateway does. The member list and the safety numbers are how a person finds out; the call code catches a server in the middle of two halves of a call. |
| can | Turn a call to transport only by claiming someone cannot encrypt, or ignore `e2ee_required`. tinyclient says so loudly and names who; it never shows a lock it does not have. |
| can | Stop calls, drop frames, and refuse or delay any of the above. Availability is not protected. |

Also: a member of the group can do anything the group can, including disrupt it with bad commits and make frames that look like another member's, since every member derives every sender's key, as in DAVE. Within one epoch the hash ratchet gives each frame generation its own key, but a member who holds the epoch's secret can derive them all; forward secrecy is per epoch. MLS ids are 64-bit hashes of user ids: two people whose ids collided would share one, which the server could only arrange by brute force on the order of 2^64 for one chosen person, and an app refuses a proposal to add anyone the call's roster does not name. tinyclient seals Opus and the H.264 picture in an end-to-end call. The picture's one-byte NAL header stays clear. This server seals neither and does not read NAL headers. Nothing of any of this is written: no key, no payload, no group state reaches Postgres or a log line the server writes. At `trace` the WebSocket library itself prints whole frames; MLS handshake messages are public by design and hold no secret, but run the server at `debug` or above.

### Compatibility

| Pair | Result |
| --- | --- |
| New app, new server, everyone new | End-to-end encrypted. |
| New app, new server, someone older in the call | Transport only, for everyone, and each new app names who cannot. Once they leave, the call is encrypted again. In a room with `e2ee_required` the older app is refused with `e2ee_required`. |
| Older app, new server | Unchanged. It never sends `e2ee` and never gets `call.e2ee`; participant objects and rooms have one more field it ignores. |
| New app, older server | `e2ee_calls` is missing from `GET /v1/capabilities`, so the app does not offer it; an older server ignores the field anyway. Transport only, shown as such. |

## Network exposure

### The problem

The tunnel is the only way in today, and the host publishes nothing. That hides the host's address and needs no port forwarding. WebRTC media needs a UDP path to the SFU, and the tunnel cannot carry one for a public client.

### Options

| Option | Port on the host | Hides the host address | Works behind CGNAT | Extra cost | Work | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| A. Publish one UDP port, ICE-TCP on the same number | 1 UDP, 1 TCP | No | No | None | Small | **Default** |
| B. SFU allocates on Cloudflare Realtime TURN, advertises only the relay | None | Yes | Yes | $0.05/GB over 1,000 GB a month | Medium: a TURN client in `crates/sfu` | **Opt-in, slice 7** |
| C. Self-hosted coturn | Still needs published ports | No | No | None | Another service | Rejected. It only adds TURN/TLS on 443, which ICE-TCP mostly covers. |
| D. Cloudflare Realtime SFU instead of str0m | None | Yes | Yes | Per GB | Rewrite of the media plane | Rejected. Media leaves the community's server. |
| E. cloudflared private network routing (WARP) | None | Yes | Yes | None | Every member runs WARP | Rejected for members |
| F. Spectrum UDP | None | Yes | Yes | Enterprise plan plus add-on | Small | Rejected for cost |

### A: the default deploy change

The server joins a third network that Docker can publish from. HTTP on 8080 stays unpublished and behind the tunnel. As built.

```diff
   tinyserver:
     environment:
       DATABASE_URL: postgres://app_runtime:${APP_RUNTIME_PASSWORD}@postgres:5432/tinyserver
       BIND: 0.0.0.0:8080
+      TINYSERVER_MEDIA_PORT: ${TINYSERVER_MEDIA_PORT:-50000}
+      TINYSERVER_PUBLIC_IP: ${TINYSERVER_PUBLIC_IP:-}
+      TINYSERVER_LAN_IP: ${TINYSERVER_LAN_IP:-}
+    ports:
+      - "${TINYSERVER_MEDIA_PORT:-50000}:${TINYSERVER_MEDIA_PORT:-50000}/udp"
+      - "${TINYSERVER_MEDIA_PORT:-50000}:${TINYSERVER_MEDIA_PORT:-50000}/tcp"
     networks:
       - inside
+      - media
 ...
 networks:
   inside:
     internal: true
   edge:
+  media:
```

| Detail | Rule |
| --- | --- |
| Same port inside and out | The SFU binds `0.0.0.0` on `TINYSERVER_MEDIA_PORT`, UDP and TCP, and advertises that port. Keep host and container numbers equal. |
| Public address | `TINYSERVER_PUBLIC_IP` must be IPv4. The server does not guess it, because guessing needs an outbound STUN call and a wrong guess fails silently. Unset, the server runs without media and `/health` says `"media": "off"`; the design had compose require it, but a host without a port should still start. A value that is not an address, or is not IPv4, stops the process (`TINYSERVER_PUBLIC_IP must be IPv4 for now`). The same IPv4 rule applies to `TINYSERVER_LAN_IP`. |
| Health | `GET /health` has `media`: `off`, `up`, or `down`, and `transports`, what members can reach while media is up: `udp`, `tcp`, and `lan` when a LAN address is set. Empty when media is off or down. `down` makes `ok` false and the status 503. |
| Same-LAN members | Many home routers do not hairpin. `TINYSERVER_LAN_IP`, IPv4, adds a UDP and a TCP host candidate on that address for members on the same network. Equal to the public address, it adds nothing. |
| Router | Forward the port, UDP and TCP, to the host. `deploy/README.md` gives the steps. |
| Docker | Default iptables DNAT keeps the client's source address, which ICE needs. |
| IPv6 | Not in the first cut. `TINYSERVER_PUBLIC_IP6` later. |
| Restrictive client networks | ICE-TCP covers networks that block UDP. Networks that allow only 443 are not covered yet: the SFU advertises the port it binds, and binding 443 needs a capability the container drops. An advertised port separate from the bound one would fix it, later. |

### Security of an open media port

| Concern | Answer |
| --- | --- |
| The host address becomes visible to every member, in the SDP answer | True. The tunnel hid it before. `deploy/README.md` says so, and relay mode (B) is the answer for owners who care. |
| Strangers sending packets | Unknown sources get STUN handling only. Without the per-join ICE password, which only travels over the authenticated socket, nothing else is processed. |
| Parser exposure | str0m parses STUN, DTLS, and SRTP from the internet. Pin the version, follow its fixes (0.23.1 hardened SRTP and RTCP parsing), and fuzz. `fuzz/` has cargo-fuzz targets for live-socket frames (client messages and outbox payloads), SDP offers through the caps, the share check, and str0m's parser into a participant (seeds include a simulcast camera and a share), ICE-TCP framing, packets into a participant with audio, camera, share, and video slot m-lines and an answer out, and the video routing arithmetic: RTP time rewriting across switches, the floor, layer choice, and the upload plan with shares. Live-socket seeds include `call.share_end`. CI builds them on nightly and runs each for 60 s, in a job apart from `check`. |
| Media privacy | DTLS-SRTP ends at the SFU. The SFU then sees the RTP payload. In a transport-only call that Opus payload is plaintext. In an end-to-end encrypted call it is DAVE ciphertext; the SFU forwards it and cannot read it. RTP headers and the RFC 6464 audio level stay in the clear. tinyclient seals the H.264 picture as well, and the one-byte NAL header stays clear. This server does not seal video and does not read NAL headers. Cloudflare sees signaling, and so the DTLS fingerprints, but not media in mode A. |
| Egress | Joining `media` gives the server a route out, which it did not have. In mode A the server makes no outbound connections. An owner can add a `DOCKER-USER` rule allowing only replies. |
| Floods | Per-source packet rate limit before str0m, and strangers' non-STUN dropped before any `Rtc` is asked. Caps on rooms and the server. TCP stream caps above. A test floods the port from 8 unknown sources for 2 s while a call runs, and the call keeps forwarding. Measured once by hand on an Apple M-series laptop, a release build: idle, the media thread uses no measurable CPU; 174,000 packets a second of STUN and RTP from 8 strangers on loopback use about 72% of one core, nearly all of it receiving and dropping at the per-source budget. A flood at line rate can fill the one media thread; the kernel and the router are the next limits. |

### B: relay mode on Cloudflare Realtime TURN

For a host that cannot or will not open a port. Only outbound traffic leaves the host.

1. The owner creates a Realtime TURN key in the Cloudflare account that already runs the tunnel, and sets `TINYSERVER_TURN_KEY_ID` and `TINYSERVER_TURN_API_TOKEN`.
2. On each `call.join`, tinyserver calls `generate-ice-servers` with a `ttl` of 4 h, and revokes the credential when that participant leaves.
3. The SFU opens one allocation per participant over UDP 3478, falling back to TLS on 443. One per participant keeps each allocation well under the 5–10 kpps limit.
4. The SFU advertises only the relay address. The client sends its server-reflexive candidate (one STUN request to `stun.cloudflare.com`) in a `call.candidate` message, and the SFU creates a permission for that address.
5. Allocation refresh every 5 min, permission refresh every 4 min, channel binding for the data path.

str0m has no TURN client. The slice adds a small sans-IO one in `crates/sfu` (allocate, refresh, permission, channel bind) or adopts a crate after review. TURN credentials are never shown to clients in this design; only the server allocates.

Cost, budgeted as if every byte both ways is billed: a 6-person camera call is about 8 GB an hour; audio only is under 1 GB an hour. The free 1,000 GB covers about 125 camera-call hours a month.

## Client impact (tinyclient)

### Reuse

| Piece | Where today | Use for Tiny calls |
| --- | --- | --- |
| `mediad` process, stdin/stdout frames, one call per process | `crates/mediad`, `docs/architecture.md` | Unchanged |
| chatd call lifecycle: deadlines, `Joined` only on `Connected`, failure always sends `CallLeave` | `chatd` `media.rs` | Unchanged |
| `Call`, `CallState`, `Participant`, meters, devices, cues | `crates/protocol/src/call.rs` | Unchanged |
| cpal capture and playback, device hot-swap, resample to 48 kHz | `mediad` `audio.rs` | Unchanged |
| Opus encode and decode, FEC, PLC | `mediad` `opus.rs` | Switch to mono 32 kb/s VOIP. DTX stays off (`usedtx=0`); the speech gate ends the spurt |
| Jitter buffer | `mediad` `jitter.rs` | Tiny inserts by RTP timestamp, because the SFU rewrites sequence numbers. A 400 ms gap does not end the spurt |
| 20 ms loop, one UDP socket, no async runtime | `mediad` `media.rs` | Same loop, with str0m driving it |
| `tungstenite` | `mediad` | Reused in `chatd` for `/v1/live` |

### Add

| Piece | Notes |
| --- | --- |
| `chatd`: a live socket thread in the Tiny adapter | `tungstenite` over the existing TLS stack. Ping, resume with `since`, map events to `Change::Occupants` and `Change::Presence`. Set `call: Native` on Tiny rooms. |
| `chatd`: rooms in the Tiny slice | The adapter has no rooms today (`crates/protocol/src/tiny.rs` says so). Rooms become call targets. |
| `protocol`: a Tiny join | New optional variant or fields, because `VoiceJoin` is Discord-shaped and the protocol only takes optional additions. It carries the SDP offer out from `mediad` and the answer and `call.slots` back in, relayed by `chatd`. `mediad` still never talks to the network for signaling. |
| `mediad`: str0m transport | Replaces `gateway.rs`, `session.rs`, and `rtp.rs` for Tiny. Discord keeps its own path. Use str0m's `apple-crypto` on macOS and `rust-crypto` elsewhere, to avoid OpenSSL and keep to one TLS stack. Follow the headless client below. |
| `mediad`: echo cancellation and noise suppression | Missing today, and needed for open speakers on any call. OS voice processing (macOS VoiceProcessingIO, Windows communications mode) or `webrtc-audio-processing` bindings. |
| `mediad`: RFC 6464 audio level on send | The SFU picks the active speaker from it |
| Video | Camera capture; H.264 constrained baseline encode in two sizes, 1280×720 at 1.2 Mb/s for `h` and 640×360 at 350 kb/s for `l`, with packetization mode 1, a keyframe on every PLI for that rid (that is what this server sends; SDP also advertises FIR), and a periodic one as a fallback; decode of the one video slot, reset when `call.slots` `video` changes; one render surface; `call.quality` from the window size, `low` for a small tile and `off` when hidden; and the camera flip in `call.offer`. The SFU does not read resolution, SPS, or a content hint. |
| Share | Screen or window capture into the one hardware H.264 encode the product doc plans: constrained baseline, one layer, up to 1920×1080 at 5–15 fps and about 2.5 Mb/s, content hint `detail` or `text`, a keyframe on every PLI for the share mid. The share m-line in the offer, `inactive` until a share starts, and its flip in `call.offer`. On `share_taken`, set it back to `inactive` locally. On a `call.state` for yourself with `sharing` false while you share, an owner or admin ended it: stop capture and set the m-line `inactive` with `call.offer`. Render `call.slots` `video` `kind` `share` for text, not a face: fit it, do not crop it. An owner or admin control that sends `call.share_end`. |

### The reference client

[`crates/sfu/src/client.rs`](../crates/sfu/src/client.rs), behind the `headless` feature, is what `mediad` should match. It is sync, one UDP socket, no async runtime.

| Step | What it does |
| --- | --- |
| Config | `release_audio_promptly(RtcConfig::new().clear_codecs().enable_opus(true, false))`, the same pinned str0m and the same audio reorder as the SFU (size 0, 40 ms). Not ICE-lite: the client is the full ICE agent and sends the checks. |
| Candidate | One host candidate for its own bound socket. No trickle, no STUN server. `Client::tcp` offers one active TCP candidate on port 9 instead (RFC 6544), and no UDP. |
| Offer | `sdp_api()`: one `add_media(Audio, SendOnly)` for the microphone, then `room cap - 1` `add_media(Audio, RecvOnly)`, then `add_media(Video, Inactive, …, Some(simulcast))` for the camera with send layers `h` and `l`, then `add_media(Video, Inactive)` for the share, then `add_media(Video, RecvOnly)` for the video slot. H.264 is `codec_config().add_h264(108, Some(109), true, 0x42e01f)`. `apply()` gives the offer and the pending state. The offer is `call.join` `sdp`. |
| Answer | `call.answer` `sdp` into `SdpAnswer::from_sdp_string`, then `accept_answer(pending, answer)`. |
| Loop | Drain `poll_output` to its timeout; send each `Transmit` by its protocol, and over TCP open a stream to that address on first use and frame each packet with a 2-byte length; on `Event::Connected` start the microphone; every 20 ms write one Opus frame with `writer(mic).audio_level(level, voice).write(pt, now, MediaTime::new(n * 960, 48 kHz), frame)`; then wait on the socket, through `mio`, until the next str0m timeout or frame, and feed what arrives as `Input::Receive` with the local candidate's address as destination, one packet at a time with a drain after each, or `Input::Timeout`. |
| Receive | `Event::MediaData` on a slot mid. `call.slots` says whose voice that mid carries. Reset that slot's jitter buffer and decoder when its `user_id` changes. The SFU keeps one SSRC and sequence run per slot and forwards the new speaker's own RTP time; it does not rewrite audio time. |
| Camera | `sdp_api().set_direction(camera, SendOnly)`, `apply()`, and send the offer as `call.offer`; accept its `call.answer`. `Inactive` stops it. Frames start once the answer is accepted. While the m-line sends, every 1/30 s write one frame per layer with the same RTP time: `writer(camera).rid("h").write(pt, now, MediaTime::new(n * 3000, 90 kHz), annex_b_frame)`, then `l`. On `Event::KeyframeRequest` for the camera mid, make that rid's next frame a keyframe. This SFU's keyframe request is PLI; the headless client treats any `KeyframeRequest` on that mid as a restart at the IDR. The test frames come from `crates/sfu/fixtures`: `camera-h.h264` is 320×180 and `camera-l.h264` is 160×90, both constrained baseline level 3.1, padded to 5,000 and 1,458 bytes a frame (1.2 Mb/s and 350 kb/s at 30 fps), with an SEI tag naming the sender and layer. The SFU does not read resolution. |
| Share | A second video m-line, after the camera: `add_media(Video, Inactive)`, no simulcast. `set_direction(share, SendOnly)` and `call.offer` start it, and frames start once the answer is accepted: every 1/15 s, `writer(share).write(pt, now, MediaTime::new(n * 6000, 90 kHz), annex_b_frame)`, no rid. On `Event::KeyframeRequest` for the share mid, make the next frame a keyframe. On `share_taken`, `Running::reject` flips the m-line back and drops the pending offer. The test share replays the `h` clip at 15 fps, tagged `s`, padded to 2.5 Mb/s; `Running::share_kbps` lowers it. That clip is 320×180, not a 1080p encode. |
| Video receive | `Event::MediaData` on the video slot mid is one Annex B frame; `is_keyframe()` says whether it starts a picture. `call.slots` `video` says whose camera or share it is. It becomes `null` as soon as that source stops or the slot is planned empty, and it names a new source only when the SFU writes that source's first keyframe, so reset the decoder then. Before the first keyframe on a slot, drop frames. A frame str0m marks not `contiguous` was lost: drop frames until a keyframe, and ask for one with `direct_api().stream_rx_by_mid(video_slot, None).request_keyframe(Pli)` every 300 ms. |
| REMB | A receiver that knows its link is slow can cap what it gets: `direct_api().stream_rx_by_mid(video_slot, None).request_remb(Bitrate::kbps(n))` every second. The tests use it to hold a listener on `l`. |
| Mute | Send `call.state` and stop writing frames. The SFU drops a muted publisher's frames anyway. |
| Leave | Send `call.leave`. A client that just stops is dropped after 30 s with `media_lost`. |

`mediad` swaps the synthetic frames for its libopus encoder (mono, 32 kb/s, VOIP, FEC, DTX off, `usedtx=0`; the speech gate ends the spurt) and the real RFC 6464 level, and feeds received frames into its existing jitter buffer. For video it swaps the fixture for its camera and two H.264 encoders, and adds a decoder and one render surface.

### Change

| Item | Change |
| --- | --- |
| Tiny adapter header comment | No longer "no calls, or media". |
| mediad size | str0m and its crypto add a few MB, well under the "tens of MB" a full WebRTC stack would. Not measured yet; measure when `mediad` adopts it. |
| Reconnect | A Tiny call survives a server restart by rejoining. `Reconnecting` already exists in `CallState`. |

## Open questions for the owner

| Question | Recommended default |
| --- | --- |
| Text messages in voice rooms? | No. Voice rooms have no messages. Text channels and direct messages already ride this socket. |
| Ringing and 1:1 calls? | No. Drop-in rooms only. A two-person room covers a 1:1. |
| Screen share? | Yes, built in slice 6. One H.264 share per room, one layer. Other listeners see it instead of a camera. The sharer keeps a camera, and nobody sees their own picture. |
| Recording? | None. The SFU never writes media to disk. Revisit only with a per-call consent prompt. |
| E2EE? | DAVE version 1. The server is the MLS delivery service and external sender and seals no frames. tinyclient seals Opus and the H.264 picture when the call is end to end. The picture's one-byte NAL header stays clear. The server does not read NAL headers and does not implement a video sealer. See [End-to-end encryption](#end-to-end-encryption). |
| Per-room permissions | Built as bits and per-room overrides. @everyone starts with view, connect, speak, video, share_screen, and voice_activity. On a call the socket enforces view (hidden room is `not_found`), connect (`forbidden`), speak (join suppressed, audio not forwarded), and priority_speaker (`call.state` `priority` is stored only when the bit is set). video, share_screen, and voice_activity are stored and can be overridden, and this socket does not refuse a camera, a share, or voice-activity audio for lack of them. Mute, deafen, move, and disconnect are enforced on the moderation frames in the call registry. See [`server.md`](server.md). |
| Room size? | Default 12 in one room's call and 32 in calls on the server (`TINYSERVER_CALL_ROOM_MAX`, `TINYSERVER_CALL_SERVER_MAX`). A room `user_limit` from 1 to 99 replaces the 12, and is still capped by the server room max. 0 means the server room max. Past the room cap is `room_full`. Past the server cap is `server_full`. |
| Port or relay? | Publish one port (A). Offer relay mode (B) for CGNAT and hidden-origin hosts. |
| Show who is in a call to members not in it? | Yes, as in Discord. It is what makes drop-in rooms work. |
| Audit call joins? | No. Presence is not an admin action. Audit only moderation: removed from call, share ended, server mute or deafen, and moves. |
| Guests without an account? | No. Invites already make accounts. |

## Slices

Each slice is one PR, merges on its own, and leaves the server working.

| # | Slice | Scope | Acceptance check | Size |
| --- | --- | --- | --- | --- |
| 1 | **Live socket and presence** (built) | `GET /v1/live` with bearer auth, ping, limits, `ready`, `presence`. Outbox writes in every room, member, and invite change. Dispatcher on `LISTEN app_wake`, replay with `since`, `resync`, 24 h sweep. No media. | Integration test: two sessions connect; A renames a room; B gets `room.renamed` with a `seq`. B reconnects with an old `since` and gets the missed events. A removed member's socket closes with `4003`. A rolled-back write emits nothing. | M, ~800 lines |
| 2 | **Call occupancy without media** (built) | `call.join`, `call.leave`, and `call.kick`, the call registry, authorization, caps, a join and leave rate, `joined_elsewhere`, `call.joined` and `call.left` to every member, `ready.calls`. Slice 2 shipped occupancy without media. On this commit a join does not ignore `sdp`. With no SFU configured, a join without `sdp` takes a seat and a join with `sdp` is `media_unavailable`. With an SFU configured, `sdp` is required (`bad_frame` without it). Caps are `room_full` and `server_full`. `full` is not sent. `call.state` moves to slice 3, with the media it describes. | Integration tests: a join reaches everyone and later `ready` frames; joining elsewhere leaves the first room; a second socket is one seat; a dropped socket leaves after the grace; caps return `room_full` and `server_full`; a deleted room empties; a removed member leaves; kick works for an admin, fails against an owner and for a member, and is audited. | S, ~400 lines |
| 3 | **SFU audio, UDP** (built) | `crates/sfu` thread with mio, str0m 0.24.1 pinned, ICE-lite, DTLS-SRTP, Opus forwarding, slots, active speaker and `call.speaking`, `call.offer`, `call.state` with mute and deafen, connect and consent timeouts, per-source packet and per-publisher bitrate caps, graceful shutdown. Compose and `deploy/README.md` gain the UDP media port. `/health` reports `media`. | A headless str0m client (`crates/sfu/src/client.rs`) joins with two peers on loopback; each receives the other's Opus frames within 2 s, and a client in another room receives nothing; leaving, a kick, and mute stop forwarding; killing one peer frees its slot after the consent timeout; a peer that never connects leaves with `media_timeout`. The SFU tests need no database; the same cases run through the live socket in `crates/api/tests/media.rs`. Manual, not yet done: two machines on different networks talk. | L, ~1,500 lines |
| 4 | **ICE-TCP and hardening** (built) | TCP passive candidate on the same port, RFC 4571 framing, stream caps, handshake and idle timeouts, the per-source rate limit on TCP too, offer size and m-line caps, `call.offer` on the join rate, cargo-fuzz targets and a CI job, `TINYSERVER_LAN_IP`, `transports` in `/health`. Compose publishes the TCP port. | A headless client with only a TCP candidate hears and is heard, alone and through the live socket. The answer has the LAN candidates when set. Streams over each cap are closed; silent, non-STUN, oversized, and idle streams are closed. Oversized and crowded offers get `bad_sdp`; rapid `call.offer` gets `slow_down`. CI runs each fuzz target for 60 s. A 2 s flood from unknown sources does not stop a call, and a hand-measured flood of 174,000 packets a second costs about 72% of one core. Not done: a CPU bound enforced in a test, and two machines on different networks over TCP. | M, ~1,400 lines |
| 5 | **Camera video** (built) | H.264 constrained baseline simulcast receive, per-listener layer choice from TWCC and REMB estimates and `call.quality`, a speaker-switched video slot with a floor, PLI, and RTP time rewriting, per-layer publisher caps, `TINYSERVER_MEDIA_EGRESS_KBPS`, `camera` in `call.state` and `ready`, `call.slots` `video`, `call.quality`. The headless client sends two layers of a canned H.264 clip and receives video. | The test client publishes two layers; a listener whose REMB says 500 kb/s gets `l` while another gets `h`; when the speaker changes, the slot switches within 2 s and the first frame from the new speaker is a keyframe the SFU asked for; over-budget video is dropped and a dropped layer gives way to the other; the upload budget drops listeners to `l` and then to audio only; camera off and leaving clear the slot; TCP-only clients send and receive video; the same through the live socket. With TWCC on, a listener starts on `l` and a REMB of 500 kb/s keeps another there, in a test binary of its own because the estimate drops on a busy machine; the other tests turn TWCC off and steer with REMB. The estimate climbing from 1,000 kb/s to `h` is an ignored test, run by hand: about 250 ms on loopback on an idle laptop, and not at all with every core busy. | L, ~2,200 lines |
| 6 | **Screen share** (built) | The share m-line, one share per room held by the first to start it, `share_taken` checked before str0m takes the offer, the share in every listener's video slot but the sharer's, `call.share_end` for owners and admins with an audit row, `sharing` in `call.state` and `ready`, shares first in the upload budget, a per-publisher share cap, and the per-source packet budget raised to 1,000 a second. The headless client shares the canned clip, starts media only after an answer, and asks for a keyframe after a loss. | A shares and B and C get `kind` `share` frames from a keyframe on; a second sharer gets `share_taken`; stopping the share brings the speaker's camera back on a keyframe; the sharer leaving ends it; an admin ends a member's share, audited, and gets `forbidden` for an owner's; a member gets `forbidden`; at 2.5 Mb/s with camera and microphone the share keeps its frame rate under the default packet budget, and at the old 500 it did not; the upload budget keeps a share and drops a camera; TCP-only clients send and receive a share; the same through the live socket. The full-rate test has a test binary of its own; the rest share at 300 kb/s, so many at once stay light. | S, ~1,500 lines |
| 7 | **Relay mode** | Cloudflare Realtime TURN credentials, sans-IO TURN client in `crates/sfu`, relay-only candidates, `call.candidate`, revoke on leave. Compose variant without `ports:`. | With no published ports and only outbound traffic allowed, two clients on different networks talk. The SDP answer contains no host address. Credentials are revoked when a participant leaves. | L, ~1,200 lines |

The tinyclient side runs alongside: live socket and rooms after slice 1, occupancy after 2, audio calls with str0m in `mediad` after 3, video after 5, screen share after 6. Slice 3 is the point where the two repos meet; its headless str0m test client becomes the reference for `mediad`.
