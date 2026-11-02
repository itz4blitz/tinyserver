# Network exposure and IP addresses

Where a person's network address goes, who learns it, and what no software change can hide. Sources: `deploy/compose.yml`, `docs/calls.md` ("Network exposure"), `crates/api/src/limit.rs`, `crates/server/src/main.rs`, `crates/sfu/src/engine.rs`, and tinyclient's `crates/mediad/src/tiny.rs` (read only, `26916e9`). Status words are in [`README.md`](README.md). The category numbers refer to the five-way split in [`guarantees.md`](guarantees.md).

## 1. Two paths

| Path | Used for | Through Cloudflare | Who sees the client address |
|---|---|---|---|
| HTTPS and WebSocket to the chat and auth hostnames | Everything except call media: REST, the live socket, call signaling, sign-in pages | Yes. Cloudflare terminates TLS | Cloudflare, always. Pocket ID, for the auth hostname (it reads the forwarded address). tinyserver sees only cloudflared's address, plus the `cf-connecting-ip` header (section 3) |
| UDP and TCP to the media port (default 50000) | Call audio and video | **No.** Published on the host, direct | The host machine and its network, and the SFU process (category 4 and category 2) |

The compose file publishes one port: `TINYSERVER_MEDIA_PORT`, UDP and TCP, the same number inside and out. HTTP on 8080 and Pocket ID's 1411 are not published. cloudflared connects outward, so the host needs no inbound HTTP port.

## 2. Address handling by component

| Component | What it does with a client address | Stored | Test |
|---|---|---|---|
| Cloudflare edge | Sees it, as any proxy does | Cloudflare's policy. Not reviewed | n/a |
| cloudflared | Passes it as a header | Container log, unreviewed | |
| tinyserver HTTP | The TCP peer is cloudflared (a private address on the `inside` network). Only `/v1/auth/start`, `/v1/auth/token` and `/v1/auth/join` use an address: the rate limiter | Not stored. The limiter keeps a truncated SHA-256 of the address (IPv6 by /64) under a per-process random secret, with hit times, for 60 seconds, in memory | Nine unit tests in `limit.rs`; `a_sixth_try_in_a_minute_is_told_to_slow_down` |
| tinyserver auth | No address is written to `auth_pending`, `auth_codes`, `sessions` or `users` | No column exists for one | Schema search, [`metadata.md`](metadata.md) section 1 |
| Live socket (WebSocket) | No address use at all: per-user (3) and total (1024) socket caps, per-socket frame rate | No | `a_fourth_socket_closes_the_oldest`, `the_total_cap_refuses_before_the_upgrade` |
| SFU, UDP | Looks up the source address in a map to route packets to a participant; per-source packet budget (1,000 a second, burst 2,000, at most 4,096 sources tracked); learns a participant's address from its first STUN check | In memory for the life of the participant | `crates/sfu/tests/*` (routing, caps); no test asserts the address is never forwarded |
| SFU, TCP | Per-stream accounting: 256 open, 16 per source IP, 64 unverified; a stream is bound to the participant whose STUN it carried first | In memory | `crates/sfu/tests/tcp.rs` |
| Logging | `peer` is dropped from stderr always; the ring keeps it only when `TINYSERVER_LOG_PEER_IP=on` and the SFU emits it at debug | Off by default | `peer_is_operator_blind_and_ring_gated` |
| Pocket ID | Records the client IP in its audit log (90 days by default) | Yes, in `pocketid_data` | Not verified for `v2.19.1`; [`metadata.md`](metadata.md) section 12 |
| Postgres | Never receives an address from the application | | |
| clamd | Never receives one | | |

## 3. Cloudflare headers and trust

tinyserver reads one header, `cf-connecting-ip` unless `TINYSERVER_CLIENT_IP_HEADER` names another (`x-real-ip`, `x-forwarded-for`: a list is read from its right), only to key the sign-in limiter, and only when the TCP peer is trusted. By default a peer is trusted if it is loopback, private, link-local, or in the carrier-grade range 100.64.0.0/10 (Tailscale uses it). `TINYSERVER_TRUSTED_PROXIES` replaces that with an explicit list of addresses or CIDR ranges, or `none`. A public peer is never believed (`limit.rs`, `trusted`, `is_local`). Findings S7 describes the previous behaviour, where any caller could rotate the header.

tinyserver ignores every other header, `X-Forwarded-For` (unless it is the one named), `CF-Ray`, `CF-IPCountry` and the rest. Pocket ID is given `TRUST_PROXY=<cloudflared's fixed address>` so it reads `X-Forwarded-For` from that one peer only.

If the tunnel is replaced by another proxy (say a TinyCloud edge, or Caddy or nginx: `deploy/README.md`, "Without Cloudflare"), `TINYSERVER_CLIENT_IP_HEADER` must name the header it sets, `TINYSERVER_TRUSTED_PROXIES` must name it unless it is on a private address, or every client behind it shares one limiter bucket (all sign-in routes then share a cap of five a minute, a self-inflicted outage).

## 4. WebRTC and ICE

A call has three network facts:

1. **The client's offer contains its own interface address.** tinyclient's `mediad` asks the operating system which local interface routes to the server and offers that address as a UDP host candidate and an active TCP candidate (`crates/mediad/src/tiny.rs`, `local_ip`). It uses no STUN server and does not trickle. The address (often private, such as 192.168.x.x; a public one on a directly addressed machine) is therefore in the SDP offer that travels through Cloudflare to tinyserver. tinyserver does not forward the offer. SDP is dropped from logs (`sdp` is a denied field).
2. **The server's answer contains the SFU's address.** The SFU is ICE-lite. Its answer carries one UDP and one passive TCP host candidate on `TINYSERVER_PUBLIC_IP`, and the same pair on `TINYSERVER_LAN_IP` when set (`engine.rs`, `candidates`). Every member in a call learns the host's public IPv4 address, and its private LAN address if one is configured. This is true for every call, E2EE or not.
3. **The SFU learns each client's public mapping** from the client's first STUN check. It never sends one participant's address to another: the answer is built from the SFU's candidates only and the offer is not relayed. Verified by reading the code; **no test asserts it**.

What participants learn about each other, by the protocol: who is in the call (user ids, joining order), mute, deafen, camera and share state, whether each can encrypt, who is speaking (`call.speaking`), and at what rank. Not addresses, not devices, not network quality. The SFU forwards media only: it is not a peer-to-peer mesh, so no participant ever sends to another's address.

IPv6 is not supported: `TINYSERVER_PUBLIC_IP` and `TINYSERVER_LAN_IP` must be IPv4 (`main.rs`).

## 5. What each party learns about a call

| Party | Learns |
|---|---|
| Other participants | The roster and state above. The SFU's address |
| The SFU host (the process and the machine) | Every participant's public address and source port, when they joined and left, packet sizes and timing, who the RFC 6464 audio level says is speaking and how loudly. In a transport-only call, the Opus and H.264 payload. In an E2EE call, DAVE ciphertext with RTP headers in the clear |
| tinyserver's database | Nothing about a call |
| Cloudflare | The signaling: that a `call.join` happened, the SDP (the client's interface address, ICE credentials, DTLS fingerprint), who sent it (by bearer token), when. Not the media |
| The hosting provider and the network path | Flows to and from the media port: addresses, ports, sizes, timing, therefore that a call is happening, who is in it by address, how long, roughly how much video |
| A passive observer near the client | The flow to the media port and the volume |

## 6. TURN and relay options

**Not implemented.** Slice 7 in `docs/calls.md` is a specification: the SFU would allocate on Cloudflare Realtime TURN and advertise only the relay address, so the host publishes no port and no member learns its address. There is no relay code in `crates/sfu` and no `call.candidate` frame (`docs/calls.md` lists `call.candidate` as an unknown type). The properties below are the design's, not the code's.

| Option | Port on the host | Host address hidden from members | Who sees what |
|---|---|---|---|
| A. Direct media port (**what exists**) | UDP and TCP, one number | No | The host sees clients; members see the host |
| B. Cloudflare Realtime TURN, SFU allocates (**designed**, not built) | None. Outbound only | Yes | Cloudflare's relay sees every client's address (clients send to it) and the host's outbound address, and the DTLS-SRTP packets (or DAVE ciphertext) with sizes and timing. It becomes a party to the call's metadata. The host sees only its connection to Cloudflare, not client addresses |
| C. Self-hosted coturn | Still needs published ports | No | Rejected in `calls.md`: it adds a service and does not hide the host |
| D. Cloudflare Realtime SFU instead of the str0m SFU | None | Yes | Media leaves the community's server entirely. Rejected in `calls.md` |
| E. WARP private routing | None | Yes | Every member runs WARP. Rejected for members |
| F. Spectrum UDP | None | Yes | Enterprise plan plus add-on. Rejected for cost |

Real privacy properties of B: it moves visibility from the host to Cloudflare and hides the host's address from members. It does not reduce what the network learns; it changes who the network is. It adds an account-level cost: Cloudflare Realtime TURN is billed at $0.05 per GB from the edge to the TURN client after 1,000 GB a month free, with per-allocation limits of roughly 5 to 10 thousand packets and 50 to 100 Mb a second (figures quoted from `docs/calls.md`, which cites Cloudflare's [TURN pricing and FAQ](https://developers.cloudflare.com/realtime/turn/faq/); Cloudflare's current terms were not re-checked). `docs/calls.md` budgets 8 GB an hour for a six-person camera call and under 1 GB for audio only.

A person who wants to hide their own address from the host and from the community (not just the host's from members) needs a VPN or a proxy of their own. Tor carries TCP streams only, and tinyclient's fallback candidate is active ICE-TCP, but whether `mediad` honours a proxy or Tor setting was **not verified** and no such test exists.

## 7. Bandwidth costs and operational limits

What exists (`docs/calls.md` "Limits", `crates/sfu`):

| Limit | Value | Setting |
|---|---|---|
| People per room call, on the server | 12 (room `user_limit` 1 to 99, capped by the server value), 32 | `TINYSERVER_CALL_ROOM_MAX`, `TINYSERVER_CALL_SERVER_MAX` |
| Forwarded audio per publisher | 192 kb/s | `sfu::Config::audio_kbps` |
| Camera per publisher | 2,400 kb/s high layer, 700 kb/s low | `camera_high_kbps`, `camera_low_kbps` |
| Share per publisher | 5,000 kb/s | `share_kbps` |
| Packets per source address | 2,000 a second, burst 4,000; 4,096 sources | `packets_per_second`, `packet_burst` |
| TCP streams | 256, 16 per source, 64 unverified; 5 s handshake, 30 s idle | `tcp_*` |
| Upload budget for all calls | None unless set | `TINYSERVER_MEDIA_EGRESS_KBPS` (kb/s; a non-positive value fails startup) |
| Container limits | tinyserver 1 GB, 2 CPUs, 512 pids; Postgres 1 GB; clamd 3 GB; Pocket ID and cloudflared 256 MB | compose variables |

What does **not** exist:

- No per-tenant or per-account bandwidth meter, no quota and no billing counters. The SFU emits an in-process `Load` event every five seconds (logged at debug) and nothing aggregates it.
- No per-user or per-address rate limit on REST writes, message posting or uploads. Only the three sign-in routes are limited by address, and the live socket by frames per socket.
- No storage quota per person, channel or community. Per-file caps only (images 8 MiB and other files 25 MiB by default, up to 100 MiB, set by `manage_server`).
- No protection of the media port against volumetric attack. Cloudflare does not proxy UDP on the plans assumed here ([`../calls.md`](../calls.md) table "Constraints"); a flood is absorbed by the per-source budget, which on a laptop measured about 72 percent of one core at 174,000 packets a second (one hand measurement, `docs/calls.md`). The kernel, the NIC and the provider's link are the next limits.
- No IPv6 and no relay (sections 4 and 6).
- No egress rule: joining the `media` network gives tinyserver a route out. An operator can restrict it on the host (`DOCKER-USER`); nothing in the repository does.

The cost model for TinyCloud ([`performance.md`](performance.md)) therefore has no measured inputs yet.

## 8. A hosted tenant's address

In a managed deployment the SDP shows a TinyCloud-controlled IPv4 address and a port per tenant: `deploy/tenant/tenant.sh` gives each tenant `50000 + index` by default and refuses a port another tenant in the same directory has, and `deploy/tests/isolation.sh` checks each tenant publishes its own port and not the other's ([`tenancy.md`](tenancy.md)). The address and port reveal no more than the tenant exists on that host. If several tenants share an IP, the port number distinguishes them to any observer of the media path. Whether hosts and IPs should be shared, rotated or fronted by a relay is still open; [`tenancy.md`](tenancy.md) does not decide it.

## 9. What can never be hidden from the hosting provider

No change to tinyserver or tinyclient can remove these. They are category 4 in [`guarantees.md`](guarantees.md).

1. **Every client address that talks to the media port**, with port, packet sizes and timing, for the whole call. Unless the relay design (B) is built, and then the relay's operator sees it instead.
2. **That a call exists and how long it lasted**, and approximately how much video flowed (bitrate).
3. **The host's own traffic pattern to Cloudflare**: the tunnel is an outbound connection whose volume and timing follow message and signaling activity. Encryption does not hide when someone sends something or roughly how large it is.
4. **Anything in the host's memory and disk** (see [`threat-model.md`](threat-model.md) 3.6): this is not network metadata but follows from the same position.
5. **The address of any outbound connection the host makes**: Cloudflare edge, ClamAV mirrors, an external OIDC provider if configured.
6. **Time correlation** between a flow on the media port and an event on the live socket: who joined a call shortly before a new address appeared on the media port. The provider sees the address; whoever reads logs or Cloudflare sees the user. Joined, the two identify a person. No such join is performed by tinyserver; the data is available to a party that holds both.

And from Cloudflare, for the HTTP path: every client address and every request's timing and size, regardless of E2EE.

## 10. Requirements and open items

| Item | Type |
|---|---|
| A test that the SDP answer contains only the SFU's candidates and no client address | Engineering requirement. Not written |
| A test that the SFU never emits `peer` outside the ring and only when enabled | Partly covered by `peer_is_operator_blind_and_ring_gated` (log layer) |
| Per-tenant bandwidth and storage accounting and caps | Engineering requirement. Not built |
| Relay mode (slice 7), IPv6, a way to advertise a port other than the bound one (so 443 can be used) | Not built |
| Whether `mediad` can use a proxy or Tor for the ICE-TCP path | Not verified |
| Whether the media IP of a TinyCloud tenant may be shared across tenants and how it appears in logs the provider keeps | Open design question |
| Retention of flow logs at the hosting provider | Provider's policy, not reviewed. Question for counsel and procurement |
