# Running a community

Postgres and Pocket ID sit on the internal `inside` network, which has no route to the internet. tinyserver is on `inside` and on `media`. clamd is only on `media`, so tinyserver and clamd have a route out. cloudflared is on `inside` and on a second network that can reach Cloudflare, and it carries HTTP and the live socket for the chat hostname and the sign-in pages for the auth hostname. The one published port is the media port, UDP and the same number on TCP, for call audio and video. Starting a sign-in, trading its code, and asking for a sign-up link are limited to a few tries a minute per address.

tinyserver keeps no passwords. People sign in at an OpenID Connect provider; Pocket ID comes set up as the default one, and people sign in there with a passkey. Signing in proves who someone is to the server. It never grants decryption.

The environment file next to `deploy/compose.yml` is `deploy/.env` when you run compose from `deploy/`. It has to set the database passwords, `TUNNEL_TOKEN`, `TINYSERVER_PUBLIC_URL`, `AUTH_PUBLIC_URL`, and `TINYSERVER_MEDIA_HMAC_SECRET` (64 hex characters, `openssl rand -hex 32`) before `setup.sh`, because that script renders the compose file. There is no `deploy/.env.example`. The repo-root `.env.example` is for a local checkout and does not include `AUTH_PUBLIC_URL` or `TUNNEL_TOKEN`. The sign-in secrets are files in `deploy/secrets`. Neither the env file nor the secrets are committed.

`deploy/compose.yml` reads them. `TINYSERVER_PG_DIR` and `POCKETID_DATA_DIR` can be host directories. Left unset, Docker keeps named volumes.

## Two hostnames

| Variable | Example | What it is |
| --- | --- | --- |
| `TINYSERVER_PUBLIC_URL` | `https://chat.example.org` | The address people type into tinyclient. The tunnel sends it to `http://tinyserver:8080`. |
| `AUTH_PUBLIC_URL` | `https://auth.example.org` | Pocket ID's pages and its OIDC issuer (`APP_URL`). The tunnel sends it to `http://pocket-id:1411`. `TINYAUTH_PUBLIC_URL`, its name before Pocket ID, still works when `AUTH_PUBLIC_URL` is unset. |

Add both as public hostnames on the tunnel in Cloudflare. Passkeys need https; Cloudflare provides it. The issuer is `AUTH_PUBLIC_URL` exactly, with no trailing slash and no path, because that is what Pocket ID puts in its tokens.

tinyserver reaches Pocket ID on the internal network, at `http://pocket-id:1411` (`TINYSERVER_OIDC_INTERNAL_URL`). Discovery, the keys, the code exchange, and the sign-up API go there, with the endpoints Pocket ID names moved to that address. The issuer in discovery and in every ID token must still be the public one. The browser always goes to the public auth hostname. clamd listens at `clamd:3310` on `media` only. That port is not published. Uploads are stored at `/data/uploads` and scanned there. Signature updates use clamd's route out.

The other way is to give tinyserver a route out and leave `TINYSERVER_OIDC_INTERNAL_URL` unset; it then reads everything at the public issuer, through Cloudflare. That is the only way for a provider outside this compose file, such as Google; tinyserver is already on the `media` network, which has a route out.

## Pocket ID

Pocket ID (BSD-2-Clause) is a small OpenID Connect provider where people sign in with passkeys only. It runs as `ghcr.io/pocket-id/pocket-id:v2.19.1-distroless`, pinned by digest, as user 65532, with a read-only root, no capabilities, `no-new-privileges`, a small `/tmp`, and process and memory limits. It has no published port and only the internal network. It keeps its SQLite database, signing key, and uploads in the `pocketid_data` volume at `/app/data`; a one-shot `pocket-id-data` container gives that volume to user 65532 before each start, because the image has no shell.

It is configured from the environment, not its admin pages (`UI_CONFIG_DISABLED`): accounts only with a sign-up token (`ALLOW_USER_SIGNUPS=withToken`), email optional and never trusted as verified, no client secret made on its own, https callback URLs only, no analytics, no version check, no icon library, JSON logs. It trusts the forwarded-for header only from cloudflared, which has the fixed address `TINYSERVER_TUNNEL_IP` (172.30.53.10) on `TINYSERVER_INSIDE_SUBNET` (172.30.53.0/24) (`TRUST_PROXY`). Change both if that range is taken on the host. `AUTH_APP_NAME` names it on its pages, `tinyserver` unless set.

Three secrets: `oidc_client_secret`, tinyserver's secret at Pocket ID; `pocketid_encryption_key`, which encrypts Pocket ID's private data (`ENCRYPTION_KEY_FILE`); and `pocketid_api_key`, Pocket ID's static API key (`STATIC_API_KEY_FILE`). The API key acts as an admin and cannot sign in; tinyserver uses it only to make sign-up tokens, and the setup script to make the client and the first admin. Keep it as close as the database passwords.

Set up, from `deploy/`, with the environment file in place:

```sh
sh pocket-id/setup.sh moss
```

It writes the three secrets unless they are there (the directory 0700, the files 0444, because the containers read them as their own users), starts Pocket ID and waits for it, and then, through Pocket ID's API from a `curlimages/curl` container pinned by digest that shares Pocket ID's network:

1. Makes the OIDC client `tinyserver`, or brings it up to date: a confidential client with PKCE, callback `${TINYSERVER_PUBLIC_URL}/v1/auth/callback`, back-channel logout to `http://tinyserver:8080/v1/auth/backchannel-logout`, and no consent screen. The setup request does not set `client_secret_basic`. tinyserver uses that method when discovery allows it, and `client_secret_post` when that is all discovery offers. Its secret is `secrets/oidc_client_secret`, registered once.
2. With a username, here `moss`: makes that admin unless they exist and prints a one-time login link, `${AUTH_PUBLIC_URL}/lc/<token>`, good for an hour. Open it, and Pocket ID asks for a passkey. Run it again for a fresh link. It also prints their Pocket ID user id, which is the subject tinyserver sees.

Run it again any time; every step is skipped when already done. Compose options go in `COMPOSE`. From `deploy/`, an explicit env file is `COMPOSE="docker compose -p tinyserver --env-file .env" sh pocket-id/setup.sh`. The API key and the client secret reach curl on its standard input, never in a command line. Then `docker compose up -d`.

A Pocket ID account is not a membership, and a membership needs no Pocket ID account made by hand. People still need an invite, or a group the owner allows, to join.

### An invite makes the account

tinyserver runs with `TINYSERVER_OIDC_SIGNUP=pocket-id` and `TINYSERVER_OIDC_POCKETID_API_KEY_FILE`. When someone pastes an invite into tinyclient, the browser opens a page at tinyserver first: "I'm new here" or "I already have an account".

- Already have one: a link straight to Pocket ID's sign-in, as without an invite.
- New: tinyserver checks that the invite could still be accepted (known, not expired, not used up), then asks Pocket ID for one sign-up token, `POST /api/signup-tokens` with `{"ttl":"15m","usageLimit":1}` and the `X-API-Key` header. The page then has two steps: "Make my account" opens `${AUTH_PUBLIC_URL}/signup?token=<token>` in a new tab, where they pick a username and make a passkey; "Continue" in the first tab goes on to Pocket ID's sign-in, where their new session signs them straight in, and back to tinyclient with the invite accepted.

Pocket ID's sign-up page ignores any redirect and ends at its own account page, so the way back is the first tab. One sign-in gets at most one token; asking again shows "Continue" and makes none. An unknown, expired, or used-up invite makes no token and sends tinyclient `not_member`. The form allows 3 tries a minute per address and 30 a minute in all. Neither the key nor any token is logged. tinyclient waits 5 minutes for the browser to come back, so a person who takes longer starts again from the invite; the sign-up token they did not use expires on its own.

`TINYSERVER_OIDC_POCKETID_GROUP_IDS` lists Pocket ID group ids a new account joins. `TINYSERVER_OIDC_SIGNUP=none` turns sign-up off; then invites only work for people who already have an account.

### Moving from Tinyauth

The auth hostname stays, so the issuer stays and tinyclient notices nothing. Every subject changes: Tinyauth's subject is not Pocket ID's user id. So each existing tinyserver account is moved by hand to the person's new Pocket ID account; until then they sign in as a stranger and are turned away as `not_member`, and nothing is kept.

1. Back up the database: `docker compose exec -T postgres pg_dump -U postgres tinyserver > backups/before-pocket-id.sql`.
2. Deploy this version. In the environment file, rename `TINYAUTH_PUBLIC_URL` to `AUTH_PUBLIC_URL` (or leave it; it still works).
3. In Cloudflare, point the tunnel's auth hostname at `http://pocket-id:1411` instead of `http://tinyauth:3000`.
4. `sh pocket-id/setup.sh <username>` for each person who already has a tinyserver account. It keeps `secrets/oidc_client_secret` and registers it on the new client, and prints each one's login link and Pocket ID user id. Send each person their link; they add a passkey.
5. `docker compose up -d --remove-orphans` starts everything and removes the Tinyauth container. `secrets/tinyauth_users` and the `tinyauth_data` volume can go once all is well.
6. List the accounts and their old subjects, then move each one to its new subject:

   ```sh
   docker compose run --rm --entrypoint tinyserver tinyserver-init identities
   docker compose run --rm --entrypoint tinyserver tinyserver-init relink-identity --user-id <tinyserver user id> --subject <Pocket ID user id>
   ```

   Both need the database owner role, so they run in `tinyserver-init`, not in the serving container. `--issuer` defaults to `TINYSERVER_OIDC_ISSUER`. It refuses a subject another account already has, and writes an `identity.relink` audit row. The same in SQL, as `app_owner` (`docker compose exec -T postgres psql -U app_owner tinyserver`):

   ```sql
   UPDATE user_identities SET subject = '<Pocket ID user id>'
   WHERE user_id = '<tinyserver user id>' AND issuer = '<AUTH_PUBLIC_URL>';
   ```

Sessions already open keep working until they expire.

## The first owner

There is no owner password. `tinyserver-init` runs before the server on every `up`. It is the only container that holds the database owner password. It migrates and, while there is no owner, writes a single-use owner invite that lasts 24 hours to a file on a small volume the server mounts read only. The log line only names that file.

```sh
docker compose exec tinyserver cat /bootstrap/owner-invite
```

It is a tinyclient invite code. Paste it into tinyclient's "Join a server" and sign in at Pocket ID ("I'm new here" makes their account); that person becomes the owner. The first time the owner opens the server, the app asks them to choose what the community keeps about its members (convenience or privacy, or each choice one by one: [`docs/server-setup.md`](../docs/server-setup.md)). Nothing is chosen for them. A later start with no owner does not write the token again. It prints that the file still works until the invite expires. Lost it, run `docker compose run --rm --entrypoint tinyserver tinyserver-init bootstrap --owner-invite` for a new one, and the old one stops working. After an owner exists, the file is removed.

`RUST_LOG` is ignored. Set `TINYSERVER_LOG_LEVEL` (`error`, `warn`, `info`, `debug`, or `trace`; default `info`). A debug bundle is written only when the process receives `SIGUSR1`, under `/var/lib/tinyserver/logs`, and old bundles are removed on the next start. Nothing in the log path leaves the box.

The external sender private key is `/var/lib/tinyserver-keys/e2ee-server-key` on the `tinyserver_keys` volume (mode `0600`). It is not in Postgres and not on the tmpfs at `/var/lib/tinyserver`, so a container restart keeps the key clients have pinned. `operator.sh backup` and `restore` carry the file. Losing the file and the database row together stops the server from starting, rather than minting a key the clients would reject.

A server from before sign-in moved to OIDC still has its owner, but that account has no provider identity and cannot sign in. Make an owner invite anyway, sign in with it, and remove the old account from the member list:

```sh
docker compose run --rm --entrypoint tinyserver tinyserver-init bootstrap --owner-invite
```

## Other providers

Any provider with discovery, the authorization code flow, and a confidential client works: Authentik, Keycloak, Zitadel, Google, and so on. Register the redirect URI `${TINYSERVER_PUBLIC_URL}/v1/auth/callback`.

| Variable | Meaning |
| --- | --- |
| `TINYSERVER_OIDC_ISSUER` | The issuer. Discovery is read from it, and it must match exactly. |
| `TINYSERVER_OIDC_CLIENT_ID` | The client id. |
| `TINYSERVER_OIDC_CLIENT_SECRET_FILE` or `TINYSERVER_OIDC_CLIENT_SECRET` | The secret. tinyserver uses `client_secret_basic`, or `client_secret_post` when that is all discovery offers. |
| `TINYSERVER_OIDC_PRIVATE_KEY_FILE`, `TINYSERVER_OIDC_PRIVATE_KEY_ID` | Instead of a secret: `private_key_jwt` (RFC 7523) with an RSA key in PKCS#1 PEM (`openssl genrsa -traditional`), signed RS256, `kid` set when the id is. |
| `TINYSERVER_OIDC_SCOPES` | `openid profile email` unless set. |
| `TINYSERVER_OIDC_PROVIDER_NAME` | Shown in tinyclient. The issuer's host unless set. |
| `TINYSERVER_OIDC_INTERNAL_URL` | Where tinyserver reaches the provider, when not at the issuer. |
| `TINYSERVER_OIDC_GROUPS_CLAIM` | The ID token claim with the person's groups, an array or a comma list. `groups` unless set. |
| `TINYSERVER_OIDC_MEMBER_GROUPS` | Comma list. Someone in one of these, or in an admin group, joins without an invite, audited as `member.join` with `via` `groups`. Unset is invite only. |
| `TINYSERVER_OIDC_ADMIN_GROUPS` | Comma list. A member in one of these gets the admin role (the most senior role with manage_server or manage_roles, "Admin" on an upgraded server) when they sign in, audited as `member.role` with `via` `groups`. Leaving the group does not take it away; someone with manage_roles does that. No claim makes an owner. |
| `TINYSERVER_OIDC_SIGNUP` | `pocket-id` lets an invite make the person's account at Pocket ID ([above](#an-invite-makes-the-account)). Unset or `none` for every other provider. |
| `TINYSERVER_OIDC_POCKETID_API_KEY_FILE` or `TINYSERVER_OIDC_POCKETID_API_KEY` | With `pocket-id`: a Pocket ID API key that may create users (the static API key, or an admin's), at least 16 characters, sent as `X-API-Key`. Required. |
| `TINYSERVER_OIDC_POCKETID_INTERNAL_URL` | Where tinyserver reaches Pocket ID's API. `TINYSERVER_OIDC_INTERNAL_URL` unless set, else the issuer. |
| `TINYSERVER_OIDC_POCKETID_GROUP_IDS` | Comma list of Pocket ID group ids a new account joins. |
| `TINYSERVER_SESSION_MAX_DAYS` | How long a session from sign-in lasts, 1 to 365, 30 unless set. Then tinyclient signs in again. Nothing extends a session, and no provider refresh token is kept. |

Several providers at once: `TINYSERVER_OIDC_PROVIDERS=pocket-id,google`, then the same variables with the id after `OIDC_`, such as `TINYSERVER_OIDC_GOOGLE_ISSUER`, `TINYSERVER_OIDC_GOOGLE_CLIENT_SECRET_FILE`, and `TINYSERVER_OIDC_GOOGLE_NAME` for the name. The first is the default. A person is the pair of issuer and subject, so the same person at two providers is two accounts. Email never links accounts.

PKCE is S256 on every sign-in, whether or not discovery mentions it. ID tokens must be RS256, ES256, or EdDSA, signed by a key in the provider's key set; a token with a key id the server has not seen makes it read the set again, at most every 10 seconds.

## Logout from the provider

A provider that supports OpenID Connect Back-Channel Logout 1.0 can end the sessions it started. Register this as the client's back-channel logout URI:

```
${TINYSERVER_PUBLIC_URL}/v1/auth/backchannel-logout
```

The logout token is checked like an ID token: signature, issuer, audience, `iat`, the logout event, no `nonce`. A `sid` ends the sessions from that provider session; a `sub` ends that person's sessions from that provider; both together end the ones that match both. Their live sockets close at once. Sessions are audited as `session.logout`. Pocket ID supports this, by `sub` (it sends no `sid`); the setup script registers `http://tinyserver:8080/v1/auth/backchannel-logout`, inside the compose network. Signing out in tinyclient ends its own session, and "Sign out everywhere" ends all of them.

## Calls and the media port

Call audio and video do not fit through the tunnel, so they go straight to the server on one port number, UDP first and TCP for networks that block UDP. Without it the server still runs, and a `call.join` with an offer gets `media_unavailable`.

1. Set `TINYSERVER_PUBLIC_IP` in the environment file to the address members reach: the router's public IPv4 address. The server does not guess it.
2. `TINYSERVER_MEDIA_PORT` is 50000 unless set. Compose publishes the same number inside and out, on UDP and TCP, because the server advertises the port it binds.
3. On the router, forward that port to the host, both UDP and TCP.
4. On the host firewall, allow `TINYSERVER_MEDIA_PORT` for both UDP and TCP. With the default port that is `ufw allow 50000/udp` and `ufw allow 50000/tcp`.
5. Optional: set `TINYSERVER_LAN_IP` to the host's address on the home network, such as `192.168.1.20`. Many routers do not hairpin, so members at home cannot reach the public address from inside; with this set, the answer offers them the LAN address too. Members elsewhere see it in the SDP but cannot use it.
6. Optional: set `TINYSERVER_MEDIA_EGRESS_KBPS` to what the host can upload for calls, in kb/s, such as `8000` on a 10 Mb/s line. The server plans every call together to fit it: everyone gets the high camera layer if that fits, else the low one, else audio only. Each listener with a camera to watch costs about 1,200 kb/s on the high layer and 350 on the low one, and each voice about 50.
7. `docker compose up -d`, then open `GET /health` on the chat hostname through the tunnel. Port 8080 is not published on the host. It says `"media": "up"` and lists `"transports": ["udp", "tcp"]`, with `"lan"` as well when step 5 is set.

Every member in a call sees `TINYSERVER_PUBLIC_IP` in the SDP answer. The tunnel no longer hides the host's address from members. Strangers who find the port get STUN handling only; nothing else is processed without the per-join ICE password, which only travels over the signed-in live socket.

Joining the `media` network gives the server a route out, which it did not have. It makes no outbound connections except to a sign-in provider outside the compose file, when one is configured; an owner who wants that enforced can add a `DOCKER-USER` rule that allows only replies.

### On a VPS

A small VPS drops call packets long before its CPU is busy, because the kernel's socket buffer limits are low and the provider's line is shared. Two settings matter.

The media socket asks for 4 MiB of buffer each way. Linux caps that at `net.core.rmem_max` and `net.core.wmem_max`, about 200 KiB on a stock kernel, without an error. These are host settings; a container cannot change them. On the host:

```sh
printf 'net.core.rmem_max=4194304\nnet.core.wmem_max=4194304\n' | sudo tee /etc/sysctl.d/90-tinyserver-media.conf
sudo sysctl --system
```

Restart tinyserver afterwards. Its log says `media socket buffers` with what it got; Linux reports double the usable size, so about 8388608 each way means the setting took.

Set `TINYSERVER_MEDIA_EGRESS_KBPS` to a little under what the plan really sustains, not its burst figure. A VPS sold as "1 Gb/s" may hold far less for long, or meter it. Without a budget, nothing stops a large room sending more than the line carries, and everyone's audio suffers along with the video. Once a minute during a call the log has a `media counters` line: `send_dropped` above zero means the host could not keep up, and `longest_turn_ms` above about 20 means the media thread was short of CPU.

TCP streams are capped at 256 in all and 16 from one address. One that sends no STUN for a call within 5 s, or nothing at all for 30 s, is closed. A relay mode for hosts that cannot open a port comes in a later release.

## Without Cloudflare

Nothing in tinyserver needs Cloudflare. It is a plain HTTP server on `BIND` (inside the container, port 8080). `compose.yml` happens to carry cloudflared as the way to reach it from the internet. If you use something else, `compose.proxy.yml` turns cloudflared off and publishes port 8080 on the host's loopback for your proxy:

```
docker compose -f compose.yml -f compose.proxy.yml up -d
```

(Add `-f compose.passkey.yml` between them if you use passkeys only.) Put `TUNNEL_TOKEN=unused` in `.env`: compose reads that variable while it loads the file even though cloudflared is not started.

What your front end must do:

1. **End TLS** for the hostname in `TINYSERVER_PUBLIC_URL`. tinyserver does not do TLS itself, and browsers refuse passkeys on plain HTTP (only `localhost` is exempt). Keep that hostname fixed: a passkey belongs to it.
2. **Pass WebSockets** (the live socket is `/v1/live`) and leave idle connections open.
3. **Set one header to the client's address**, replacing whatever the client sent, and tell tinyserver its name with `TINYSERVER_CLIENT_IP_HEADER`. This keys the sign-in rate limits. Without it every member counts as the proxy's own address and shares one allowance, so recovery and claim links (8 a minute in all) lock up as soon as a few people try at once. tinyserver believes the header only from a proxy it trusts, by default any private or loopback address, so a proxy on the same host or Docker network needs nothing more; list a proxy with a public address in `TINYSERVER_TRUSTED_PROXIES`. A list such as `x-forwarded-for` is read from its right, where the proxy wrote, never the left, where the client can.

Caddy, on the host (it gets and renews the certificate itself):

```
chat.example.org {
    reverse_proxy 127.0.0.1:8080 {
        header_up X-Real-IP {remote_host}
    }
}
```

with `TINYSERVER_CLIENT_IP_HEADER=x-real-ip`. nginx, in a `server` block that already ends TLS:

```
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_read_timeout 3600s;
}
```

with the same setting. Another tunnel (Tailscale Funnel, Pangolin, frp) works the same way: it needs a hostname, TLS, WebSockets and a header with the client's address. If it cannot set one, leave the variable unset and accept one shared allowance, which is fine for a handful of people and not for a community.

Two things do not change. Calls still need their media port reached directly, forwarded on your router (see "Calls and the media port"): a proxy or tunnel carries the web pages and chat, not call audio. And with a proxy that listens on 80 and 443 you forward those instead of using a tunnel's outbound connection.

Not tested here: these configurations were written from each proxy's documentation and checked only as far as `docker compose config` accepting the files and the server reading the header (`crates/api/src/limit.rs` tests). Run a sign-in through yours, and check `tinyserver` logs for "rate limited" under normal use, before relying on it.

## Upgrading

From `deploy/`, back up the database first (`docker compose exec -T postgres pg_dump -U postgres tinyserver > backups/before-upgrade.sql`), then:

```sh
git pull
docker compose up -d --build --remove-orphans
```

`tinyserver-init` applies any new migrations and exits before the server starts. If it fails, the server does not start; `docker compose logs tinyserver-init` says why. Going back to an older release means restoring that backup: an older binary refuses a database that has migrations it does not know.

### Upgrading to the release with `tinyserver-init`

The release that moved the database owner password out of the serving container changes these things for an existing install. The steps above are all that is needed; nothing has to be set by hand.

- **New one-shot service and volume.** `tinyserver-init` (same image) migrates and bootstraps, and `tinyserver_bootstrap` holds the owner invite. `up` creates both. `tinyserver-uploads` now also gives the new volume to user 10001, and runs again because its command changed. The serving container has `DATABASE_URL` (`app_runtime`) only, and its entrypoint no longer migrates.
- **Owner-role commands move.** `bootstrap --owner-invite`, `identities` and `relink-identity` run as `docker compose run --rm --entrypoint tinyserver tinyserver-init <command>`. `docker compose exec tinyserver tinyserver <command>` now fails with `DATABASE_URL_MIGRATE is not set`, on purpose.
- **The owner invite moved** from `/var/lib/tinyserver/owner-invite` to `/bootstrap/owner-invite` (read it with `docker compose exec tinyserver cat /bootstrap/owner-invite`). It is removed by the next `tinyserver-init` run after an owner exists, so after the owner has signed in, `docker compose up -d` once more clears it.
- **Migrations 0013 and 0014.** 0013 replaces `app_emit` and `app_emit_to` so a channel's live events go only to members who can view it. 0014 adds `users.deleted_at`, `DELETE /v1/me`, and a trigger that rewrites the live-event history when a message is edited or deleted. Neither rewrites existing rows; both are quick. Events already in the 24 hour outbox before the upgrade keep the audience and text they were stored with until they age out.
- **Resource limits.** tinyserver 1 GiB, 512 processes, 2 CPUs; Postgres 1 GiB, 256 processes; clamd 3 GiB; tinyserver-init 256 MiB. These are caps, not reservations. A large community raises them with `TINYSERVER_MEM_LIMIT`, `TINYSERVER_PIDS_LIMIT`, `TINYSERVER_CPUS`, `TINYSERVER_PG_MEM_LIMIT`, `TINYSERVER_PG_PIDS_LIMIT` and `TINYSERVER_CLAMD_MEM_LIMIT`. Postgres now starts with only the capabilities it needs to drop to its own user, and the tunnel with none and a read-only root.
- **Sign-in limiter.** It counts the connection's own address and believes `cf-connecting-ip` only from a trusted proxy. The default trusts private, loopback, link-local and 100.64.0.0/10 peers, which covers cloudflared on the `inside` network. To trust only the tunnel, set `TINYSERVER_TRUSTED_PROXIES=172.30.53.10` (or your `TINYSERVER_TUNNEL_IP`); a proxy with a public address must be listed; `none` believes no header.
- **Single-container setups** that run the image on its own and pass `DATABASE_URL_MIGRATE` keep working: the entrypoint still migrates and bootstraps when that variable is set, then drops it before serving. That keeps the owner role in the serving process; prefer running `tinyserver migrate` and `tinyserver bootstrap` as a separate step.

## Stopping

`docker stop` sends SIGTERM. The server ends every call, finishes the requests it has, and exits.

## Suspending, backups and moving out

`sh tenant/operator.sh <command> --stack .env` works on this community as it does on a tenant: `suspend --read-only` or `--offline` with a notice members see, `resume`, a legal `hold`, an age-encrypted `backup` and `restore` of the database, uploads, Pocket ID's data and the secrets, an `export` to run elsewhere, and `delete`. `tenant/README.md` has the commands and `docs/privacy/tenancy.md` what each one does and does not do. Nothing in them listens on a port.

## Scanning off

`docker compose -f compose.yml -f scan-off.yml up -d` runs without clamd and without scanning uploads. Only for a community whose attachments are sealed on the sending device; anywhere else it lets any file through. Without that file nothing changes.

## Many communities on one host

`tenant/tenant.sh` runs this compose file once per community, each with its own project, networks, volumes, subnet, media port, database, roles, passwords and secrets. See `tenant/README.md`.

## Housekeeping

Every ten minutes the server deletes expired sessions, expired invites nobody used, expired idempotency keys, sign-ins nobody finished, and one-time codes nobody redeemed. Accepted invites stay in the invite list.

## Images

`cloudflare/cloudflared` is pinned to a release tag, and Pocket ID and the setup script's `curlimages/curl` to a release tag and digest. Raise them on purpose, after reading the release notes.
