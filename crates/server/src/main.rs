use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use db::PgPool;

mod auth_cmd;
mod log_policy;

#[derive(Parser)]
#[command(name = "tinyserver", about = "Self-hosted voice and video server")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Apply SQL migrations as the database owner.
    Migrate,
    /// Print a single-use owner invite, good for 24 hours, when there is no owner yet. While
    /// an earlier one still works it is kept and nothing new is printed. Does nothing when an
    /// owner exists.
    Bootstrap {
        /// Make a new owner invite even though an earlier one still works or an owner exists.
        /// For a lost invite, or an owner who signed in with a password before sign-in moved to
        /// an OIDC provider.
        #[arg(long)]
        owner_invite: bool,
    },
    /// Serve HTTP and the live socket, and media when `TINYSERVER_PUBLIC_IP` is set.
    Serve,
    /// List every account's sign-in identities: user id, display name, issuer, subject.
    Identities,
    /// Point an account's identity at an issuer to a new subject, for a provider whose subjects
    /// changed, such as a move from Tinyauth to Pocket ID at the same address. The person then
    /// signs in as that subject and keeps their account.
    RelinkIdentity {
        /// The tinyserver account, from `tinyserver identities`.
        #[arg(long)]
        user_id: String,
        /// The person's subject at the provider now, such as their Pocket ID user id.
        #[arg(long)]
        subject: String,
        /// The provider's issuer. `TINYSERVER_OIDC_ISSUER` when unset.
        #[arg(long)]
        issuer: Option<String>,
    },
    /// The operator's switches for the whole community: suspend it read-only or offline with a
    /// notice, resume it, and place or release a legal hold. As the owner role
    /// (`DATABASE_URL_MIGRATE`), so only whoever runs the init container can. The serving process
    /// obeys a change within `TINYSERVER_OPERATOR_REFRESH_MS` (2 seconds).
    Operator {
        #[command(subcommand)]
        action: OperatorAction,
    },
    /// Move a community to passkeys: export the roster, make claim links, check progress,
    /// retire the old provider's identities. See docs/auth-passkeys.md.
    Auth {
        #[command(subcommand)]
        command: auth_cmd::AuthCommand,
    },
}

#[derive(Subcommand)]
enum OperatorAction {
    /// Print the mode, the notice, the hold, and how many reports owners put on hold.
    Status {
        /// One line of JSON, for scripts.
        #[arg(long)]
        json: bool,
    },
    /// Read-only: members sign in, read, download, report, sign out and delete their account,
    /// and nothing else. Offline: only the notice and the health check answer.
    Suspend {
        #[arg(long, conflicts_with = "offline", required_unless_present = "offline")]
        read_only: bool,
        #[arg(long)]
        offline: bool,
        /// What members see, 1 to 500 characters.
        #[arg(
            long,
            conflicts_with = "notice_file",
            required_unless_present = "notice_file"
        )]
        notice: Option<String>,
        /// The notice from a file.
        #[arg(long)]
        notice_file: Option<String>,
    },
    /// Back to normal. The notice goes; a legal hold stays.
    Resume,
    /// Stop the community being deleted, and resolved reports expiring, until released.
    Hold {
        /// For the operator's record only; never shown to members.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Release the legal hold.
    Release {
        #[arg(long)]
        reason: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let logs = log_policy::install(log_policy::LogPolicy::from_env());

    match Cli::parse().command {
        Command::Migrate => migrate().await?,
        Command::Bootstrap { owner_invite } => bootstrap(owner_invite).await?,
        Command::Serve => {
            if let Err(error) = logs.sweep_bundles() {
                tracing::warn!(%error, "old debug bundles not removed");
            }
            serve().await?;
        }
        Command::Identities => identities().await?,
        Command::RelinkIdentity {
            user_id,
            subject,
            issuer,
        } => relink_identity(&user_id, &subject, issuer).await?,
        Command::Operator { action } => operator(action).await?,
        Command::Auth { command } => auth_cmd::run(command).await?,
    }
    Ok(())
}

async fn identities() -> anyhow::Result<()> {
    let pool = db::owner_pool(&env_var("DATABASE_URL_MIGRATE")?).await?;
    let rows = db::list_identities(&pool).await?;
    if rows.is_empty() {
        println!("No identities.");
    }
    for (user_id, name, issuer, subject) in rows {
        println!("{user_id}  {name}\n    issuer  {issuer}\n    subject {subject}");
    }
    Ok(())
}

async fn relink_identity(
    user_id: &str,
    subject: &str,
    issuer: Option<String>,
) -> anyhow::Result<()> {
    let issuer = match issuer {
        Some(issuer) => issuer,
        None => env_var("TINYSERVER_OIDC_ISSUER").context("pass --issuer")?,
    };
    if !(1..=255).contains(&subject.len()) || subject.trim() != subject {
        bail!("the subject must be 1 to 255 characters with no space around it");
    }
    let pool = db::owner_pool(&env_var("DATABASE_URL_MIGRATE")?).await?;
    match db::relink_identity(&pool, user_id, &issuer, subject).await? {
        db::Relinked::Moved { previous } => {
            tracing::info!(%user_id, %issuer, "identity relinked");
            match previous {
                Some(previous) => println!("{user_id} at {issuer}: {previous} -> {subject}"),
                None => println!("{user_id} at {issuer}: now {subject}"),
            }
        }
        db::Relinked::Unchanged => println!("{user_id} at {issuer} is already {subject}."),
        db::Relinked::Taken { user_id: other } => bail!(
            "account {other} already signs in as {subject} at {issuer}; remove that account \
             first or pick another"
        ),
        db::Relinked::NoSuchUser => bail!("no account {user_id}"),
    }
    Ok(())
}

async fn operator(action: OperatorAction) -> anyhow::Result<()> {
    let pool = db::owner_pool(&env_var("DATABASE_URL_MIGRATE")?).await?;
    let state = match action {
        OperatorAction::Status { json } => {
            let state = db::operator_state(&pool).await?;
            let held = db::held_reports(&pool).await?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "mode": state.mode.as_str(),
                        "notice": state.notice,
                        "since": state.since,
                        "legal_hold": state.legal_hold,
                        "held_reports": held,
                    })
                );
            } else {
                print_state(&state);
                println!("reports on hold  {held}");
            }
            return Ok(());
        }
        OperatorAction::Suspend {
            read_only,
            offline: _,
            notice,
            notice_file,
        } => {
            let notice = match (notice, notice_file) {
                (Some(notice), _) => notice,
                (None, Some(path)) => std::fs::read_to_string(&path)
                    .with_context(|| format!("read --notice-file {path}"))?,
                (None, None) => bail!("pass --notice or --notice-file"),
            };
            let mode = if read_only {
                db::Mode::ReadOnly
            } else {
                db::Mode::Offline
            };
            let state = db::set_operator_mode(&pool, mode, Some(&notice)).await?;
            tracing::info!(mode = mode.as_str(), "community suspended");
            state
        }
        OperatorAction::Resume => {
            let state = db::set_operator_mode(&pool, db::Mode::Normal, None).await?;
            tracing::info!("community resumed");
            state
        }
        OperatorAction::Hold { reason } => {
            let state = db::set_legal_hold(&pool, true, reason.as_deref()).await?;
            tracing::info!("legal hold placed");
            state
        }
        OperatorAction::Release { reason } => {
            let state = db::set_legal_hold(&pool, false, reason.as_deref()).await?;
            tracing::info!("legal hold released");
            state
        }
    };
    print_state(&state);
    Ok(())
}

fn print_state(state: &db::OperatorState) {
    println!("mode             {}", state.mode.as_str());
    if let Some(notice) = &state.notice {
        println!("notice           {notice}");
    }
    println!("since            {}", state.since);
    println!(
        "legal hold       {}",
        if state.legal_hold { "yes" } else { "no" }
    );
}

async fn migrate() -> anyhow::Result<()> {
    let url = env_var("DATABASE_URL_MIGRATE")?;
    let pool = db::owner_pool(&url).await?;
    db::migrate(&pool).await?;
    tracing::info!("migrations applied");
    Ok(())
}

async fn bootstrap(force: bool) -> anyhow::Result<()> {
    let url = env_var("DATABASE_URL_MIGRATE")?;
    let pool = db::owner_pool(&url).await?;
    let issued = match db::bootstrap(&pool, force)
        .await
        .context("bootstrap failed")?
    {
        db::Bootstrap::Created(issued) => issued,
        db::Bootstrap::OwnerExists => {
            tracing::info!("an owner already exists");
            log_policy::remove_owner_invite(&log_policy::data_dir());
            return Ok(());
        }
        db::Bootstrap::Pending { id, expires_at } => {
            // The token was written once, to the invite file. The database keeps only its hash.
            let path = log_policy::data_dir().join("owner-invite");
            tracing::info!(
                invite_id = %id,
                %expires_at,
                path = %path.display(),
                "no owner yet; the earlier owner invite still works"
            );
            println!();
            println!(
                "No owner yet. The owner invite in {} works until {expires_at}.",
                path.display()
            );
            println!(
                "If that file is gone, run `tinyserver bootstrap --owner-invite` for a new one."
            );
            println!();
            return Ok(());
        }
    };
    let expires_at = issued.expires_at.clone().unwrap_or_default();
    tracing::info!(invite_id = %issued.id, %expires_at, "owner invite created");
    // The database keeps only the hash. The link and token go to a file the operator reads,
    // not to stdout, which Docker keeps.
    let mut body = format!("Owner invite, single use, until {expires_at}:\n");
    if let Ok(origin) = env_var("TINYSERVER_PUBLIC_URL") {
        if let Some(link) = join_url(&origin, &issued.token) {
            body.push_str(&format!("  Link: {link}\n"));
        }
    }
    match env_var("TINYSERVER_PUBLIC_URL")
        .ok()
        .and_then(|origin| join_code(&origin, &issued.token))
    {
        Some(code) => body.push_str(&format!("  Code: {code}\n")),
        None => body.push_str(&format!("  Token: {}\n", issued.token)),
    }
    body.push_str(
        "Paste it into tinyclient's \"Join a server\" and sign in. That person becomes the owner.\n",
    );
    let written = log_policy::write_owner_invite(&log_policy::data_dir(), &body);
    let status = log_policy::invite_status(&written);
    println!("{status}");
    tracing::info!("{status}");
    Ok(())
}

/// Shareable URL; use the same strict origin rules as the client link parser.
fn join_url(origin: &str, token: &str) -> Option<String> {
    let origin = origin.trim().trim_end_matches('/');
    if origin.len() > 512
        || origin
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || c == '\\')
    {
        return None;
    }
    let (_, authority) = origin.split_once("://")?;
    if authority.is_empty()
        || authority.contains(['/', '?', '#', '@'])
        || authority.ends_with(':')
        || authority.ends_with('.')
    {
        return None;
    }
    let parsed = url::Url::parse(origin).ok()?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none_or(|host| host.ends_with('.'))
        || parsed.port() == Some(0)
        || token.is_empty()
        || token.len() > 256
        || !token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return None;
    }
    let code = join_code(&parsed.origin().ascii_serialization(), token)?;
    (code.len() <= 2048).then(|| format!("https://tinyclient.io/invite#{code}"))
}

/// tinyclient's invite code: `tc1.`, the server origin in hex, `.`, the invite token.
fn join_code(origin: &str, token: &str) -> Option<String> {
    let origin = origin.trim().trim_end_matches('/');
    if origin.is_empty() || origin.chars().any(char::is_whitespace) {
        return None;
    }
    let hex: String = origin.bytes().map(|byte| format!("{byte:02x}")).collect();
    Some(format!("tc1.{hex}.{token}"))
}

async fn serve() -> anyhow::Result<()> {
    api::engine_from_env().context("refusing to serve without a scan engine")?;
    if api::scanning_off() {
        tracing::warn!(
            "TINYSERVER_SCAN_ENGINE is none: uploads are not scanned; only for sealed attachments"
        );
    }
    // Whose `cf-connecting-ip` is believed. Unset: any private or loopback peer, which is the
    // tunnel on the Docker network. `none`: nobody's, and every client counts as its own
    // connection. A public address is never believed, whatever this says it trusts by default.
    if let Ok(spec) = env_var("TINYSERVER_TRUSTED_PROXIES") {
        api::trust_proxies(&spec)
            .map_err(|error| anyhow::anyhow!("TINYSERVER_TRUSTED_PROXIES: {error}"))?;
    }
    // Which header the proxy puts the client's address in. Cloudflare's unless told otherwise.
    if let Ok(name) = env_var("TINYSERVER_CLIENT_IP_HEADER") {
        api::client_ip_header(&name)
            .map_err(|error| anyhow::anyhow!("TINYSERVER_CLIENT_IP_HEADER: {error}"))?;
    }
    // Days a dismissed or actioned abuse report is kept. A bad value stops the start: guessing
    // would either keep reports longer than the operator said or delete them sooner.
    if db::retention_days_from(
        std::env::var("TINYSERVER_REPORT_RETENTION_DAYS")
            .ok()
            .as_deref(),
    )
    .is_none()
    {
        anyhow::bail!(
            "TINYSERVER_REPORT_RETENTION_DAYS is not a whole number from {} to {}",
            db::RETENTION_MIN_DAYS,
            db::RETENTION_MAX_DAYS
        );
    }
    let url = env_var("DATABASE_URL")?;
    let bind: SocketAddr = env_var("BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse()
        .context("BIND is not a socket address")?;
    let pool = db::runtime_pool(&url).await?;
    // The private half of the external sender moves out of Postgres before anything is served,
    // so a backup taken of an idle process does not hold it. A lost key stops the start.
    api::seal_external_sender(&pool)
        .await
        .map_err(|reason| anyhow::anyhow!(reason))?;
    let defaults = api::Limits::default();
    let limits = api::Limits {
        call_room_max: env_count("TINYSERVER_CALL_ROOM_MAX", defaults.call_room_max)?,
        call_server_max: env_count("TINYSERVER_CALL_SERVER_MAX", defaults.call_server_max)?,
        ..defaults
    };
    let sign_in = sign_in().await?;
    let media = media_config(limits.call_room_max)?;
    let live = match media {
        Some(config) => {
            let (media, events) = sfu::start(config).context("start media")?;
            tracing::info!(addr = %media.addr(), transports = ?media.transports(), "media advertised");
            api::Live::with_media(pool.clone(), limits, media, events)
        }
        None => {
            tracing::warn!("TINYSERVER_PUBLIC_IP is not set; calls have no media");
            api::Live::start(pool.clone(), limits)
        }
    };
    let app = api::app(pool.clone(), live.clone(), Some(sign_in));
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("bind {bind}"))?;
    tracing::info!(%bind, "listening");
    let sweeper = tokio::spawn(sweep_every(pool, SWEEP_EVERY));
    let watcher = tokio::spawn(exit_if_media_fails(live.clone()));
    let served = axum_serve(listener, app, live.clone()).await;
    sweeper.abort();
    watcher.abort();
    live.close();
    if !live.closed(DRAIN).await {
        tracing::warn!(sockets = live.sockets(), "live sockets still open at exit");
    }
    served
}

/// The OIDC providers from the environment, discovered. A provider that cannot be reached is
/// tried again for about a minute, since it may be starting next to this server; a wrong issuer
/// or setting stops the start at once.
async fn sign_in() -> anyhow::Result<api::SignIn> {
    let session_days = match env_var("TINYSERVER_SESSION_MAX_DAYS") {
        Err(_) => 30,
        Ok(value) => value
            .parse::<u32>()
            .ok()
            .filter(|days| (1..=365).contains(days))
            .context("TINYSERVER_SESSION_MAX_DAYS is not a whole number from 1 to 365")?,
    };
    let mode = AuthMode::from_env()?;
    let public_url = env_var("TINYSERVER_PUBLIC_URL")?;
    let passkeys = if mode == AuthMode::Oidc {
        None
    } else {
        // The relying party is the host of the public address: change the domain and every
        // passkey stops working. See docs/auth-passkeys.md.
        let name = env_var("TINYSERVER_PASSKEY_RP_NAME").unwrap_or_else(|_| "TinyComms".into());
        // Unset: the host of the public address. Set only to a domain above it, and before
        // anyone makes a passkey, since it cannot change afterwards.
        let rp_id = env_var("TINYSERVER_PASSKEY_RP_ID").ok();
        Some(
            api::Passkeys::new(&public_url, &name, rp_id.as_deref())
                .map_err(|error| anyhow::anyhow!("{error}"))?,
        )
    };
    if let (AuthMode::Passkey, Some(passkeys)) = (mode, &passkeys) {
        let sign_in = api::SignIn::passkey_only(&public_url, session_days, passkeys.clone())
            .map_err(|error| anyhow::anyhow!("sign-in: {error}"))?;
        tracing::info!(mode = "passkey", "sign-in ready");
        return Ok(sign_in);
    }
    let config = api::SignInConfig {
        public_url,
        session_days,
        providers: providers_from_env()?,
    };
    let mut tries = 0;
    loop {
        match api::SignIn::discover(config.clone()).await {
            Ok(sign_in) => {
                let sign_in = match passkeys.clone() {
                    Some(passkeys) => sign_in.with_passkeys(passkeys),
                    None => sign_in,
                };
                tracing::info!(
                    providers = config.providers.len(),
                    passkeys = passkeys.is_some(),
                    default = %sign_in.provider_name(),
                    "sign-in providers ready"
                );
                return Ok(sign_in);
            }
            Err(error) if error.transient && tries < DISCOVERY_TRIES => {
                tries += 1;
                tracing::warn!(%error, tries, "sign-in provider not ready; trying again");
                tokio::time::sleep(DISCOVERY_PAUSE).await;
            }
            Err(error) => bail!("sign-in: {error}"),
        }
    }
}

/// `TINYSERVER_AUTH`: how people sign in.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum AuthMode {
    /// Only the configured identity provider. What a server does when this is not set.
    Oidc,
    /// Only built-in passkeys. No provider is contacted or needed.
    Passkey,
    /// Both: the passkey page opens first and offers the provider as the other way in. For the
    /// stretch while members move from a provider to passkeys.
    Both,
}

impl AuthMode {
    fn from_env() -> anyhow::Result<Self> {
        match std::env::var("TINYSERVER_AUTH")
            .ok()
            .as_deref()
            .map(str::trim)
        {
            None | Some("" | "oidc") => Ok(Self::Oidc),
            Some("passkey") => Ok(Self::Passkey),
            Some("both") => Ok(Self::Both),
            Some(other) => bail!("TINYSERVER_AUTH is {other}; it is oidc, passkey or both"),
        }
    }
}

const DISCOVERY_TRIES: u32 = 12;
const DISCOVERY_PAUSE: Duration = Duration::from_secs(5);

/// `TINYSERVER_OIDC_PROVIDERS=pocket-id,google` reads `TINYSERVER_OIDC_POCKET_ID_*` and
/// `TINYSERVER_OIDC_GOOGLE_*`, first one the default. Without it, the single provider in
/// `TINYSERVER_OIDC_*` has the id `default`.
fn providers_from_env() -> anyhow::Result<Vec<api::ProviderConfig>> {
    let Ok(list) = env_var("TINYSERVER_OIDC_PROVIDERS") else {
        return Ok(vec![provider_from_env(
            "default",
            "TINYSERVER_OIDC_",
            "TINYSERVER_OIDC_PROVIDER_NAME",
        )?]);
    };
    list.split(',')
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(|id| {
            let id = id.to_ascii_lowercase();
            let prefix = format!(
                "TINYSERVER_OIDC_{}_",
                id.to_ascii_uppercase().replace('-', "_")
            );
            let name = format!("{prefix}NAME");
            provider_from_env(&id, &prefix, &name)
        })
        .collect()
}

fn provider_from_env(
    id: &str,
    prefix: &str,
    name_key: &str,
) -> anyhow::Result<api::ProviderConfig> {
    let key = |suffix: &str| format!("{prefix}{suffix}");
    let list = |suffix: &str| -> Vec<String> {
        env_var(&key(suffix))
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|item| !item.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    };
    Ok(api::ProviderConfig {
        id: id.to_string(),
        issuer: env_var(&key("ISSUER"))?,
        client_id: env_var(&key("CLIENT_ID"))?,
        client_secret: secret_from_env(&key("CLIENT_SECRET"))?,
        private_key_pem: match env_var(&key("PRIVATE_KEY_FILE")) {
            Err(_) => None,
            Ok(path) => Some(
                std::fs::read_to_string(&path)
                    .with_context(|| format!("read {} {path}", key("PRIVATE_KEY_FILE")))?,
            ),
        },
        private_key_id: env_var(&key("PRIVATE_KEY_ID")).ok(),
        scopes: env_var(&key("SCOPES"))
            .unwrap_or_else(|_| "openid profile email".to_string())
            .split_whitespace()
            .map(str::to_string)
            .collect(),
        name: env_var(name_key).ok(),
        internal_url: env_var(&key("INTERNAL_URL")).ok(),
        groups_claim: env_var(&key("GROUPS_CLAIM")).ok(),
        admin_groups: list("ADMIN_GROUPS"),
        member_groups: list("MEMBER_GROUPS"),
        signup: signup_from_env(prefix)?,
    })
}

/// `<prefix>SIGNUP=pocket-id` lets someone with an invite make their account at the provider.
/// It needs `<prefix>POCKETID_API_KEY_FILE` (or `<prefix>POCKETID_API_KEY`); Pocket ID's API is
/// reached at `<prefix>POCKETID_INTERNAL_URL`, else the provider's internal URL, else its issuer.
/// `<prefix>POCKETID_GROUP_IDS` lists Pocket ID groups a new account joins.
fn signup_from_env(prefix: &str) -> anyhow::Result<Option<api::SignupConfig>> {
    let key = |suffix: &str| format!("{prefix}{suffix}");
    let Ok(kind) = env_var(&key("SIGNUP")) else {
        return Ok(None);
    };
    match kind.trim().to_ascii_lowercase().as_str() {
        "none" | "off" => return Ok(None),
        "pocket-id" | "pocketid" => {}
        other => bail!("{} is {other}; the only kind is pocket-id", key("SIGNUP")),
    }
    let api_key = secret_from_env(&key("POCKETID_API_KEY"))?.with_context(|| {
        format!(
            "{} is pocket-id but {} is not set",
            key("SIGNUP"),
            key("POCKETID_API_KEY_FILE")
        )
    })?;
    let group_ids = env_var(&key("POCKETID_GROUP_IDS"))
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    Ok(Some(api::SignupConfig {
        api_url: env_var(&key("POCKETID_INTERNAL_URL")).ok(),
        api_key,
        group_ids,
    }))
}

/// `<name>_FILE`, preferred, or `<name>`. Not both. `None` when neither is set.
fn secret_from_env(name: &str) -> anyhow::Result<Option<String>> {
    let file_name = format!("{name}_FILE");
    match (env_var(name).ok(), env_var(&file_name).ok()) {
        (Some(_), Some(_)) => bail!("set {name} or {file_name}, not both"),
        (Some(secret), None) => Ok(Some(secret)),
        (None, Some(path)) => {
            let secret = std::fs::read_to_string(&path)
                .with_context(|| format!("read {file_name} {path}"))?;
            let secret = secret.trim_end().to_string();
            if secret.is_empty() {
                bail!("{file_name} {path} is empty");
            }
            Ok(Some(secret))
        }
        (None, None) => Ok(None),
    }
}

/// Media binds every interface on `TINYSERVER_MEDIA_PORT`, UDP and TCP, and advertises
/// `TINYSERVER_PUBLIC_IP` with the same port, and `TINYSERVER_LAN_IP` too when set. No public
/// address means no media. An offer may have one audio m-line per seat in a room.
/// `TINYSERVER_MEDIA_EGRESS_KBPS` caps what media uploads to everyone, in kb/s.
fn media_config(room_max: usize) -> anyhow::Result<Option<sfu::Config>> {
    let Ok(public) = env_var("TINYSERVER_PUBLIC_IP") else {
        return Ok(None);
    };
    let public_ip: IpAddr = public
        .parse()
        .context("TINYSERVER_PUBLIC_IP is not an IP address")?;
    if !public_ip.is_ipv4() {
        bail!("TINYSERVER_PUBLIC_IP must be IPv4 for now");
    }
    let port = match env_var("TINYSERVER_MEDIA_PORT") {
        Err(_) => DEFAULT_MEDIA_PORT,
        Ok(value) => value
            .parse::<u16>()
            .ok()
            .filter(|port| *port > 0)
            .context("TINYSERVER_MEDIA_PORT is not a port number")?,
    };
    let lan_ip = match env_var("TINYSERVER_LAN_IP") {
        Err(_) => None,
        Ok(value) => {
            let lan_ip: IpAddr = value
                .parse()
                .context("TINYSERVER_LAN_IP is not an IP address")?;
            if !lan_ip.is_ipv4() {
                bail!("TINYSERVER_LAN_IP must be IPv4 for now");
            }
            Some(lan_ip).filter(|lan_ip| *lan_ip != public_ip)
        }
    };
    let bind = SocketAddr::from((Ipv4Addr::UNSPECIFIED, port));
    let mut config = sfu::Config::new(bind, public_ip);
    config.lan_ip = lan_ip;
    config.offer_audio = room_max;
    config.egress_kbps = match env_var("TINYSERVER_MEDIA_EGRESS_KBPS") {
        Err(_) => None,
        Ok(value) => Some(
            value
                .parse::<u32>()
                .ok()
                .filter(|kbps| *kbps > 0)
                .context("TINYSERVER_MEDIA_EGRESS_KBPS is not a positive whole number")?,
        ),
    };
    Ok(Some(config))
}

const DEFAULT_MEDIA_PORT: u16 = 50000;

/// How often the process checks that the SFU's thread is alive.
const MEDIA_CHECK_EVERY: Duration = Duration::from_secs(1);
/// How long live sockets get to take their close frame on shutdown.
const DRAIN: Duration = Duration::from_secs(5);

const SWEEP_EVERY: Duration = Duration::from_secs(10 * 60);

/// Deletes expired sessions, invites, idempotency keys, sign-ins, one-time codes, and old
/// outbox rows. The first sweep
/// runs at start.
async fn sweep_every(pool: PgPool, period: Duration) {
    let mut ticks = tokio::time::interval(period);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        match db::sweep(&pool).await {
            Ok(swept) if swept.total() > 0 => tracing::info!(
                sessions = swept.sessions,
                invites = swept.invites,
                idempotency_keys = swept.idempotency_keys,
                events = swept.events,
                logins = swept.logins,
                reports = swept.reports,
                "expired rows deleted"
            ),
            Ok(_) => {}
            Err(error) => tracing::warn!(%error, "sweep failed"),
        }
    }
}

/// Ends the process once the SFU has stopped by itself. `/health` would say 503 and every
/// call would stay down until someone noticed; the container's restart policy brings it back.
async fn exit_if_media_fails(live: api::Live) {
    let mut ticks = tokio::time::interval(MEDIA_CHECK_EVERY);
    loop {
        ticks.tick().await;
        if live.media_failed() {
            tracing::error!("media stopped by itself; exiting");
            std::process::exit(1);
        }
    }
}

/// On a signal, live sockets get `1012 restart` first, so graceful shutdown is not held open
/// by them.
async fn axum_serve(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    live: api::Live,
) -> anyhow::Result<()> {
    // Connect info gives the sign-in limiter the peer it is really talking to.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown().await;
        live.close();
    })
    .await
    .context("server stopped")?;
    Ok(())
}

/// Resolves on SIGINT or SIGTERM. `docker stop` sends SIGTERM to PID 1.
async fn shutdown() {
    let interrupt = async {
        if tokio::signal::ctrl_c().await.is_err() {
            tracing::warn!("failed to listen for ctrl-c");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(error) => {
                tracing::warn!(%error, "failed to listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => tracing::info!("interrupt received"),
        () = terminate => tracing::info!("terminate received"),
    }
}

/// An empty value counts as unset. Compose passes `${NAME:-}` through as empty.
/// A positive count from the environment, or `default` when unset.
fn env_count(name: &str, default: usize) -> anyhow::Result<usize> {
    match env_var(name) {
        Err(_) => Ok(default),
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|count| *count > 0)
            .with_context(|| format!("{name} is not a positive whole number")),
    }
}

fn env_var(name: &str) -> anyhow::Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{name} is not set"))
}

#[cfg(test)]
mod tests {
    use super::{join_code, join_url};

    #[test]
    fn owner_link_contains_the_compatible_invite_code() {
        assert_eq!(
            join_url("https://a.b/", "tok-en_1").as_deref(),
            Some("https://tinyclient.io/invite#tc1.68747470733a2f2f612e62.tok-en_1")
        );
        assert!(join_url("https://user@host", "token").is_none());
        assert!(join_url("https://host/path", "token").is_none());
        assert!(join_url("ftp://host", "token").is_none());
        assert!(join_url("https://host", "bad.token").is_none());
        assert!(join_url("http://[::1]:8080", "token").is_some());
    }

    #[test]
    fn an_owner_invite_prints_as_a_tinyclient_code() {
        // The same as tinyclient's `pack_join`: tc1, the origin in hex, the token.
        assert_eq!(
            join_code("https://a.b/", "tok-en_1").as_deref(),
            Some("tc1.68747470733a2f2f612e62.tok-en_1")
        );
        assert_eq!(join_code("  ", "t"), None);
    }
}
