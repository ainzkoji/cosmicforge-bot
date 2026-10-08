# Fresh installation on one server (Section H, Step 1)

This guide installs the whole Step 1 system on one Linux server from an empty
database: the trading engine (CATI), the user backend, the admin backend and
the two web frontends, with Binance **demo** execution enabled and real-money
order submission **disabled**.

It complements `docs/VPS_TRADING_DEPLOYMENT.md`, which moves an existing
runtime and its database to a server. Where a topic is covered there in depth
(alerts, off-site backups, nginx internals, upgrade and rollback), this guide
gives the commands you need and points to the section.

PostgreSQL and containerised workers are not part of Step 1 (they belong to
Step 5). Everything here uses the existing architecture: three Python services
on one host sharing one SQLite file, behind nginx.

> **Read section 26 first.** It states exactly which parts of this guide have
> been executed and where. No part of it has been run on a real Linux server.

Conventions: service user `cosmicforge`, code in `/opt/cosmicforge/cosmicforge-bot`,
database in `/var/lib/cosmicforge`, backups in `/var/backups/cosmicforge`,
`APP.DOMAIN` and `ADMIN.DOMAIN` for the two public host names. Values written
`<like-this>` are placeholders. **This document contains no usable secret, and
none may be added to it.**

---

## 1. Server and operating system

| Item | Requirement |
| --- | --- |
| Operating system | Ubuntu 24.04 LTS, x86-64 (Python 3.12) |
| CPU / memory | 2 vCPU, 4 GB RAM |
| Disk | Local SSD, at least 60 GB. Never network storage: SQLite locking is not safe on NFS, SMB or object-store mounts |
| Region | One from which Binance answers (checked below) |
| Inbound ports | 22 (SSH), 80 and 443 (nginx). The three backends listen on 127.0.0.1 only |
| DNS | `APP.DOMAIN` and `ADMIN.DOMAIN` pointing at the server |

```bash
sudo apt-get update && sudo apt-get -y upgrade
sudo apt-get install -y python3 python3-venv python3-pip git sqlite3 libgomp1 tzdata ca-certificates curl chrony ufw \
    nginx certbot python3-certbot-nginx rsync

# Signed exchange requests are rejected when the clock drifts.
sudo systemctl enable --now chrony
timedatectl status | grep -E 'System clock synchronized|Time zone'

sudo ufw default deny incoming && sudo ufw default allow outgoing
sudo ufw allow OpenSSH && sudo ufw allow 'Nginx Full' && sudo ufw --force enable

# Both must print 200. Anything else means the region or provider is blocked.
curl -s -o /dev/null -w '%{http_code}\n' https://demo-fapi.binance.com/fapi/v1/ping
curl -s -o /dev/null -w '%{http_code}\n' https://fapi.binance.com/fapi/v1/ping
```

Node.js 20 or newer is needed only to build the frontends. It can be on this
server or on any other machine (section 15).

## 2. Service user and directories

```bash
sudo useradd --system --create-home --home-dir /opt/cosmicforge --shell /usr/sbin/nologin cosmicforge
sudo install -d -o cosmicforge -g cosmicforge -m 0750 /opt/cosmicforge
sudo install -d -o cosmicforge -g cosmicforge -m 0700 /var/lib/cosmicforge /var/lib/cosmicforge/kyc_uploads
sudo install -d -o cosmicforge -g cosmicforge -m 0700 /var/backups/cosmicforge
sudo install -d -o root -g cosmicforge -m 0750 /etc/cosmicforge
```

No service runs as root.

## 3. Code and the approved commit

Deploy a commit that has been reviewed and has passed CI, not whatever `main`
points at when you run the command. Record it.

```bash
sudo -u cosmicforge mkdir -p -m 700 /opt/cosmicforge/.ssh
sudo -u cosmicforge ssh-keygen -t ed25519 -N '' -f /opt/cosmicforge/.ssh/id_ed25519
sudo -u cosmicforge sh -c 'ssh-keyscan -t ed25519 github.com >> /opt/cosmicforge/.ssh/known_hosts'
sudo cat /opt/cosmicforge/.ssh/id_ed25519.pub      # add as a READ-ONLY deploy key on the repository

cd /opt/cosmicforge
sudo -u cosmicforge git clone git@github.com:ainzkoji/cosmicforge-bot.git
cd cosmicforge-bot
sudo -u cosmicforge git checkout <approved-commit-sha>
sudo -u cosmicforge git rev-parse HEAD | sudo -u cosmicforge tee /var/lib/cosmicforge/DEPLOYED_REVISION
```

## 4. Python dependencies

One virtualenv at `backends/venv` serves all three backends.
`deploy/constraints.txt` pins every package to the certified versions.

```bash
cd /opt/cosmicforge/cosmicforge-bot
sudo -u cosmicforge python3 -m venv backends/venv
sudo -u cosmicforge backends/venv/bin/pip install --upgrade pip setuptools wheel
sudo -u cosmicforge backends/venv/bin/pip install -r backends/bot-backend/requirements.txt -c deploy/constraints.txt
sudo -u cosmicforge backends/venv/bin/pip install -r backends/user-backend/requirements.txt -c deploy/constraints.txt
sudo -u cosmicforge backends/venv/bin/python -c "import uvicorn, fastapi, numpy, psutil; print(uvicorn.__version__)"
```

The admin backend has no requirements file of its own; it runs on the same
virtualenv.

## 5. The shared database

All three services use **one** SQLite file. This is the single most important
setting to get right: a service pointed at a different file sees no users, no
accounts and no bots, and reports nothing wrong.

```
DATABASE_URL=sqlite:////var/lib/cosmicforge/cosmicforge.db      # four slashes: an absolute path
SQLITE_SYNCHRONOUS=FULL                                          # trading backend
```

Set exactly this `DATABASE_URL` in all three environment files (section 7) and
confirm it after writing them:

```bash
sudo grep -H '^DATABASE_URL=' /opt/cosmicforge/cosmicforge-bot/backends/*/.env     # three identical lines
```

Rules: one host only (never point a backend on another machine at this file),
local disk only, one trading process (the runtime lease refuses a second one).

## 6. Database migrations

There is no separate migration step to remember. Each service applies the
versioned, additive migrations in `backends/shared/shared_lib/persistence/migrations.py`
when it starts, and running them again changes nothing.

To create and inspect the database before the first start:

```bash
cd /opt/cosmicforge/cosmicforge-bot
sudo -u cosmicforge env PYTHONPATH=backends/shared DATABASE_URL=sqlite:////var/lib/cosmicforge/cosmicforge.db \
  backends/venv/bin/python -c "from shared_lib.persistence.migrations import migrate; migrate('/var/lib/cosmicforge/cosmicforge.db')"
sudo chmod 600 /var/lib/cosmicforge/cosmicforge.db
sudo -u cosmicforge sqlite3 /var/lib/cosmicforge/cosmicforge.db 'PRAGMA quick_check; PRAGMA journal_mode;'    # ok / wal
sudo -u cosmicforge sqlite3 /var/lib/cosmicforge/cosmicforge.db \
  "SELECT name FROM sqlite_master WHERE name IN ('deployment_consents','account_equity_snapshots','user_events','notification_outbox');"
```

The last command must print all four Step 1 tables. Step 1 added columns to
`bot_instances` (`risk_profile_version`, `max_position_usdt`,
`risk_acknowledged_at`, `deploy_request_id`, `stopped_reason`) and the tables
`deployment_consents`, `account_equity_snapshots`, `account_equity_daily`,
`user_events` and `notification_outbox`. (`billing_operator_grants` is created
by the user backend the first time an administrator issues a grant.) Nothing is
dropped or rewritten, so older code can still read a migrated database.

## 7. Secrets, encryption keys and where they live

Generate every secret **on the server**, write it straight into the
environment file, and never paste one into a ticket, a chat or this repository.

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(48))"                                   # SECRET_KEY, CREDENTIAL_KEY, ENGINE_API_KEY
backends/venv/bin/python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # BROKER_SECRET_KEY
openssl rand -hex 32                                                                             # KYC_ENCRYPTION_KEY, KYC_URL_SECRET, TELEGRAM_WEBHOOK_SECRET
```

`BROKER_SECRET_KEY` must be a Fernet key (the second command); the others are
random strings of at least 32 characters. Generate each one separately: no two
keys share a value, except where the table below says "the same".

| Key | Trading backend | User backend | Admin backend | Notes |
| --- | --- | --- | --- | --- |
| `DATABASE_URL` | yes | yes | yes | identical in all three (section 5) |
| `SECRET_KEY` | yes | yes | yes | **the same value in all three**; at least 32 characters; the services refuse to start in production without it |
| `BROKER_SECRET_KEY` | yes | yes | no | **the same value in both**; encrypts the exchange API keys stored in the database. Losing or changing it makes every connected account unreadable |
| `CREDENTIAL_KEY` | no | yes | no | encrypts 2FA secrets; different from `SECRET_KEY` |
| `KYC_ENCRYPTION_KEY`, `KYC_URL_SECRET` | no | yes | no | required in production; keep stable and backed up |
| `TELEGRAM_WEBHOOK_SECRET` | no | yes | no | only if the Telegram webhook is used |
| `SUPERADMIN_EMAIL`, `SUPERADMIN_PASS` | no | first start only | no | section 8 |
| `SMTP_*` | optional | yes | no | section 9 |
| `STRIPE_*` | no | optional | no | section 11 |

Create the three files from their examples, owned by the service user and
readable by nobody else:

```bash
cd /opt/cosmicforge/cosmicforge-bot
for s in bot-backend user-backend admin-backend; do
  sudo -u cosmicforge install -m 600 backends/$s/.env.example backends/$s/.env
done
sudo -u cosmicforge nano backends/bot-backend/.env      # then user-backend, then admin-backend
```

For the trading backend, apply sections A, B and C of
`deploy/env.production.example` on top of `backends/bot-backend/.env.example`
(that file's header describes a migration; on a fresh install there is no older
`.env` to copy, so you generate `BROKER_SECRET_KEY` and `SECRET_KEY` here).

Back up the secrets separately from the database, in a password manager or a
secrets store. A database backup without `BROKER_SECRET_KEY` cannot decrypt the
connected exchange accounts.

Check without printing a value:

```bash
for s in bot-backend user-backend admin-backend; do
  sudo awk -F= -v s=$s '/^(SECRET_KEY|BROKER_SECRET_KEY|CREDENTIAL_KEY|KYC_ENCRYPTION_KEY)=/ {print s, $1, (length($2) >= 32 ? "set" : "MISSING OR SHORT")}' \
    /opt/cosmicforge/cosmicforge-bot/backends/$s/.env
done
sudo sh -c 'cd /opt/cosmicforge/cosmicforge-bot/backends && grep -h "^SECRET_KEY=" */.env | sort -u | wc -l'              # 1
sudo sh -c 'cd /opt/cosmicforge/cosmicforge-bot/backends && grep -h "^BROKER_SECRET_KEY=" bot-backend/.env user-backend/.env | sort -u | wc -l'   # 1
```

## 8. Administrator bootstrap

The first administrator is created by the user backend at start-up, only when
the `admins` table is empty, from two environment variables:

```
SUPERADMIN_EMAIL=<operator email>
SUPERADMIN_PASS=<a long generated passphrase>
```

Put them in `backends/user-backend/.env`, start the service once (section 19),
confirm the account, then **remove both lines** and restart:

```bash
journalctl -u cosmicforge-user-backend | grep BOOTSTRAP        # "Successfully created bootstrap admin account"
sudo -u cosmicforge sqlite3 /var/lib/cosmicforge/cosmicforge.db 'SELECT email, role, is_active FROM admins;'
sudo -u cosmicforge sed -i '/^SUPERADMIN_/d' /opt/cosmicforge/cosmicforge-bot/backends/user-backend/.env
sudo systemctl restart cosmicforge-user-backend
```

Sign in at `https://ADMIN.DOMAIN`, change the password and enable two-factor
authentication. A second start with the variables still present does nothing
(`Admins table is not empty`).

Administrator passwords that were ever committed to this repository's history
must be treated as public. The authorised operator rotates them; this guide
cannot do that for you.

## 9. Email (SMTP)

Customers cannot verify their address or reset a password without mail, and the
closing test starts with a verified account.

```
SMTP_HOST=<smtp host>
SMTP_PORT=587
SMTP_USER=<smtp user>
SMTP_PASSWORD=<smtp password>
SMTP_FROM=<sender address on a domain you control>
```

These go in `backends/user-backend/.env`. To have the engine email trading
events (section 23, customer notifications), put the same five lines in
`backends/bot-backend/.env` as well.

Registration answers `pending_verification` even when the mail server cannot be
reached, so test delivery explicitly: register a test address you own and check
that the code arrives. If it does not, look at `journalctl -u cosmicforge-user-backend`.

## 10. Binance demo accounts

Exchange credentials are **not** server configuration. Each customer connects
their own account in the portal (Dashboard, then Brokers), and the key is stored
encrypted with `BROKER_SECRET_KEY`. Leave `BINANCE_API_KEY` and
`BINANCE_API_SECRET` empty in the environment files.

For the Step 1 test, create a Binance **demo** futures API key, connect it in
the portal with environment **Demo**, and confirm the account shows as
connected. The engine then talks to `https://demo-fapi.binance.com` for that
account. Rules the software enforces:

* the environment is a property of the connected account, never of a request
  and never of a bot;
* a key with withdrawal permission is refused;
* a demo account is never sent to a live endpoint, and a live account cannot be
  deployed in Step 1 (`LIVE_NOT_AVAILABLE`).

## 11. Billing enforcement

```
BILLING_ENFORCED=false
```

Set it in `backends/bot-backend/.env` **and** `backends/user-backend/.env`
(both read it; they must agree). `false` is the default and the Step 1 setting:
plan entitlements block nothing on the demo path, so a new customer needs no
paid plan. With `true`, the existing plan rules apply; a bot on a demo exchange
account still needs no live-trading entitlement.

Stripe is optional while enforcement is off. Without the `STRIPE_*` keys,
checkout answers 503 "Billing is not configured". An administrator can grant a
plan for a limited time without Stripe:
`POST /api/admin/billing/grants` (admin console, audited, revocable).

## 12. Demo order submission

```
DEMO_ORDER_SUBMISSION_ENABLED=true
```

In `backends/bot-backend/.env`. With `false` the engine evaluates accounts and
records its decisions but sends nothing, and the portal shows
`BLOCKED_DEMO_ORDER_GATE`.

## 13. Live order submission: must stay off

```
LIVE_ORDER_SUBMISSION_ENABLED=false
```

In `backends/bot-backend/.env`. **Do not change this as part of any Step 1
deployment.** The health check fails when it is on (section 20). Confirm:

```bash
sudo grep -H '^LIVE_ORDER_SUBMISSION_ENABLED=' /opt/cosmicforge/cosmicforge-bot/backends/*/.env     # false everywhere it appears
```

## 14. Frontend build variables

The API address is compiled into each build. An empty value silently falls
back to `http://localhost:8000`, which only shows up later as a portal that
cannot log in.

| Frontend | Variable | Value |
| --- | --- | --- |
| user | `VITE_API_BASE` | `https://APP.DOMAIN` |
| user | `VITE_APP_ENV` | `PRODUCTION` |
| admin | `VITE_API_BASE` | `https://ADMIN.DOMAIN` |
| admin | `VITE_ADMIN_API_BASE` | `https://ADMIN.DOMAIN/admin-api` |
| admin | `VITE_USE_ADMIN_BACKEND_*` | as in `frontends/admin-frontend/.env.example` |

The customer portal reaches the engine only through the user backend. It has no
engine address of its own (`VITE_CATI_API_BASE` and `VITE_API_URL` are no longer
read by the user frontend).

## 15. Building the frontends

```bash
cd /opt/cosmicforge/cosmicforge-bot/frontends/user-frontend
npm ci && VITE_API_BASE=https://APP.DOMAIN VITE_APP_ENV=PRODUCTION npm run build

cd ../admin-frontend
npm ci && VITE_API_BASE=https://ADMIN.DOMAIN VITE_ADMIN_API_BASE=https://ADMIN.DOMAIN/admin-api npm run build

sudo install -d -o root -g root -m 0755 /var/www/cosmicforge/app /var/www/cosmicforge/admin
sudo rsync -a --delete /opt/cosmicforge/cosmicforge-bot/frontends/user-frontend/dist/  /var/www/cosmicforge/app/
sudo rsync -a --delete /opt/cosmicforge/cosmicforge-bot/frontends/admin-frontend/dist/ /var/www/cosmicforge/admin/
grep -rl "localhost:8000" /var/www/cosmicforge/app/assets | head -1      # must print nothing
```

nginx serves the copies. It is not given access to the checkout, which holds
the environment files.

## 16. The service processes

| Unit | What | Listens on | Workers |
| --- | --- | --- | --- |
| `cosmicforge-trading` | CATI engine: 30-second broker cycle, hourly collector, executions, the engine API | 127.0.0.1:9000 | 1 |
| `cosmicforge-user-backend` | accounts, sessions, onboarding, billing, KYC, the API of both frontends and the proxy to the engine | 127.0.0.1:8000 | 1 |
| `cosmicforge-admin-backend` | read-only reporting API of the admin console | 127.0.0.1:8100 | 1 |

Each is one uvicorn process, started from its own directory so it reads its own
`.env`. The trading unit is `Type=notify`: it reports ready only when it holds
the database lease and the CATI production task is running.

## 17. systemd

```bash
cd /opt/cosmicforge/cosmicforge-bot
sudo cp deploy/systemd/cosmicforge-*.service deploy/systemd/cosmicforge-*.timer /etc/systemd/system/
sudo mkdir -p /etc/systemd/journald.conf.d
sudo cp deploy/systemd/journald-cosmicforge.conf /etc/systemd/journald.conf.d/cosmicforge.conf
sudo systemctl restart systemd-journald
sudo systemctl daemon-reload
sudo systemctl enable cosmicforge-trading cosmicforge-user-backend cosmicforge-admin-backend
sudo systemctl enable --now cosmicforge-db-backup.timer cosmicforge-healthcheck.timer
```

Do not start the services yet (section 19).

## 18. nginx and HTTPS

```bash
sudo certbot certonly --nginx -d APP.DOMAIN -d ADMIN.DOMAIN
sudo cp deploy/nginx/cosmicforge-app.conf /etc/nginx/sites-available/cosmicforge-app.conf
sudo sed -i 's/admin\.example\.com/ADMIN.DOMAIN/g; s/app\.example\.com/APP.DOMAIN/g' /etc/nginx/sites-available/cosmicforge-app.conf
sudo ln -s /etc/nginx/sites-available/cosmicforge-app.conf /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
sudo systemctl enable --now certbot.timer
```

Set `FRONTEND_URL=https://APP.DOMAIN` in the user backend's `.env`, and
uncomment the `allow <office-ip>; deny all;` lines in the admin `server` block
so the admin console is reachable only from your own addresses. What the file
proxies and which headers it sets is described in
`docs/VPS_TRADING_DEPLOYMENT.md`, section 10.4.

## 19. Start-up order

```bash
sudo systemctl start cosmicforge-user-backend       # 1. creates the schema, bootstraps the administrator
curl -s http://127.0.0.1:8000/health; echo          #    "status":"healthy","database_reachable":true
sudo systemctl start cosmicforge-trading            # 2. returns when the engine reports ready (up to a minute)
sudo systemctl start cosmicforge-admin-backend      # 3.
curl -s http://127.0.0.1:8100/health; echo          #    "status":"ok"
sudo systemctl reload nginx                         # 4.
```

The user backend goes first on a fresh install so that the administrator exists
and you can fix an environment mistake before the engine is running. After a
reboot systemd starts all three; their order then does not matter, because each
applies the same idempotent migrations and the engine waits for nothing else.

## 20. Health and readiness

```bash
systemctl status cosmicforge-trading --no-pager           # active (running), Status: HEALTHY
sudo -u cosmicforge /opt/cosmicforge/cosmicforge-bot/backends/venv/bin/python \
    /opt/cosmicforge/cosmicforge-bot/scripts/vps_health_check.py \
    --expect-revision "$(cat /var/lib/cosmicforge/DEPLOYED_REVISION)"
```

Every line `OK`, last line `VERDICT HEALTHY`, exit status 0. On a fresh install
`cati_decision` turns `OK` after the first hourly boundary. From outside:

```bash
curl -sI https://APP.DOMAIN/ | grep -iE 'strict-transport|content-security|x-frame'    # all three present
curl -s  https://APP.DOMAIN/api/v1/auth/me; echo                                       # 401 from the user backend
curl -s -o /dev/null -w '%{http_code}\n' https://APP.DOMAIN/api/admin/users            # 404: not published on the customer host
```

### What each state means

These are different things, and "the process is up" proves only the first.

| State | How to tell | Where a customer sees it |
| --- | --- | --- |
| **Process alive** | `systemctl is-active cosmicforge-trading` | not shown |
| **Engine healthy** | `vps_health_check.py` verdict `HEALTHY`: lease held, cycle and collector not stalled | bot page: "Last engine cycle … ago"; older than two minutes is shown as stale, never as running |
| **Exchange connected** | account `status` is `SYNCED` in `GET /api/v1/cati/runtime/status`; `READ_FAILED` with a reason otherwise | reason on the bot page |
| **Demo execution enabled** | `DEMO_ORDER_SUBMISSION_ENABLED=true`; otherwise permission `BLOCKED_DEMO_ORDER_GATE` | reason on the bot page |
| **Bot eligible to enter** | bot status `eligibility.eligible_to_enter` is true (`GET /api/v1/cati/bots/{id}/status`) | "Able to open a trade when a signal qualifies" |
| **No current strategy signal** | permission `WAITING_SIGNAL`, reason `AWAITING_NATURAL_CATI_DECISION`. This is the normal resting state, not a fault | "No qualifying CATI decision is open right now." |
| **Blocked by a safety gate** | permission `BLOCKED_RISK` or `BLOCKED_ACCOUNT` with a reason code: kill switch, daily loss pause, drawdown limit, protection state unknown, unresolved order outcome | the reason and suggested action; severity "attention" or "critical" |

A bot shows `deploying` until the engine's first evaluation of its account,
then `running`, `paused` or `stopped`. A stopped bot stays stopped across
restarts until its owner starts it.

## 21. Database backups

```bash
sudo systemctl start cosmicforge-db-backup                       # run one now
journalctl -u cosmicforge-db-backup -n 20 --no-pager             # [DB_BACKUP] OK backup=... sha256=...
systemctl list-timers cosmicforge-db-backup.timer                # daily, 03:30 UTC
ls -lh /var/backups/cosmicforge/
```

The backup is one consistent snapshot taken while the services run, verified
with `PRAGMA quick_check`, compressed, with a SHA-256 manifest; the newest seven
are kept. Backups on the same disk do not survive the disk: set up the
encrypted off-site copy (`docs/VPS_TRADING_DEPLOYMENT.md`, section 10.3). Back
up `/var/lib/cosmicforge/kyc_uploads` and the secrets as well.

## 22. Restore

Restoring replaces the trading record with an older one. Everything written
after the backup is lost, so restore only when the current file is damaged.

```bash
sudo systemctl stop cosmicforge-trading cosmicforge-user-backend cosmicforge-admin-backend
cd /var/lib/cosmicforge
sudo -u cosmicforge mv cosmicforge.db cosmicforge.db.before-restore
sudo -u cosmicforge rm -f cosmicforge.db-wal cosmicforge.db-shm
sudo -u cosmicforge sh -c 'gunzip -c /var/backups/cosmicforge/cosmicforge-<timestamp>.db.gz > cosmicforge.db'
sudo chmod 600 cosmicforge.db
sudo -u cosmicforge sqlite3 cosmicforge.db 'PRAGMA quick_check;'      # ok
sudo systemctl start cosmicforge-user-backend cosmicforge-trading cosmicforge-admin-backend
```

On start the engine reconciles against the exchange. A position opened after
the backup is still on the exchange with its protective stop; the engine treats
it as account exposure and opens nothing new while it exists. The restored
database must be used with the `BROKER_SECRET_KEY` it was written with.

## 23. Failure alerts and customer notifications

**Operator alerts.** The units call `cosmicforge-alert@.service` when a service
crashes, a backup fails or the two-minute health check fails. Configure its
channels from `deploy/alerts.env.example` and test them
(`docs/VPS_TRADING_DEPLOYMENT.md`, sections 10.1 and 10.2). Until then a
failure is only written to the journal. Add a monitor outside the server: it is
the only thing that notices the server itself being down.

**Customer notifications.** The engine records each committed event (entry
filled, exit filled, protection needs attention, paused, resumed, stopped,
daily loss pause, entry blocked) and delivers it in the portal immediately and,
where the customer enabled a channel and it is configured, by email, Telegram
or push with bounded retries. Pending and failed deliveries are visible to the
operator:

```bash
sudo -u cosmicforge sqlite3 /var/lib/cosmicforge/cosmicforge.db \
  "SELECT channel, status, COUNT(*) FROM notification_outbox GROUP BY 1,2;"
```

A row stays `pending` while it is being retried and becomes `failed` after
eight attempts. It is never marked `sent` unless the channel accepted it.

## 24. Operational smoke test

Run it after every install and upgrade. It uses a Binance **demo** account and
moves no real money.

1. Register a new customer at `https://APP.DOMAIN`, receive the code by email, verify, sign in.
2. Complete the onboarding wizard. It ends on the deployment screen with the risk level and budget pre-filled. Nothing is deployed yet.
3. Brokers: connect the Binance demo key. It must show as connected, environment Demo.
4. Auto Pilot: choose the account, Balanced, a budget. The panel on the right shows the environment `DEMO`, the risk per trade, the daily loss pause and any blocker with its reason.
5. Tick the acknowledgement and deploy. You land on the bot page with status `Deploying`.
6. Within a minute the status becomes `Running` and the page says either that a trade can be opened when a signal qualifies, or why not.
7. Deploy a second bot on the same account: refused with "already has a running or paused bot".
8. Pause: the status becomes `Paused` and the activity list shows the event. Resume, then Stop.
9. `sudo systemctl restart cosmicforge-trading`: the stopped bot is still stopped.
10. Admin console: the bot is visible; engage the kill switch, confirm the bot page shows the block, release it.
11. `vps_health_check.py` still prints `VERDICT HEALTHY`.

An order on the demo exchange happens only when the strategy produces a
qualifying signal. Do not force one. Its absence within the test window is
recorded as "pending", not as a failure and not as a pass.

## 25. Safe restart, upgrade and rollback

**Restart.** `sudo systemctl restart cosmicforge-trading` is safe at any time:
the engine stops gracefully, releases its lease, and on start reconciles every
open position and unresolved order with the exchange before doing anything
else. Protective stops live on the exchange and are not affected by a restart.
Prefer a moment outside minutes :00 to :15 of the hour, when entries occur.

**Upgrade.**

```bash
cd /opt/cosmicforge/cosmicforge-bot
sudo systemctl start cosmicforge-db-backup                                           # 1. backup first
cat /var/lib/cosmicforge/DEPLOYED_REVISION | sudo -u cosmicforge tee /var/lib/cosmicforge/PREVIOUS_REVISION
sudo systemctl stop cosmicforge-trading cosmicforge-user-backend cosmicforge-admin-backend
sudo -u cosmicforge git fetch origin
sudo -u cosmicforge git checkout <approved-commit-sha>                               # 2. new code
sudo -u cosmicforge backends/venv/bin/pip install -r backends/bot-backend/requirements.txt -c deploy/constraints.txt
sudo -u cosmicforge backends/venv/bin/pip install -r backends/user-backend/requirements.txt -c deploy/constraints.txt
sudo cp deploy/systemd/cosmicforge-*.service deploy/systemd/cosmicforge-*.timer /etc/systemd/system/ && sudo systemctl daemon-reload
sudo -u cosmicforge git rev-parse HEAD | sudo -u cosmicforge tee /var/lib/cosmicforge/DEPLOYED_REVISION
sudo systemctl start cosmicforge-user-backend cosmicforge-trading cosmicforge-admin-backend   # 3. start, then section 20
```

Rebuild and copy the frontends (section 15) when they changed.

**Rollback.** Check out `PREVIOUS_REVISION` and start the services again.
Step 1's schema changes are additive, so the previous code runs against the
upgraded database and no restore is needed. Restore the pre-upgrade backup
(section 22) only if the data itself is wrong, and accept that everything
written since is lost. Details and the special cases:
`docs/VPS_TRADING_DEPLOYMENT.md`, section 6.2.

## 26. What has and has not been verified

**No part of this guide has been executed on a real Linux server.** No server
or SSH access existed when it was written (8 October 2026), and the machine it
was written on has no WSL distribution and no Docker.

Executed on 8 October 2026, on Windows, from a pristine `git archive` export of
the commit and an empty database, with throwaway secrets generated for the run:

| Section | What was run | Result |
| --- | --- | --- |
| 6 | the migration command, twice, with only `PYTHONPATH` and `DATABASE_URL` set | 193 tables, the Step 1 tables present, `quick_check` ok, WAL mode, second run changed nothing |
| 7, 8, 19 | the user backend started with the unit's uvicorn command in the production profile on 127.0.0.1 | `/health` 200 with `database_reachable: true`; one administrator created by the bootstrap; a second start did not create another |
| 20 | unauthenticated requests to `/api/v1/auth/me`, `/api/v1/cati/runtime/status`, `/api/admin/billing/grants`, `/api/onboarding/state` | 401 each |
| 9 | registration with the mail server unreachable | 200, account `pending_verification`; this is why section 9 says to test delivery |
| 21, 22 | `scripts/backup_trading_db.py --compress` and a restore of its output | verified backup with manifest; restored copy passes `quick_check` and holds the same rows |
| 15 | `npm run build` of the user frontend | builds |

Not executed anywhere, and to be treated as untested until someone runs them:

* every `apt-get`, `ufw`, `useradd`, `systemctl`, `journalctl`, `certbot` and `nginx` command;
* the three systemd units (`Type=notify` readiness, watchdog, `ExecStopPost` alerts) and the timers;
* the nginx site, TLS, the security headers and the admin address restriction;
* a start of the trading backend on an empty database in the production profile;
* the admin backend and the admin frontend build;
* SMTP delivery, Telegram and push;
* sections 10 and 24: no Binance demo credentials were available, so no exchange account was connected and no demo order was observed;
* section 25 as a whole.

One observation from the rehearsal to resolve before exposing the service: the
user backend answered `/docs` with 200 in the production profile. The provided
nginx site does not publish that path, so it is reachable only from the server
itself, but it should be confirmed and, if unwanted, disabled.

The first real execution of this guide is the Step 1 clean-install test
(closing scenario K). Record its outcome here when it is done.
