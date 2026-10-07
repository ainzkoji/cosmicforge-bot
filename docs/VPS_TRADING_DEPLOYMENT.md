# Running the trading backend 24/7 on a VPS

This is the runbook for moving the CosmicForge trading backend from a
workstation to an Ubuntu server and keeping it running unattended.

```
Ubuntu VPS
  └─ systemd                       starts on boot, restarts on any exit
       └─ ONE uvicorn process      127.0.0.1:9000, no --reload, one worker
            └─ ONE CATI runtime    holds the database lease
                 └─ SQLite (WAL)   /var/lib/cosmicforge/cosmicforge.db
                      └─ Binance   account-scoped execution (DEMO gate on, LIVE gate off)
```

Nothing here changes what the bot trades or how it sizes. It changes where it
runs and what happens when the process, the host or the network fails.

## 0. What you need before you start

| Item | Requirement |
| --- | --- |
| Server | Ubuntu 24.04 LTS (Python 3.12), x86-64, 2 vCPU, 4 GB RAM |
| Disk | Local SSD, **not** network storage. Database size × 3, at least 60 GB (the database is ~8.4 GB today; a backup needs room for one uncompressed copy) |
| Region | One from which Binance is reachable (step 1.2 checks this) |
| From the current machine | the backend `.env`, including `BROKER_SECRET_KEY`, and a verified database backup |
| GitHub | read access to `ainzkoji/cosmicforge-bot` (a read-only deploy key is enough) |

Conventions used below: service user `cosmicforge`, code in
`/opt/cosmicforge/cosmicforge-bot`, database in `/var/lib/cosmicforge`,
backups in `/var/backups/cosmicforge`. Commands are run as a sudo-capable
login user unless they start with `sudo -u cosmicforge`.

> **Only one runtime may trade an account.** The database lease guarantees one
> scheduler *per database*. It cannot see a second machine with its own copy.
> Stop the workstation runtime for good (step 2.1) before the server starts.

## 1. Prepare the server

### 1.1 Packages, clock, firewall

```bash
sudo apt-get update && sudo apt-get -y upgrade
sudo apt-get install -y python3 python3-venv python3-pip git sqlite3 libgomp1 tzdata ca-certificates curl chrony ufw

# Signed broker requests are rejected when the clock drifts.
sudo systemctl enable --now chrony
timedatectl status | grep -E 'System clock synchronized|Time zone'

# Inbound: SSH only. The backend listens on 127.0.0.1 and needs no open port.
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow OpenSSH
sudo ufw --force enable
```

### 1.2 Confirm the venue is reachable from this server

Both must print `200`. Anything else (`451`, `403`, a timeout) means this
region or provider is blocked: choose another before going further.

```bash
curl -s -o /dev/null -w '%{http_code}\n' https://fapi.binance.com/fapi/v1/ping
curl -s -o /dev/null -w '%{http_code}\n' https://demo-fapi.binance.com/fapi/v1/ping
```

If the Binance API key restricts source addresses, add the server's public IP
(`curl -s https://api.ipify.org`) to the key's allow-list.

### 1.3 Service user and directories

```bash
sudo useradd --system --create-home --home-dir /opt/cosmicforge --shell /usr/sbin/nologin cosmicforge
sudo install -d -o cosmicforge -g cosmicforge -m 0750 /opt/cosmicforge
sudo install -d -o cosmicforge -g cosmicforge -m 0700 /var/lib/cosmicforge
sudo install -d -o cosmicforge -g cosmicforge -m 0700 /var/backups/cosmicforge
```

The service never runs as root.

### 1.4 Code

```bash
# Read-only deploy key for the repository (add the printed public key under
# GitHub → repository → Settings → Deploy keys; do not allow write access).
sudo -u cosmicforge mkdir -p -m 700 /opt/cosmicforge/.ssh
sudo -u cosmicforge ssh-keygen -t ed25519 -N '' -f /opt/cosmicforge/.ssh/id_ed25519
sudo -u cosmicforge sh -c 'ssh-keyscan -t ed25519 github.com >> /opt/cosmicforge/.ssh/known_hosts'
sudo cat /opt/cosmicforge/.ssh/id_ed25519.pub

cd /opt/cosmicforge
sudo -u cosmicforge git clone --branch main --depth 100 git@github.com:ainzkoji/cosmicforge-bot.git
cd cosmicforge-bot
sudo -u cosmicforge git checkout main
sudo -u cosmicforge git rev-parse HEAD        # the revision you are deploying
```

Later updates are `git pull` (section 6).

### 1.5 Python environment

The virtualenv lives at `backends/venv`, the same place the operator scripts
and tests expect it. `deploy/constraints.txt` pins every package to the version
the certified runtime uses, so the server does not get whatever is newest.

```bash
cd /opt/cosmicforge/cosmicforge-bot
sudo -u cosmicforge python3 -m venv backends/venv
sudo -u cosmicforge backends/venv/bin/pip install --upgrade pip setuptools wheel
sudo -u cosmicforge backends/venv/bin/pip install -r backends/bot-backend/requirements.txt -c deploy/constraints.txt
sudo -u cosmicforge backends/venv/bin/python -c "import uvicorn, fastapi, numpy, psutil; print(uvicorn.__version__)"
```

## 2. Move the state: database and environment

### 2.1 Stop the old runtime and take a verified backup (on the current machine)

```powershell
# Windows workstation, from the repository root
.\scripts\trading_runtime.ps1 stop
.\scripts\trading_runtime.ps1 status          # running : False, lease released
backends\venv\Scripts\python.exe scripts\backup_trading_db.py --output-dir C:\cosmicforge-migration --keep 3 --compress
```

The script prints the backup's file name and SHA-256 and writes them to
`<name>.json`. For the current 8.4 GB database it takes about ten minutes and
produces a 1.4 GB file. **Do not start the workstation runtime again after
this.**

Never copy `cosmicforge.db` itself while anything has it open: the committed
data still in `cosmicforge.db-wal` would be left behind. The backup script is
the only supported way to take a copy (section 5).

### 2.2 Transfer and restore the database (on the server)

```powershell
# from the workstation -- use the exact file name the backup printed
scp C:\cosmicforge-migration\cosmicforge-YYYYMMDDTHHMMSSZ.db.gz LOGIN@SERVER:/tmp/
scp C:\cosmicforge-migration\cosmicforge-YYYYMMDDTHHMMSSZ.db.gz.json LOGIN@SERVER:/tmp/
```

```bash
# on the server: verify the transfer, restore, verify the database
cd /tmp
sha256sum cosmicforge-*.db.gz
grep sha256 cosmicforge-*.db.gz.json                       # the two values must be identical

sudo sh -c 'gunzip -c /tmp/cosmicforge-*.db.gz > /var/lib/cosmicforge/cosmicforge.db'
sudo chown cosmicforge:cosmicforge /var/lib/cosmicforge/cosmicforge.db
sudo chmod 600 /var/lib/cosmicforge/cosmicforge.db
sudo -u cosmicforge sqlite3 /var/lib/cosmicforge/cosmicforge.db 'PRAGMA quick_check;'   # must print: ok
rm /tmp/cosmicforge-*.db.gz /tmp/cosmicforge-*.db.gz.json
```

Keep `/var/lib/cosmicforge` for the production database only.

### 2.3 Environment file and secrets

Copy the workstation's `backends/bot-backend/.env` — it already satisfies the
production profile — and change only what is server-specific.

```powershell
# from the workstation
scp backends\bot-backend\.env LOGIN@SERVER:/tmp/cosmicforge.env
```

```bash
# on the server
ENV=/opt/cosmicforge/cosmicforge-bot/backends/bot-backend/.env
sudo install -o cosmicforge -g cosmicforge -m 0600 /tmp/cosmicforge.env "$ENV"
shred -u /tmp/cosmicforge.env
sudo sed -i 's/\r$//' "$ENV"                                # strip Windows line endings

# Server-specific values (deploy/env.production.example explains each key)
sudo sed -i 's|^DATABASE_URL=.*|DATABASE_URL=sqlite:////var/lib/cosmicforge/cosmicforge.db|' "$ENV"
sudo grep -q '^SQLITE_SYNCHRONOUS=' "$ENV" || echo 'SQLITE_SYNCHRONOUS=FULL' | sudo tee -a "$ENV" >/dev/null

# Confirm, without printing any secret
sudo grep -E '^(DATABASE_URL|SQLITE_SYNCHRONOUS|APP_ENV|DATABASE_ROLE|DEMO_ORDER_SUBMISSION_ENABLED|LIVE_ORDER_SUBMISSION_ENABLED)=' "$ENV"
sudo grep -c '^BROKER_SECRET_KEY=.\+' "$ENV"                # must print: 1
sudo stat -c '%U:%G %a' "$ENV"                              # cosmicforge:cosmicforge 600
```

Expected: `DEMO_ORDER_SUBMISSION_ENABLED=true`, `LIVE_ORDER_SUBMISSION_ENABLED=false`.

**`BROKER_SECRET_KEY`** decrypts the broker credentials stored in the database.
It must be the exact value from the machine that encrypted them, which is why
the `.env` is copied rather than rewritten. The behaviour is fail-closed:

| Situation | Result |
| --- | --- |
| Key missing | the process refuses to start (`BROKER_SECRET_KEY is required in production`) |
| Key wrong | credentials cannot be decrypted; the account shows `READ_FAILED` / `BLOCKED_ACCOUNT`; nothing is traded |
| Key correct | `synced_accounts` equals `discovered_accounts` in the health check |

Never commit the `.env`, never paste the key into chat or a ticket, and keep an
offline copy: without it the stored credentials are unrecoverable and the
broker account must be reconnected.

**`SECRET_KEY`** signs and verifies every API token, and it is the only thing
that authenticates one service to another: the user-backend calls the trading
backend with the signed-in user's own JWT, or — for the admin emergency
controls — with a short-lived service JWT it signs with `SECRET_KEY`. The real
requirements are therefore:

* the trading backend listens on `127.0.0.1` only (the systemd unit binds it
  there; port 9000 is never opened), and
* the trading backend, the user-backend and the admin-backend share **one**
  strong `SECRET_KEY` (`python -c "import secrets; print(secrets.token_urlsafe(48))"`).
  In production each of them refuses to start with a missing, default or short one.

**`ENGINE_API_KEY` / `X-ENGINE-KEY` and `SERVICE_AUTH_TOKEN` protect nothing.**
The user-backend sends the `X-ENGINE-KEY` header, but no endpoint of the trading
backend checks it, and nothing reads `SERVICE_AUTH_TOKEN`. Do not count either
as a control when deciding what may reach port 9000.

## 3. Install and start the service

```bash
cd /opt/cosmicforge/cosmicforge-bot
sudo cp deploy/systemd/cosmicforge-trading.service 'deploy/systemd/cosmicforge-alert@.service' /etc/systemd/system/
sudo cp deploy/systemd/cosmicforge-db-backup.service deploy/systemd/cosmicforge-db-backup.timer /etc/systemd/system/
sudo mkdir -p /etc/systemd/journald.conf.d
sudo cp deploy/systemd/journald-cosmicforge.conf /etc/systemd/journald.conf.d/cosmicforge.conf
sudo systemctl restart systemd-journald

sudo systemctl daemon-reload
sudo systemctl enable cosmicforge-trading                 # start on every boot
sudo systemctl enable --now cosmicforge-db-backup.timer   # daily verified backup
sudo systemctl start cosmicforge-trading
```

`systemctl start` returns when the runtime reports **ready**, and ready means
"this process holds the database lease and the CATI production task is
running" — not merely "the port is open". Allow up to a minute.

The units send a failure alert through `cosmicforge-alert@.service`. Until its
channels are configured (section 10.1) a failure is only written to the
journal: do that next.

### 3.1 Verify

```bash
systemctl status cosmicforge-trading --no-pager           # active (running), Status: HEALTHY
sudo -u cosmicforge /opt/cosmicforge/cosmicforge-bot/backends/venv/bin/python \
    /opt/cosmicforge/cosmicforge-bot/scripts/vps_health_check.py \
    --expect-revision "$(sudo -u cosmicforge git -C /opt/cosmicforge/cosmicforge-bot rev-parse HEAD)"
```

Every line should be `OK` and the last line `VERDICT HEALTHY` (exit status 0).
`cati_decision` turns `OK` after the first hourly boundary following the start;
until then a fresh migration reports the age of the last decision made on the
old machine.

What a healthy start looks like in the journal:

```
[RUNTIME_BASELINE] code_revision=<the deployed commit> ...
[RUNTIME_SESSION] id=rts_... database_role=production path=/var/lib/cosmicforge/cosmicforge.db
[RUNTIME_SUPERVISOR] started interval_s=15 startup_grace_s=180 confirmations=4 systemd_notify=yes
[BACKGROUND_JOBS] BACKGROUND_JOBS_OWNER reason=RUNTIME_OWNER
[RUNTIME_SUPERVISOR] READY lease_owner_pid=... runtime_session_id=rts_...
```

## 4. Day-to-day operation

```bash
systemctl status cosmicforge-trading --no-pager      # state, since when, last log lines
sudo systemctl restart cosmicforge-trading           # graceful stop, then start
sudo systemctl stop cosmicforge-trading              # graceful stop; stays stopped until started
sudo systemctl start cosmicforge-trading

journalctl -u cosmicforge-trading -f                 # follow
journalctl -u cosmicforge-trading --since "1 hour ago" --no-pager
journalctl -u cosmicforge-trading -b -p warning      # this boot, warnings and worse
journalctl -u cosmicforge-trading | grep -E 'RUNTIME_SUPERVISOR|RUNTIME_OWNERSHIP|RUNTIME_SHUTDOWN'
journalctl -u cosmicforge-trading | grep CATI_RESIDUAL_PROSPECTIVE     # hourly decisions

curl -s -o /dev/null -w '%{http_code}\n' 'http://127.0.0.1:9000/health?ready=1'   # 200 only while the scheduler is healthy
python3 /opt/cosmicforge/cosmicforge-bot/scripts/vps_health_check.py   # exit 0 / 1 / 2
```

Why the last natural signal did or did not trade is recorded for every
evaluation, append-only, with the deepest reason:

```bash
sudo -u cosmicforge sqlite3 -readonly /var/lib/cosmicforge/cosmicforge.db \
  "SELECT datetime(observed_at/1000,'unixepoch'), stage, reason, execution_permission
     FROM cati_execution_evaluations ORDER BY observed_at DESC LIMIT 20;"
```

### What recovers by itself

| Event | What happens |
| --- | --- |
| Process crash, `kill -9`, out-of-memory | systemd restarts it in 10 s. The dead holder's lease is taken over at once; every durable order intent is read back from the broker before anything is sent; positions and their native stop/target are rediscovered. No order is submitted twice. |
| Server reboot | The service starts on boot. A lease left by the previous boot is recognised as dead even if the process ID is reused. |
| Trading loop stalls while the web server stays up (lease never taken, heartbeat stale, production task ended, cycle or collector stuck) | The runtime supervisor detects it within about a minute, stops new entries, releases the lease if it can and exits with status 70; systemd starts a fresh process. |
| Whole process frozen | No keep-alive reaches systemd for 180 s; it kills and restarts the process. |
| Binance or network outage, HTTP 429/5xx, timeouts | **No restart, and no blind retry of an order.** Reads fail closed and are retried on the next 30 s cycle; an order whose outcome is unknown stays unknown and is resolved by read-back only. The health check reports `broker_sync` failing; trading resumes when the venue answers. A decision whose hourly window passes during an outage is recorded as an explicit skip and is never traded late. |
| `systemctl stop` / restart / deploy | New entries stop immediately; the broker cycle in flight is allowed to finish (so a just-filled entry still gets its stop and target); the lease is released and the runtime session closed. Open positions are **not** closed: their protection lives on the exchange. |

### Monitoring

The process being active is not the test; `vps_health_check.py` is. Exit status
0 is healthy, 1 needs attention, 2 means the runtime cannot trade or cannot be
reached. `cosmicforge-healthcheck.timer` runs it every two minutes and sends an
alert when it reports 2 (section 10.2). It replaces the cron line earlier
versions of this guide installed: `sudo rm -f /etc/cron.d/cosmicforge-health`.

```bash
systemctl list-timers cosmicforge-healthcheck.timer
journalctl -u cosmicforge-healthcheck -n 25 --no-pager
```

A check that runs on the server cannot report that the server is gone. An
uptime monitor **outside** it is still required; section 10.2 shows how to
give one a readiness URL that answers 200 only while the trading scheduler is
healthy (the backend's `/health?ready=1`; plain `/health` answers 200 whenever
the web server is up).

## 5. Database: safety, backup, restore

The database is SQLite. That is appropriate here because of how it is used, and
only as long as these hold:

* **one writer process** — the single trading service (enforced by the lease and
  the startup preflight, which refuses a second process before it does anything);
* **local disk** — never NFS, SMB or an object-store mount, where SQLite's
  locking is unreliable;
* **WAL mode with a 10 s busy timeout** — set on every connection; readers
  (the API, the backup, `sqlite3 -readonly`) never block the writer;
* **`SQLITE_SYNCHRONOUS=FULL`** on the server — each commit is on disk before it
  returns, so the order intent written before a broker CREATE survives a host
  power loss, not only a process crash;
* **checkpoints** — automatic; connections are short-lived, so the WAL is
  folded back into the main file continuously and stays small.

### Growth

The execution-evaluation evidence is append-only: about 12 KB for each
30-second evaluation, roughly 40 MB a day (about 14 GB a year) on top of the
current 8.4 GB. A full disk stops every write, including the order intent that
must precede a broker CREATE, so the runtime watches it: the supervisor adds
`DATABASE_DISK_LOW` to its warnings below 2 GB free, and `vps_health_check.py`
warns below 5 GB and fails below 1 GB (`--min-free-gb`). Size the disk, and the
number of local backups kept (`--keep`), with that in mind.

### Backup

```bash
sudo systemctl start cosmicforge-db-backup                       # run one now
journalctl -u cosmicforge-db-backup -n 20 --no-pager             # [DB_BACKUP] OK backup=... sha256=...
systemctl list-timers cosmicforge-db-backup.timer                # next scheduled run (daily 03:30 UTC)
ls -lh /var/backups/cosmicforge/
```

The backup uses SQLite's online backup API: one consistent snapshot, including
commits still in the WAL, taken while the runtime keeps running. The copy is
verified with `PRAGMA quick_check` before it is kept, written as a single
self-contained file with a SHA-256 manifest, and the newest seven are retained.
A non-zero exit (2 could not copy, 3 failed verification, 4 retention) shows in
`systemctl status cosmicforge-db-backup`.

Backups on the same disk do not survive the disk, the server or the hosting
account. `cosmicforge-offsite-backup.timer` encrypts the newest verified backup
and uploads it every night: set it up as described in section 10.3.

### Restore

Restoring replaces the trading record with an older one. Everything written
after the backup — fills, reconciliation, durable order intents — is lost, so
restore only when the current file is damaged or a rollback requires it
(section 6.2).

```bash
sudo systemctl stop cosmicforge-trading
cd /var/lib/cosmicforge
sudo -u cosmicforge mv cosmicforge.db cosmicforge.db.before-restore
sudo -u cosmicforge rm -f cosmicforge.db-wal cosmicforge.db-shm        # they belong to the replaced file
sudo -u cosmicforge sh -c 'gunzip -c /var/backups/cosmicforge/cosmicforge-YYYYMMDDTHHMMSSZ.db.gz > cosmicforge.db'
sudo chmod 600 cosmicforge.db
sudo -u cosmicforge sqlite3 cosmicforge.db 'PRAGMA quick_check;'      # ok
sudo systemctl start cosmicforge-trading
python3 /opt/cosmicforge/cosmicforge-bot/scripts/vps_health_check.py
```

After a restore the runtime reconciles against the broker as on any start. A
position opened after the backup is still on the exchange with its protection;
the runtime sees it as account exposure and opens nothing new while it exists.

## 6. Upgrade and rollback

### 6.1 Upgrade to a newer commit

Do it outside a decision window (entries can occur from :00 to :15 of each
hour); an open position is not a reason to wait.

```bash
cd /opt/cosmicforge/cosmicforge-bot
sudo systemctl start cosmicforge-db-backup                                  # 1. backup first
sudo -u cosmicforge git rev-parse HEAD | sudo -u cosmicforge tee /var/lib/cosmicforge/PREVIOUS_REVISION

sudo systemctl stop cosmicforge-trading                                     # 2. graceful stop
sudo -u cosmicforge git fetch origin main
sudo -u cosmicforge git checkout main
sudo -u cosmicforge git pull --ff-only origin main                          # 3. new code
sudo -u cosmicforge backends/venv/bin/pip install -r backends/bot-backend/requirements.txt -c deploy/constraints.txt
sudo cp deploy/systemd/cosmicforge-*.service deploy/systemd/cosmicforge-*.timer /etc/systemd/system/
sudo systemctl daemon-reload

sudo systemctl start cosmicforge-trading                                    # 4. start, verify
python3 scripts/vps_health_check.py --expect-revision "$(sudo -u cosmicforge git rev-parse HEAD)"
```

Schema changes are applied by the application at startup and are additive.

### 6.2 Roll back to the previous commit

| | Revision |
| --- | --- |
| Current | the commit you deployed: `git -C /opt/cosmicforge/cosmicforge-bot rev-parse HEAD` |
| Previous on this server | `cat /var/lib/cosmicforge/PREVIOUS_REVISION` (written by step 6.1) |
| Last revision before 24/7 support | `b8e60749127c4f424eda5bbb5f9090e29d969743` (certified DEMO execution) |

```bash
cd /opt/cosmicforge/cosmicforge-bot
sudo systemctl stop cosmicforge-trading                                     # 1. stop
sudo -u cosmicforge git checkout "$(cat /var/lib/cosmicforge/PREVIOUS_REVISION)"   # 2. previous code
sudo systemctl start cosmicforge-trading                                    # 3. start
python3 scripts/vps_health_check.py                                         # 4. verify
```

The virtualenv is left as it is. Reinstall packages only if the upgrade you are
undoing changed them (`git diff --stat HEAD main -- deploy/constraints.txt
backends/bot-backend/requirements.txt` prints something), and then with the
constraints file of the revision you rolled back to.

**Do not restore the database as part of a rollback** unless the older code
cannot read it. Schema changes are additive (new tables and columns), which
older code ignores, so the normal rollback keeps the current database and loses
nothing. Restore the pre-upgrade backup (section 5) only if the rolled-back
runtime fails on the schema itself — and accept that it discards every record
written since that backup.

Rolling back to `b8e60749` or earlier needs one more step. Those revisions have
no runtime supervisor, so they never send the readiness and keep-alive
notifications the unit waits for, and systemd would restart them in a loop:

```bash
sudo systemctl edit cosmicforge-trading        # add the three lines below, save
#   [Service]
#   Type=simple
#   WatchdogSec=0
sudo systemctl daemon-reload && sudo systemctl restart cosmicforge-trading
```

Remove that override (`sudo systemctl revert cosmicforge-trading`) when you move
forward again.

## 7. Optional: HTTPS access to the API

Trading needs no inbound connection. Install a proxy only if the user
interface or the user-backend must reach this API from elsewhere.

1. Confirm the three services share one strong `SECRET_KEY` (section 2.3). It
   is what authenticates every request; `ENGINE_API_KEY` does not.
2. Point a DNS name at the server, then follow the header of
   `deploy/nginx/cosmicforge.conf` (certificate first, then the site).
3. `sudo ufw allow 'Nginx Full'`.

uvicorn keeps listening on `127.0.0.1:9000`. Port 9000 is never opened.

## 8. Acceptance tests on the server

Run these once after the first start, with no position open and outside a
decision window (:00 to :15). Each ends with `vps_health_check.py` reporting
`VERDICT HEALTHY` and exactly one unreleased lease. A, B and C were run on the
development machine before this guide was written; the systemd-specific parts
(boot start, `Restart=`, the watchdog) can only be exercised on the server.

```bash
hc()    { python3 /opt/cosmicforge/cosmicforge-bot/scripts/vps_health_check.py; }
lease() { sudo -u cosmicforge sqlite3 -readonly /var/lib/cosmicforge/cosmicforge.db \
            "SELECT pid, runtime_session_id, heartbeat_at, released_at FROM runtime_ownership WHERE released_at IS NULL;"; }

# A. Graceful restart: the old session closes, one new lease owner.
sudo systemctl restart cosmicforge-trading && sleep 45 && hc && lease
journalctl -u cosmicforge-trading --since "2 min ago" | grep -E 'RUNTIME_SHUTDOWN|RUNTIME_OWNERSHIP'
#   [RUNTIME_OWNERSHIP] released pid=... reason=APPLICATION_SHUTDOWN
#   [RUNTIME_SHUTDOWN] clean=True ... production_cycle_drained=True lease_released=True session_closed=True
#   [RUNTIME_OWNERSHIP] acquired pid=... took_over=NO

# B. Crash: no clean shutdown at all.
sudo systemctl kill -s SIGKILL cosmicforge-trading && sleep 60 && hc && lease
journalctl -u cosmicforge-trading --since "2 min ago" | grep -E 'RUNTIME_OWNERSHIP|RUNTIME_SUPERVISOR'
#   [RUNTIME_OWNERSHIP] acquired pid=... took_over=HOLDER_PID_GONE
#   [RUNTIME_SUPERVISOR] READY ...

# C. A second process must refuse before doing anything.
cd /opt/cosmicforge/cosmicforge-bot/backends/bot-backend
sudo -u cosmicforge ../venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 9001; echo "exit=$?"   # exit=1, "[COSMICFORGE_RUNTIME] ... already active"

# D. Reboot.
sudo reboot
#   ...log in again, define hc and lease again...
systemctl is-active cosmicforge-trading && hc && lease

# E. No duplicate orders were created by any of the above: this must print nothing.
sudo -u cosmicforge sqlite3 -readonly /var/lib/cosmicforge/cosmicforge.db \
  "SELECT trade_plan_id, COUNT(DISTINCT execution_attempt_id) AS attempts, COUNT(DISTINCT broker_order_id) AS orders
     FROM cati_execution_attempts GROUP BY trade_plan_id HAVING attempts > 1 OR orders > 1;"
```

## 9. Things that are deliberately not automatic

* **Real-money trading.** `LIVE_ORDER_SUBMISSION_ENABLED` is `false` and no step
  in this guide changes it.
* **The 15% stop ceiling.** A CATI selection whose structural stop is wider than
  15% of price is rejected (`CATI_STRUCTURAL_STOP_EXCEEDS_SYSTEM_MAX`); the bot
  does not fall through to the next-ranked symbol. Hours in which the top-ranked
  symbol is a very large mover therefore produce no trade by design.
* **Closing positions on shutdown.** A stop never flattens; protection is native
  to the exchange and survives the process.
* **Where alerts and off-site backups are sent.** The units and scripts are
  here (section 10), but they do nothing until you give them a channel, a
  destination and an encryption key. The external uptime monitor is a service
  you choose; nothing on this server can replace it.

## 10. Alerting, off-site backups and the web application

Sections 1 to 9 keep the trading runtime alive. This section makes sure a
person finds out when that fails (10.1, 10.2), that the database survives the
loss of the server (10.3), and adds the deployment of the rest of the product:
the user backend, the admin backend and the two frontends (10.4). Do 10.1 to
10.3 on every server that trades; 10.4 only where the web application runs.

All commands are run from the checkout:

```bash
cd /opt/cosmicforge/cosmicforge-bot
sudo install -d -m 0750 -o root -g cosmicforge /etc/cosmicforge     # settings files of this section
```

| File on the server | Template | Read by |
| --- | --- | --- |
| `/etc/cosmicforge/alerts.env` | `deploy/alerts.env.example` | `cosmicforge-alert@.service` |
| `/etc/cosmicforge/offsite-backup.env` | `deploy/offsite-backup.env.example` | `cosmicforge-offsite-backup.service` |
| `backends/user-backend/.env` | `backends/user-backend/.env.example` | the user backend |
| `backends/admin-backend/.env` | `backends/admin-backend/.env.example` | the admin backend |

The two files in `/etc/cosmicforge` are plain `KEY=value` lines (no `export`,
no comment after a value), mode `0640 root:cosmicforge`.

### 10.1 Failure alerts

`scripts/notify_failure.py` sends a short message — unit, host, time (UTC),
the unit's state and its last 20 journal lines — to every channel configured in
`alerts.env`: a Slack- or Discord-compatible webhook, Telegram, and/or email.
The message never contains a configuration value, and anything in the log
lines that looks like a key, token or password is masked.

```bash
sudo install -m 0640 -o root -g cosmicforge deploy/alerts.env.example /etc/cosmicforge/alerts.env
sudoedit /etc/cosmicforge/alerts.env                    # set at least one channel

sudo cp 'deploy/systemd/cosmicforge-alert@.service' /etc/systemd/system/
sudo cp deploy/systemd/cosmicforge-trading.service deploy/systemd/cosmicforge-db-backup.service /etc/systemd/system/
sudo systemctl daemon-reload                            # no restart of the runtime is needed
```

Test it, in this order:

```bash
# 1. The channels themselves. Exit status 0 and a message on every channel;
#    1 means a channel is missing or failed (the reason is printed, without secrets).
sudo -u cosmicforge python3 scripts/notify_failure.py --test

# 2. The unit systemd will start. A message "test failed" must arrive.
sudo systemctl start cosmicforge-alert@test.service
journalctl -u 'cosmicforge-alert@*' -n 10 --no-pager   # [ALERT] unit=test delivered=...

# 3. The real path (optional; this is acceptance test B of section 8).
sudo systemctl kill -s SIGKILL cosmicforge-trading      # an alert arrives within seconds; systemd restarts the runtime
```

How it is wired, and what to expect:

* `OnFailure=cosmicforge-alert@%n.service` is set on every unit of this
  deployment. It fires when a unit ends in the *failed* state — which is how
  the backup, off-site and health-check jobs report.
* A service that systemd restarts automatically never reaches that state, so
  the three long-running services also start the alert from `ExecStopPost=`
  whenever they end for any reason other than a clean stop: crash, supervisor
  exit (status 70), watchdog kill, out-of-memory, start or stop timeout.
  `systemctl stop` and `restart` send nothing.
* One alert per unit per 15 minutes (`--cooldown 900` in the alert unit); a
  crash loop does not send a message every ten seconds. To be told again at
  once after fixing something: `sudo rm /var/lib/cosmicforge-alert/*.last-alert`.
* If a channel is down the alert unit still succeeds and logs
  `channel=... FAILED`. Nothing watches the alert path itself, which is one
  more reason for the external monitor in 10.2.

### 10.2 Health check every two minutes, and the monitor outside

```bash
sudo cp deploy/systemd/cosmicforge-healthcheck.service deploy/systemd/cosmicforge-healthcheck.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cosmicforge-healthcheck.timer
sudo rm -f /etc/cron.d/cosmicforge-health               # the cron line of earlier versions of this guide

sudo systemctl start cosmicforge-healthcheck; journalctl -u cosmicforge-healthcheck -n 25 --no-pager   # VERDICT HEALTHY
```

The timer runs `scripts/vps_health_check.py`. `UNHEALTHY` (exit status 2: the
API does not answer, the lease is stale, the broker is not synced, no hourly
decision, under 1 GB of disk, the LIVE gate is on) fails the unit and sends an
alert containing the failing checks. `DEGRADED` (status 1) is recorded in the
journal but does not page; remove `SuccessExitStatus=1` from the unit if you
want it to. A stopped runtime is unhealthy, so pause the timer for planned
work: `sudo systemctl stop cosmicforge-healthcheck.timer`, and `start` it after.

**An external probe is still required.** Everything above runs on the server:
if the server is powered off, loses its network, or cannot reach your alert
channel, it says nothing. Use any uptime monitor that fetches a URL from
outside and alerts on a non-200 answer or a timeout.

By default nginx answers `/health` only to the server itself, and the health
report must not be public. To give the monitor a readiness URL, uncomment the
`location = /health/ready` and `location = /_cosmicforge_ready` blocks in
`/etc/nginx/sites-available/cosmicforge.conf` (they follow the `/health` line
of `deploy/nginx/cosmicforge.conf`), replace `203.0.113.10` with the monitor's
published probe addresses, and reload:

```bash
sudo nginx -t && sudo systemctl reload nginx
curl -s -o /dev/null -w '%{http_code}\n' https://trading.example.com/health/ready   # from elsewhere: 403
```

Point the monitor at `https://<host>/health/ready`. From its addresses the
answer is 200 while the backend's `/health?ready=1` is 200, and 500 while the
runtime is starting, stopping, failing or not answering; only the status code
leaves the server. Confirm it once by stopping the runtime and watching the
monitor turn red. (If only the web application's site is installed, put the
same two blocks into the customer `server` block of `cosmicforge-app.conf` and
write `cosmicforge_trading_backend` instead of `cosmicforge_backend`.)

### 10.3 Off-site backups

`scripts/offsite_backup.py` takes the newest backup that
`cosmicforge-db-backup` produced and verified, checks it against its SHA-256
manifest, encrypts it, uploads it and verifies the remote copy. It refuses to
upload unencrypted: the database holds broker credentials and identity data.

```bash
sudo apt-get install -y age rclone                      # or: gnupg and/or rsync

# Encryption key: generate it on ANOTHER machine and keep the private key
# there (and in a second safe place). Only the public key comes to the server.
#   age-keygen -o cosmicforge-backup.key     ->  "Public key: age1..."

sudo install -m 0640 -o root -g cosmicforge deploy/offsite-backup.env.example /etc/cosmicforge/offsite-backup.env
sudoedit /etc/cosmicforge/offsite-backup.env            # BACKUP_RCLONE_REMOTE and/or BACKUP_RSYNC_TARGET, BACKUP_ENCRYPTION_RECIPIENT
sudo -H -u cosmicforge rclone config                    # define the remote named in BACKUP_RCLONE_REMOTE

sudo -H -u cosmicforge python3 scripts/offsite_backup.py --dry-run     # selects and verifies, prints the commands, sends nothing

sudo cp deploy/systemd/cosmicforge-offsite-backup.service deploy/systemd/cosmicforge-offsite-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cosmicforge-offsite-backup.timer           # daily 04:30 UTC, one hour after the local backup
sudo systemctl start cosmicforge-offsite-backup                        # run one now
journalctl -u cosmicforge-offsite-backup -n 20 --no-pager              # [OFFSITE_BACKUP] OK destination=... verified=yes
```

Give the storage key write access to that one bucket and nothing else, and set
retention there (lifecycle rule or object lock): the job never deletes a remote
file, so a compromised server cannot erase the copies. A failed run — no fresh
local backup, checksum mismatch, encryption, upload or verification — exits
non-zero and sends an alert. The job covers the database only; if the user
backend stores KYC documents (`KYC_UPLOAD_DIR`), copy that directory off the
server as well, together with an offline copy of `KYC_ENCRYPTION_KEY`,
`BROKER_SECRET_KEY` and `CREDENTIAL_KEY`, without which a restored database
cannot be read.

**Restore drill, every quarter.** A backup nobody has restored is a hope. On a
machine that is *not* the server, with the private key:

```bash
rclone copy REMOTE:PATH/cosmicforge-YYYYMMDDTHHMMSSZ.db.gz.age .       # the newest one, and its manifest:
rclone copy REMOTE:PATH/cosmicforge-YYYYMMDDTHHMMSSZ.db.gz.json .
age --decrypt -i cosmicforge-backup.key -o drill.db.gz cosmicforge-*.db.gz.age     # gpg: gpg --output drill.db.gz --decrypt FILE.gpg
sha256sum drill.db.gz; grep sha256 cosmicforge-*.db.gz.json            # the two values must be identical
gunzip -c drill.db.gz > drill.db
sqlite3 drill.db 'PRAGMA quick_check;'                                 # must print: ok
sqlite3 drill.db "SELECT datetime(MAX(observed_at)/1000,'unixepoch') FROM cati_execution_evaluations;"   # close to the backup time
shred -u drill.db drill.db.gz 2>/dev/null || rm -f drill.db drill.db.gz
```

These are the first steps of the real restore in section 5 (which continues
with stopping the service and replacing the file). Write down the date, the
backup's name and the result; if any step fails, treat it as an outage of the
backup system, not as a failed exercise.

### 10.4 The web application

Three more pieces, all on this server, all reached only through nginx:

| Unit / site | What | Listens on |
| --- | --- | --- |
| `cosmicforge-user-backend.service` | accounts, sessions, billing, KYC, the API of both frontends | `127.0.0.1:8000` |
| `cosmicforge-admin-backend.service` | read-only reporting API of the admin console | `127.0.0.1:8100` |
| `deploy/nginx/cosmicforge-app.conf` | the two built frontends and the proxy in front of both backends | 80, 443 |

**Environment.** Each backend reads its own `.env` (mode 600, owned by
`cosmicforge`); start from its `.env.example`, which already carries the
production profile, and set at least:

| Key | user-backend | admin-backend |
| --- | --- | --- |
| `DATABASE_URL` | `sqlite:////var/lib/cosmicforge/cosmicforge.db` — the same file as the trading backend | the same |
| `SECRET_KEY` | the same value as the trading backend (section 2.3) | the same |
| `CREDENTIAL_KEY`, `BROKER_SECRET_KEY`, `KYC_ENCRYPTION_KEY`, `KYC_URL_SECRET` | required; `BROKER_SECRET_KEY` equal to the trading backend's | — |
| `ENGINE_URL` | `http://127.0.0.1:9000` | — |
| `FRONTEND_URL` | `https://APP.DOMAIN` | — |
| `SMTP_HOST`, `SMTP_USER`, `SMTP_PASSWORD` | required (verification and reset codes) | — |
| `KYC_UPLOAD_DIR` | a private directory outside the checkout, e.g. `/var/lib/cosmicforge/kyc_uploads` | — |
| `STRIPE_*`, `TELEGRAM_WEBHOOK_SECRET` | see `deploy/env.production.example`, sections D and G | — |

The user backend writes its own tables (accounts, sessions, KYC, billing) to
the same SQLite file as the trading runtime. That is supported on one host —
WAL mode, a 10 s busy timeout, and a busy database is answered with HTTP 503 —
and only there: never point a backend on another machine at this file.

```bash
sudo -u cosmicforge backends/venv/bin/pip install -r backends/user-backend/requirements.txt -c deploy/constraints.txt
sudo -u cosmicforge install -m 600 backends/user-backend/.env.example  backends/user-backend/.env     # then edit
sudo -u cosmicforge install -m 600 backends/admin-backend/.env.example backends/admin-backend/.env    # then edit
sudo -u cosmicforge install -d -m 700 /var/lib/cosmicforge/kyc_uploads

sudo cp deploy/systemd/cosmicforge-user-backend.service deploy/systemd/cosmicforge-admin-backend.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cosmicforge-user-backend cosmicforge-admin-backend
curl -s http://127.0.0.1:8000/health; echo              # "status":"healthy","database_reachable":true
curl -s http://127.0.0.1:8100/health; echo              # "status":"ok"
```

Both run one uvicorn worker bound to loopback, with the same restrictions as
the trading unit, and both alert on a crash (10.1). The user backend stays at
one worker on purpose: the reasons are in the unit file.

**Frontends and nginx.** The header of `deploy/nginx/cosmicforge-app.conf` has
the exact build and install commands. In short: build both frontends with the
public addresses compiled in, copy `dist/` to `/var/www/cosmicforge/{app,admin}`
(nginx is not given access to the checkout), obtain the certificate, enable
the site. What the file does:

* serves each frontend as a single-page application; the customer build's
  second entry (`cati.html`) is served as a file;
* proxies `/api/`, `/kyc/` and `/public/` to the user backend; on the admin
  host additionally `/admin-api/` to the admin backend (both backends answer
  under `/api/admin/`, so the console is built with
  `VITE_ADMIN_API_BASE=https://ADMIN.DOMAIN/admin-api`);
* does not publish the admin API on the customer host, so that the
  `allow <office-ip>; deny all;` lines in the admin `server` block — uncomment
  them — really restrict it;
* keeps the event stream (`/api/v1/events/stream`) unbuffered and open for an
  hour, and out of the access log (its token is in the query string);
* allows 11 MB request bodies on the KYC upload path only (the backend enforces
  10 MB), 2 MB elsewhere;
* limits credential endpoints to 30 requests a minute per address in addition
  to the backend's own limits, and the API to 20 a second;
* sends HSTS, `nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy` and a
  Content-Security-Policy whose few non-`'self'` entries are each explained in
  the file.

Check it from outside:

```bash
curl -sI https://APP.DOMAIN/ | grep -iE 'strict-transport|content-security|x-frame'    # all three present
curl -s  https://APP.DOMAIN/api/v1/auth/me; echo                                       # 401 JSON from the user backend
curl -s -o /dev/null -w '%{http_code}\n' https://APP.DOMAIN/api/admin/users            # 404: admin API is not on this host
curl -s -o /dev/null -w '%{http_code}\n' https://APP.DOMAIN/dashboard/bots             # 200: client-side route
curl -s -o /dev/null -w '%{http_code}\n' https://ADMIN.DOMAIN/                         # 403 from an address not listed, once allow/deny is active
```

Then sign in through the browser and watch its console once: a
Content-Security-Policy violation there means the application loads something
the policy does not list.

To update the frontends, rebuild and repeat the two `rsync` commands. To update
the backends, follow section 6.1 and restart `cosmicforge-user-backend` and
`cosmicforge-admin-backend` as well.

### 10.5 What pages you

| What happened | Noticed by | Alert names | Within |
| --- | --- | --- | --- |
| Trading process crashed, was killed by the watchdog or the kernel, exited with status 70, or timed out starting or stopping | systemd (`ExecStopPost=`) | `cosmicforge-trading.service` | seconds |
| Runtime is up but cannot trade: API not answering, lease stale, broker not synced, no hourly decision, disk nearly full, LIVE gate on | `cosmicforge-healthcheck.timer` | `cosmicforge-healthcheck.service` | 2 minutes |
| User backend or admin backend crashed | systemd (`ExecStopPost=`) | `cosmicforge-user-backend.service` / `cosmicforge-admin-backend.service` | seconds |
| Local database backup failed | `OnFailure=` | `cosmicforge-db-backup.service` | at the 03:30 UTC run |
| Off-site copy failed, or there was no fresh local backup to send | `OnFailure=` | `cosmicforge-offsite-backup.service` | at the 04:30 UTC run |
| Server down, network gone, nginx down, alert channel unreachable from the server | the **external** monitor only | — | the monitor's interval |

Not paged: `DEGRADED` health (kill switch engaged, disk getting low, start-up),
a clean `systemctl stop`, a repeat of the same unit's alert within 15 minutes,
and a venue outage shorter than the health check's limits (`broker_sync` fails
after 180 s without a sync). Look at
`journalctl -u 'cosmicforge-alert@*' --since '7 days ago'` now and then: a
`FAILED` there is an alert that reached nobody.
