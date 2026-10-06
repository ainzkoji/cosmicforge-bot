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

**`SECRET_KEY` and `ENGINE_API_KEY`** have insecure built-in defaults. Trading
does not depend on them, but anything that can reach the API does. Before
putting the API behind a public proxy (section 7), set both to long random
values (`openssl rand -hex 32`) and set the same values in the user-backend.

## 3. Install and start the service

```bash
cd /opt/cosmicforge/cosmicforge-bot
sudo cp deploy/systemd/cosmicforge-trading.service /etc/systemd/system/
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

curl -s http://127.0.0.1:9000/health/runtime         # 200 only while the scheduler is healthy
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
reached. A minimal local check every five minutes that leaves a trace in the
journal:

```bash
echo '*/5 * * * * cosmicforge /opt/cosmicforge/cosmicforge-bot/backends/venv/bin/python /opt/cosmicforge/cosmicforge-bot/scripts/vps_health_check.py --json | logger -t cosmicforge-health' \
  | sudo tee /etc/cron.d/cosmicforge-health
journalctl -t cosmicforge-health -n 5 --no-pager
```

For alerting from outside, have an uptime monitor fetch `/health/runtime`
through the proxy from an allowed address: it returns 503 unless the trading
scheduler is healthy.

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

Backups on the same disk do not survive the disk. Copy them off the server:

```bash
rsync -a --remove-source-files /var/backups/cosmicforge/ BACKUP_HOST:/srv/cosmicforge-backups/   # example
```

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

1. Set non-default `SECRET_KEY` and `ENGINE_API_KEY` (section 2.3) and restart.
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
* **Off-site backups and external alerting.** The pieces are here (section 4 and
  5); where they are sent is yours to choose.
