# Backup and restore (Section H, Step 8.5)

**RPO target: UNSET. RTO target: UNSET.** Both are pending the operations owner's decision (and no operations
owner is assigned -- see `ALERTING.md`). The numbers below are what a drill *measured*, not commitments.

## What is backed up, how

| Step | Tool | Notes |
|---|---|---|
| Local, verified snapshot | `scripts/backup_trading_db.py` (`cosmicforge-db-backup.timer`) | SQLite online backup API, `quick_check`, SHA-256 manifest |
| Off-site, encrypted | `scripts/offsite_backup.py` (`cosmicforge-offsite-backup.timer`) | age (`BACKUP_ENCRYPTION_RECIPIENT`) or gpg (`BACKUP_GPG_RECIPIENT`); **refuses plaintext** |
| Platform PostgreSQL (when used) | `pg_dump -Fc` | rehearsed by the drill; no scheduled job exists yet |
| Drill | `scripts/restore_drill.py` | this document |
| Age monitoring | `app.ops.alerting.evaluate_backup_age` | `BACKUP_FAILURE` when the newest verified backup is missing or > 36 h old |

Manual restore steps for production remain in `docs/VPS_TRADING_DEPLOYMENT.md` ("Restore" in section 5 and the
quarterly restore drill in section 10.3).

## Secrets are never restored from a backup

The database holds broker credentials only as Fernet ciphertext (`broker_credentials*.encrypted_blob`), TOTP
secrets in the `enc:v2:` envelope, KYC PII as base64(Fernet), passwords and refresh tokens as hashes. The keys
that decrypt them are **not** in the database and are **not** in any backup:

| Key | Decrypts |
|---|---|
| `BROKER_SECRET_KEY` | broker API credentials |
| `CREDENTIAL_KEY` | TOTP / admin MFA secrets, versioned credentials |
| `KYC_ENCRYPTION_KEY` | KYC PII and documents |
| `SECRET_KEY` | session tokens (rotating it signs everyone out) |

Restoration procedure for secrets:

1. Restore the database file (or `pg_restore`) from the off-site copy as below.
2. Restore the keys from the secret manager / the offline copy kept per `VPS_TRADING_DEPLOYMENT.md` section
   10.3 into the service environment (`/etc/cosmicforge/*.env`, mode 0640) -- never from the backup, never by
   e-mail or chat.
3. Start the services; `assert_broker_encryption_configured` refuses to start in production without
   `BROKER_SECRET_KEY`.
4. Verify one credential decrypts (broker connection test from the admin console) before enabling trading.
5. If a key is lost, the matching ciphertext is unrecoverable: users must reconnect their exchange accounts and
   re-enrol 2FA. Do not "fix" this by restoring an older database.

The off-site `.env`-style configuration and `rclone.conf` are not part of the backup set; the drill refuses any
backup set that contains an env file or a plaintext credential.

## The drill

    python scripts/restore_drill.py --workdir <scratch> --out evidence/step8/restore_drill.json \
        [--source <sqlite file>] [--remote-dir <dir>] [--postgres-url postgresql://user@host:port/postgres]

1. backup with `backup_trading_db.py` (online, `quick_check`, manifest);
2. **secret scan** of the backup set -- every credential column must be ciphertext/hash/masked/empty; every text
   cell is checked for private keys, cloud/payment/platform/bot tokens, env-file lines, passwords in connection
   strings and `api_key=`-style assignments; any `.env` file in the set is refused. A finding stops the drill
   with exit 2 **before** anything is encrypted or uploaded, and the evidence names table/column/row, never
   the value;
3. encrypt: age/gpg through `offsite_backup.encrypt_command` when recipients (and, for age, a decryption
   identity `RESTORE_DRILL_AGE_IDENTITY`) are configured; otherwise **TEST-ONLY** Fernet with
   `RESTORE_DRILL_TEST_FERNET_KEY` or an ephemeral in-memory key. Also checks that `offsite_backup` still refuses
   to upload without a recipient;
4. "upload" to a local directory standing in for off-site storage; SHA-256 verified; no plaintext SQLite file
   may exist there;
5. restore from that copy into a fresh directory;
6. verify `integrity_check`, foreign keys, and counts + content hashes of **every** table against the backup
   snapshot and the source at backup time, with research-governance (`cati_family_*`, `cati_governance_*`,
   `cati_promotion_*`, `cati_holdout_registry`, `cati_experiment_registry`, `cati_certification_*`,
   `cati_adapter_cert_*`), account-configuration (`bot_instances`, `broker_accounts`, `broker_credentials*`) and
   trading-intent (`cati_execution_intents`, `platform_order_intents`) tables reported by name; credentials still
   ciphertext after restore and decryptable only with the key from the (simulated) secret manager;
7. PostgreSQL leg: platform schema in a throwaway database, rows written by the platform code, `pg_dump -Fc`,
   scan of the plain-SQL rendering, encrypt, "upload", `pg_restore` into a second throwaway database, per-table
   counts and hashes equal, both databases dropped;
8. RPO = newest data in the source now minus newest data in the restored backup (the synthetic source receives
   a write after the backup, so the measurement is exercised); RTO = remote fetch -> decrypt -> verified.

## Drill evidence (2026-10-17, workstation, synthetic source)

`evidence/step8/restore_drill.json`: **PASSED**, 20/20 checks. Synthetic representative database: 286 tables,
1,360 rows (5 per table where constraints allowed) built from the real schema; encryption `fernet-TEST-ONLY`
(ephemeral key; age not installed, no gpg recipient configured); PostgreSQL 18 leg PASSED (12 platform/ops
tables equal after `pg_restore`). Measured RPO 441 ms (one synthetic write after the backup), RTO 191 ms for
SQLite and 372 ms for PostgreSQL -- a few hundred rows on a local disk, so these say nothing about production
scale; they show the measurement works.

Not shown by this drill: a real off-site provider, real age/gpg keys, a production-sized database, or the
production server. The quarterly manual drill in `VPS_TRADING_DEPLOYMENT.md` section 10.3 remains required
once the server exists.
