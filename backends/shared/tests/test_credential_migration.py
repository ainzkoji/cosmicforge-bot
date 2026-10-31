import sys,os,sqlite3,json,base64
from pathlib import Path
sys.path.insert(0,str(Path('backends/shared').resolve()))
from shared_lib.broker.credential_migration import rotate
from cryptography.fernet import Fernet
import pytest

@pytest.fixture
def records(monkeypatch):
 monkeypatch.setenv('BROKER_SECRET_KEY',Fernet.generate_key().decode())
 monkeypatch.setenv('APP_ENV','production')
 monkeypatch.delenv('BROKER_LEGACY_KEY_DECRYPT',raising=False)
 c=sqlite3.connect(':memory:');c.row_factory=sqlite3.Row
 c.executescript('''
 CREATE TABLE broker_accounts(id TEXT PRIMARY KEY,user_id TEXT,broker_id TEXT,active_credential_version INTEGER,updated_at TEXT,last_error_code TEXT,last_error_message TEXT,validation_error TEXT);
 CREATE TABLE broker_credentials_v2(id INTEGER PRIMARY KEY,account_id TEXT,version INTEGER,status TEXT,encrypted_blob TEXT,key_metadata TEXT,created_at TEXT,updated_at TEXT,superseded_at TEXT,permissions_json TEXT,UNIQUE(account_id,version));
 CREATE TABLE broker_credentials(account_id TEXT PRIMARY KEY,encrypted_blob TEXT,key_metadata TEXT,updated_at TEXT);
 CREATE TABLE broker_audit_log(broker_account_id TEXT,user_id TEXT,event_type TEXT,details_json TEXT,timestamp_utc TEXT);
 INSERT INTO broker_accounts(id,user_id,broker_id,active_credential_version) VALUES('a','owner','binance',1);
 ''')
 token=Fernet(base64.urlsafe_b64encode(b'0'*32)).encrypt(json.dumps({'api_key':'fixture','api_secret':'fixture'}).encode()).decode()
 c.execute("INSERT INTO broker_credentials_v2(account_id,version,status,encrypted_blob,permissions_json) VALUES('a',1,'active',?,'{}')",(token,))
 c.execute("INSERT INTO broker_credentials VALUES('a',?,'legacy','')",(token,));c.commit()
 return c,token

def test_dry_run_rotation_version_audit_and_idempotency(records):
 c,old=records
 assert rotate(c)==[('a','LEGACY'),('a','LEGACY')]
 assert c.execute('SELECT encrypted_blob FROM broker_credentials').fetchone()[0]==old
 rotate(c,apply=True)
 assert c.execute('SELECT user_id,active_credential_version FROM broker_accounts').fetchone()[:]==('owner',2)
 assert c.execute('SELECT COUNT(*) FROM broker_audit_log').fetchone()[0]==1
 assert all(s=='PRIMARY' for _,s in rotate(c,apply=True))
 assert c.execute('SELECT COUNT(*) FROM broker_credentials_v2').fetchone()[0]==2
 assert c.execute('SELECT permissions_json FROM broker_credentials_v2 WHERE version=2').fetchone()[0]=='{}'

def test_unreadable_keeps_account_and_marks_reconnect(records):
 c,_=records;c.execute("UPDATE broker_credentials_v2 SET encrypted_blob='corrupt'");c.commit()
 rotate(c,apply=True)
 assert c.execute('SELECT user_id,active_credential_version,last_error_code FROM broker_accounts').fetchone()[:]==('owner',1,'CREDENTIAL_RECONNECT_REQUIRED')

def test_transaction_rolls_back_when_audit_fails(records):
 c,old=records
 c.execute("CREATE TRIGGER audit_fail BEFORE INSERT ON broker_audit_log BEGIN SELECT RAISE(ABORT,'fixture'); END");c.commit()
 with pytest.raises(sqlite3.IntegrityError):rotate(c,apply=True)
 assert c.execute('SELECT encrypted_blob FROM broker_credentials_v2').fetchone()[0]==old
 assert c.execute('SELECT active_credential_version FROM broker_accounts').fetchone()[0]==1


def test_production_refuses_a_weak_broker_credential_key(monkeypatch):
    """Section J: a short or placeholder BROKER_SECRET_KEY is refused in production, never used."""
    import pytest
    from cryptography.fernet import Fernet
    from shared_lib.core.security import broker_security as bs
    monkeypatch.setenv("APP_ENV", "production")
    for weak in ("a", "changeme", "CHANGE_ME_BEFORE_PRODUCTION", "x" * 31):
        monkeypatch.setenv("BROKER_SECRET_KEY", weak)
        with pytest.raises(bs.BrokerEncryptionConfigError):
            bs.encrypt_credentials({"api_key": "k"})
    monkeypatch.setenv("BROKER_SECRET_KEY", Fernet.generate_key().decode())
    assert bs.decrypt_credentials(bs.encrypt_credentials({"api_key": "k"}))["api_key"] == "k"
    monkeypatch.setenv("APP_ENV", "TEST")
    monkeypatch.setenv("BROKER_SECRET_KEY", "short")             # development keeps working
    assert bs.decrypt_credentials(bs.encrypt_credentials({"api_key": "k"}))["api_key"] == "k"


def test_production_never_falls_back_to_an_unnamed_database(monkeypatch):
    """Section K4: without a usable DATABASE_URL a production process refuses to open data/bot.db."""
    import pytest
    from shared_lib.persistence import db as dbmod
    monkeypatch.setattr("dotenv.load_dotenv", lambda *a, **k: False)
    monkeypatch.setenv("APP_ENV", "PRODUCTION")
    for value in ("", "mysql://somewhere/db", "bot.db"):
        monkeypatch.setenv("DATABASE_URL", value)
        with pytest.raises(RuntimeError, match="DATABASE_URL_REQUIRED_IN_PRODUCTION"):
            dbmod.DB()
