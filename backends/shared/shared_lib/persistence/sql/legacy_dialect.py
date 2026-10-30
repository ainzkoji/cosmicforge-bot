"""SQLite -> PostgreSQL translation for the LEGACY persistence (EXT-DB-01, Step 5.1).

The legacy runtime (``shared_lib.persistence.db.DB``, ``migrations.migrate()``, ``production_schema.ensure`` and the
``*_schema`` modules) writes raw SQLite SQL. This module translates exactly the SQLite dialect that source uses into
PostgreSQL, statement by statement, at execution time. It is a CLOSED translator: a construct it does not know is
never passed through hoping PostgreSQL accepts it -- it raises :class:`DialectUnsupported` (and records it, so a
caller's ``except Exception`` cannot hide it from the test plugin).

The adapter that executes the result (catalog-dependent work: ``PRAGMA``, ``INSERT OR REPLACE``, trigger
installation, rowid/lastrowid, the legacy write lock) is :mod:`shared_lib.persistence.legacy_postgres`.

What is translated, and how (every rule is unit-tested in ``tests/test_legacy_dialect.py``):

=============================  ========================================================================================
SQLite                         PostgreSQL
=============================  ========================================================================================
``?``, ``?NNN``, ``:name``     ``%(pN)s`` / ``%(name)s`` (string literals respected, literal ``%`` doubled)
INSERT OR IGNORE               ``INSERT ... ON CONFLICT DO NOTHING`` (before RETURNING)
INSERT OR REPLACE / REPLACE    :class:`ReplacePlan` -- the adapter deletes every row that conflicts on ANY unique
                               constraint (PK included) and inserts, in one savepoint (SQLite's REPLACE semantics);
                               the ``VALUES(...)`` form without a column list uses the catalog column order
INSERT OR ABORT                plain INSERT (ABORT is the default); OR FAIL / OR ROLLBACK / UPDATE OR x: unsupported
ON CONFLICT(..) DO ...         passed through (a DO UPDATE without a conflict target is unsupported)
PRAGMA                         :class:`PragmaPlan` (table_info/index_list/index_info/user_version answered from the
                               catalog with SQLite's row shape; journal_mode, synchronous, busy_timeout, foreign_keys,
                               wal_checkpoint, query_only, page_size/page_count/freelist_count documented below)
sqlite_master / sqlite_schema  a compatibility VIEW installed by :data:`BOOTSTRAP_SQL` (type, name, tbl_name, rootpage,
                               sql) -- existence probes run unchanged
BEGIN [DEFERRED]               ``BEGIN ISOLATION LEVEL REPEATABLE READ`` (+ legacy write lock at the first write)
BEGIN IMMEDIATE / EXCLUSIVE    ``BEGIN`` + ``pg_advisory_xact_lock(LEGACY_WRITE_LOCK_KEY)`` (one writer, like SQLite)
COMMIT / END / ROLLBACK        same; SAVEPOINT / RELEASE / ROLLBACK TO passed through
rowid / _rowid_                ``cf_rowid``: every translated table carries it (an identity column, or a generated
                               alias of the INTEGER PRIMARY KEY); it is hidden from ``SELECT *`` results and PRAGMAs
last_insert_rowid(), changes() bound from the connection's own counters
datetime/date/time/julianday/  ``cf_datetime``/``cf_date``/``cf_time``/``cf_julianday``/``cf_strftime``/``cf_unixepoch``
strftime/unixepoch             -- PL/pgSQL ports of SQLite's date functions (time-value formats 1-12, modifiers
                               ``N days|hours|minutes|seconds|months|years``, ``start of day|month|year``,
                               ``unixepoch``, ``julianday``, ``auto``, ``weekday N``, ``subsec``; every strftime
                               specifier of SQLite 3.45 except the locale ones). ``'now'`` is the statement time
                               (``statement_timestamp()``), as SQLite fixes 'now' per statement.
CURRENT_TIMESTAMP/DATE/TIME    the same text SQLite returns (UTC ``YYYY-MM-DD HH:MM:SS``)
IFNULL, IIF                    COALESCE, CASE
GROUP_CONCAT(x [, sep])        ``string_agg(CAST(x AS TEXT), sep)`` (default ``','``; DISTINCT kept)
TOTAL(x)                       ``COALESCE(CAST(SUM(x) AS DOUBLE PRECISION), 0.0)``
MAX(a,b,..)/MIN(a,b,..)        GREATEST/LEAST, NULL if any argument is NULL (SQLite semantics; PG ignores NULLs)
ROUND(x[,n])                   numeric rounding (half away from zero, as SQLite), returned as DOUBLE PRECISION
INSTR(a,b)                     ``strpos(a, b)``
CAST(x AS INTEGER|REAL)        ``cf_cast_int``/``cf_cast_real`` (SQLite truncation and prefix parsing; PG rounds)
CAST(x AS BLOB)                ``CAST(x AS BYTEA)``
json_extract(x, path)          ``cf_json_extract`` (``$.a.b`` / ``$[n]`` paths; scalar results as text)
substr(x, n[, m])              passed through only for a positive literal start (identical semantics there)
LIKE / NOT LIKE                ILIKE ... ESCAPE '' (SQLite LIKE is case-insensitive and has no default escape; the
                               database is created with the C locale so ILIKE folds ASCII only, as SQLite does)
x IS y / x IS NOT y            IS NOT DISTINCT FROM / IS DISTINCT FROM (unless y is NULL/TRUE/FALSE)
==                             =
TRUE / FALSE                   1 / 0 (SQLite stores booleans as integers)
ORDER BY x [ASC|DESC]          + NULLS FIRST / NULLS LAST (SQLite sorts NULL lowest; PG sorts NULL highest)
IN ()                          ``IN (SELECT NULL WHERE FALSE)``
LIMIT a, b / LIMIT -1          ``LIMIT b OFFSET a`` / ``LIMIT ALL``
unaliased select expressions   aliased with their SQLite column name (``SELECT COUNT(*)`` -> column ``COUNT(*)``)
"Ident", [Ident], `Ident`      case-folded quoted identifiers (SQLite identifiers are case-insensitive)
CREATE TABLE                   type affinity mapping: INT -> BIGINT, ``INTEGER PRIMARY KEY [AUTOINCREMENT]`` -> BIGINT
                               identity, REAL -> ``legacy_money.PG_MONEY_TYPE`` (unconstrained NUMERIC) for the columns in
                               ``legacy_money.MONETARY_COLUMNS``
                               and DOUBLE PRECISION otherwise, BLOB -> BYTEA, TEXT/CHAR/CLOB/JSON/DATE* -> TEXT,
                               BOOLEAN -> BIGINT, NUMERIC/DECIMAL -> NUMERIC; DEFAULT and CHECK expressions
                               translated; REFERENCES clauses kept in ``sqlite_master.sql`` but NOT enforced, because
                               the legacy SQLite connections never enable ``foreign_keys`` (so no cascade/orphan
                               behaviour changes); ``cf_rowid`` appended.
ALTER TABLE ADD/RENAME/DROP    translated column definition; the stored ``sqlite_master.sql`` is kept in step
CREATE [UNIQUE] INDEX          expressions and partial ``WHERE`` translated
CREATE TRIGGER                 a PL/pgSQL function + trigger. ``SELECT RAISE(ABORT|FAIL|ROLLBACK, msg)`` becomes
                               ``RAISE EXCEPTION`` with SQLSTATE ``23CF1`` (an integrity error, which the adapter
                               raises as ``sqlite3.IntegrityError(msg)`` -- the same class and text as SQLite);
                               RAISE(IGNORE) skips the row; WHEN becomes ``IF (when) IS NOT TRUE THEN RETURN`` so
                               subqueries are allowed; a trigger whose body writes its own table does not re-fire
                               itself (SQLite's recursive_triggers=OFF).
CREATE VIEW [IF NOT EXISTS]    translated select
=============================  ========================================================================================

Not translated (raise :class:`DialectUnsupported`): GLOB, REGEXP, MATCH, ATTACH/DETACH, VACUUM, EXPLAIN, COLLATE,
``printf``/``format``, ``random()``, ``hex``/``quote`` and every function not in :data:`PASSTHROUGH_FUNCTIONS` or the
translation table, ``UPDATE ... LIMIT``, column conflict clauses (``UNIQUE ON CONFLICT REPLACE``).

Known, documented differences that are surfaced as errors rather than emulated: integer/text comparisons that SQLite
resolves by column affinity, ``x / 0`` (SQLite NULL, PostgreSQL error), non-aggregated bare columns in GROUP BY
queries, non-boolean WHERE operands (``WHERE flag``).
"""
from __future__ import annotations

import hashlib
import logging
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

#: ``pg_advisory_xact_lock`` key every legacy writer takes: legacy writers serialize like SQLite's single write
#: lock. ("CFLEGACY" as a 64-bit integer.)
LEGACY_WRITE_LOCK_KEY = 0x43464C4547414359
#: The hidden column that stands in for SQLite's rowid.
ROWID = "cf_rowid"
#: SQLSTATE raised by translated ``RAISE(ABORT, msg)``: class 23 (integrity constraint violation).
TRIGGER_ABORT_SQLSTATE = "23CF1"


class DialectUnsupported(RuntimeError):
    """A SQLite construct the translator does not handle. Never caught by ``except sqlite3.Error``."""

    code = "LEGACY_DIALECT_UNSUPPORTED"


_EVENTS: List[str] = []
_EVENTS_LOCK = threading.Lock()


def _unsupported(message: str, sql: str = "") -> DialectUnsupported:
    text = f"{message}" + (f" | SQL: {' '.join(sql.split())[:300]}" if sql else "")
    with _EVENTS_LOCK:
        if len(_EVENTS) < 10_000:
            _EVENTS.append(text)
    logger.error("[LEGACY_DIALECT_UNSUPPORTED] %s", text)
    return DialectUnsupported(text)


def unsupported_events() -> List[str]:
    """Every DialectUnsupported raised in this process (a caller may have swallowed the exception)."""
    with _EVENTS_LOCK:
        return list(_EVENTS)


# ═══════════════════════════════════════════════════════════════════════════════════════════════ tokenizer
class Tok:
    __slots__ = ("kind", "text", "ws")

    def __init__(self, kind: str, text: str, ws: str = " ") -> None:
        self.kind = kind   # id | kw-less identifier, qid, str, blob, num, param, op, semi
        self.text = text
        self.ws = ws

    @property
    def up(self) -> str:
        return self.text.upper() if self.kind == "id" else ""

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Tok({self.kind},{self.text!r})"


_TOKEN_RE = re.compile(r"""
    (?P<ws>\s+)
  | (?P<lcomment>--[^\n]*)
  | (?P<bcomment>/\*.*?(?:\*/|$))
  | (?P<blob>[xX]'[0-9a-fA-F]*')
  | (?P<str>'(?:[^']|'')*')
  | (?P<qid>"(?:[^"]|"")*"|`(?:[^`]|``)*`|\[[^\]]*\])
  | (?P<num>0[xX][0-9a-fA-F]+|(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)
  | (?P<param>\?\d*|[:@$][A-Za-z_][A-Za-z0-9_]*)
  | (?P<id>[A-Za-z_\x80-￿][A-Za-z0-9_$\x80-￿]*)
  | (?P<op>\|\||<=|>=|<>|!=|==|<<|>>|->>|->|[(),.;+\-*/%<>=&|~])
""", re.S | re.X)


def tokenize(sql: str) -> List[Tok]:
    toks: List[Tok] = []
    pos, ws = 0, ""
    n = len(sql)
    while pos < n:
        m = _TOKEN_RE.match(sql, pos)
        if not m:
            raise _unsupported(f"untokenizable character {sql[pos]!r}", sql)
        kind = m.lastgroup
        text = m.group()
        pos = m.end()
        if kind in ("ws", "lcomment", "bcomment"):
            ws = " "
            continue
        if kind == "op" and text == ";":
            kind = "semi"
        toks.append(Tok(kind, text, ws))
        ws = ""
    return toks


_WORDLIKE = frozenset({"id", "num", "param", "str", "qid", "raw", "blob"})


def _sep(prev: Optional[Tok], t: Tok) -> str:
    """The whitespace before ``t``: its own, or one space when two word-like tokens would otherwise fuse (tokens
    spliced together by a translation rule)."""
    if prev is None:
        return ""
    if not t.ws and prev.kind in _WORDLIKE and t.kind in _WORDLIKE:
        return " "
    return t.ws


def emit(toks: Sequence[Tok]) -> str:
    out = []
    prev = None
    for t in toks:
        out.append(_sep(prev, t) + t.text)
        prev = t
    return "".join(out)


def _matching(toks: Sequence[Tok], i: int) -> int:
    """Index of the ``)`` matching the ``(`` at ``i``."""
    depth = 0
    for j in range(i, len(toks)):
        t = toks[j]
        if t.kind == "op" and t.text == "(":
            depth += 1
        elif t.kind == "op" and t.text == ")":
            depth -= 1
            if depth == 0:
                return j
    raise _unsupported("unbalanced parentheses", emit(toks))


def _split_top(toks: Sequence[Tok], sep: str = ",") -> List[List[Tok]]:
    parts: List[List[Tok]] = [[]]
    depth = 0
    for t in toks:
        if t.kind == "op" and t.text == "(":
            depth += 1
        elif t.kind == "op" and t.text == ")":
            depth -= 1
        if depth == 0 and ((t.kind == "op" and t.text == sep) or (sep == ";" and t.kind == "semi")):
            parts.append([])
            continue
        parts[-1].append(t)
    return parts


def _is(t: Optional[Tok], *words: str) -> bool:
    return t is not None and t.kind == "id" and t.up in words


def _is_op(t: Optional[Tok], *ops: str) -> bool:
    return t is not None and t.kind == "op" and t.text in ops


def _ident_name(t: Tok) -> str:
    """The case-folded name of an identifier token (quoted or not)."""
    if t.kind == "id":
        return t.text.lower()
    if t.kind == "qid":
        body = t.text[1:-1]
        if t.text[0] == '"':
            body = body.replace('""', '"')
        elif t.text[0] == "`":
            body = body.replace("``", "`")
        return body.lower()
    if t.kind == "str":  # SQLite accepts 'name' where an identifier is expected in DDL
        return t.text[1:-1].replace("''", "'").lower()
    raise _unsupported(f"expected an identifier, got {t.text!r}")


def _qident(name: str) -> str:
    if re.fullmatch(r"[a-z_][a-z0-9_]*", name) and name.upper() not in _PG_RESERVED:
        return name
    return '"' + name.replace('"', '""') + '"'


_PG_RESERVED = frozenset("""ALL ANALYSE ANALYZE AND ANY ARRAY AS ASC ASYMMETRIC BOTH CASE CAST CHECK COLLATE COLUMN
CONSTRAINT CREATE CURRENT_CATALOG CURRENT_DATE CURRENT_ROLE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER DEFAULT
DEFERRABLE DESC DISTINCT DO ELSE END EXCEPT FALSE FETCH FOR FOREIGN FROM GRANT GROUP HAVING IN INITIALLY INTERSECT
INTO LATERAL LEADING LIMIT LOCALTIME LOCALTIMESTAMP NOT NULL OFFSET ON ONLY OR ORDER PLACING PRIMARY REFERENCES
RETURNING SELECT SESSION_USER SOME SYMMETRIC TABLE THEN TO TRAILING TRUE UNION UNIQUE USER USING VARIADIC WHEN
WHERE WINDOW WITH""".split())


def _id(text: str) -> Tok:
    return Tok("id", text)


def _op(text: str) -> Tok:
    return Tok("op", text)


def _raw(text: str) -> Tok:
    """Pre-translated SQL text (emitted verbatim apart from the ``%`` escaping)."""
    return Tok("raw", text)


# ═══════════════════════════════════════════════════════════════════════════════════════════════ results
@dataclass
class Translation:
    kind: str                         # query | dml | ddl | pragma | txn | replace | trigger | noop | view
    sql: Optional[str] = None         # PostgreSQL text with %(name)s placeholders
    param_names: Tuple[str, ...] = ()
    positional: bool = True
    writes: bool = False
    insert_table: Optional[str] = None
    insert_columns: Optional[Tuple[str, ...]] = None
    has_returning: bool = False
    is_select: bool = False
    plan: Any = None
    registry: List[tuple] = field(default_factory=list)
    catalog_change: bool = False
    needs_lastrowid: bool = False
    needs_changes: bool = False
    #: the table a DML statement writes and, for bare ``?`` values, which column each parameter is stored in
    #: (lets the adapter apply SQLite's REAL affinity: an int written to a REAL column is stored as a float)
    target_table: Optional[str] = None
    param_columns: Dict[str, str] = field(default_factory=dict)
    #: INSERT OR IGNORE: SQLite also skips a row that violates NOT NULL or CHECK (not only uniqueness)
    or_ignore: bool = False
    #: parameterless statements run right after a DDL statement, in the same batch
    post_sql: List[str] = field(default_factory=list)


@dataclass
class PragmaPlan:
    name: str
    arg: Optional[str] = None
    value: Optional[str] = None


@dataclass
class TxnPlan:
    action: str        # begin | begin_immediate | commit | rollback | savepoint | release | rollback_to
    name: Optional[str] = None


@dataclass
class ReplacePlan:
    table: str
    columns: Optional[Tuple[str, ...]]
    rows: Optional[List[str]] = None            # translated VALUES rows: each "expr, expr, ..." (no parens)
    rows_are_params: Optional[List[List[str]]] = None  # when every value is a bare parameter: their names
    select_sql: Optional[str] = None
    returning: Optional[str] = None


@dataclass
class RetypeMoney:
    table: str


@dataclass
class TriggerPlan:
    name: str
    table: str
    if_not_exists: bool
    function_name: str
    function_sql: str
    trigger_sql: str


@dataclass
class DropTriggerPlan:
    name: str
    if_exists: bool


@dataclass
class ViewPlan:
    name: str
    if_not_exists: bool
    sql: str


# ═══════════════════════════════════════════════════════════════════════════════════════════════ functions
#: Functions whose PostgreSQL meaning equals SQLite's for the argument types the legacy source passes.
PASSTHROUGH_FUNCTIONS = frozenset({
    "count", "sum", "avg", "abs", "coalesce", "nullif", "lower", "upper", "length", "replace", "trim", "ltrim",
    "rtrim", "exists", "row_number", "rank", "dense_rank", "lag", "lead", "first_value", "last_value", "ntile",
    "percent_rank", "cume_dist", "nth_value", "sign", "greatest", "least", "string_agg", "strpos", "btrim",
})

_DATE_FUNCS = {"datetime": "cf_datetime", "date": "cf_date", "time": "cf_time", "julianday": "cf_julianday",
               "unixepoch": "cf_unixepoch"}


class _Ctx:
    """Per-statement translation state."""

    def __init__(self, sql: str) -> None:
        self.sql = sql
        self.positional_count = 0
        self.named: List[str] = []
        self.mode: Optional[str] = None
        self.needs_lastrowid = False
        self.needs_changes = False

    def param(self, tok: Tok) -> Tok:
        text = tok.text
        if text == "?":
            if self.mode == "named":
                raise _unsupported("mixed positional and named parameters", self.sql)
            self.mode = "positional"
            name = f"p{self.positional_count}"
            self.positional_count += 1
        elif text.startswith("?"):
            if self.mode == "named":
                raise _unsupported("mixed positional and named parameters", self.sql)
            self.mode = "positional"
            idx = int(text[1:]) - 1
            if idx < 0:
                raise _unsupported("parameter ?0", self.sql)
            name = f"p{idx}"
            self.positional_count = max(self.positional_count, idx + 1)
        else:
            if self.mode == "positional":
                raise _unsupported("mixed positional and named parameters", self.sql)
            self.mode = "named"
            name = text[1:]
            if name not in self.named:
                self.named.append(name)
        return Tok("param", f"%({name})s", tok.ws)


def _cast_text(arg: List[Tok]) -> List[Tok]:
    return [_id("CAST"), _op("(")] + arg + [_id("AS"), _id("TEXT"), _op(")")]


def _paren(toks: List[Tok]) -> List[Tok]:
    return [_op("(")] + toks + [_op(")")]


def _join(parts: List[List[Tok]], sep: str = ",") -> List[Tok]:
    out: List[Tok] = []
    for i, p in enumerate(parts):
        if i:
            out.append(_op(sep))
        out.extend(p)
    return out


_TERMINATORS_ORDER = {"LIMIT", "OFFSET", "UNION", "EXCEPT", "INTERSECT", "WINDOW", "FOR", "RETURNING", "ON"}


def translate_expr(toks: Sequence[Tok], ctx: _Ctx) -> List[Tok]:
    """Translate a token run (expression, clause or whole DML statement body)."""
    toks = list(toks)
    out: List[Tok] = []
    case_stack: List[bool] = []
    i = 0
    n = len(toks)
    while i < n:
        t = toks[i]
        nxt = toks[i + 1] if i + 1 < n else None
        prev = out[-1] if out else None
        if t.kind == "param":
            after = toks[i + 2] if i + 2 < n else None
            after2 = toks[i + 3] if i + 3 < n else None
            if _is(nxt, "IS") and (_is(after, "NULL") or (_is(after, "NOT") and _is(after2, "NULL"))):
                # "? IS NULL": nothing gives an untyped parameter a type on PostgreSQL; SQLite needs none
                out.extend([Tok("id", "CAST", t.ws), _op("("), ctx.param(Tok("param", t.text, "")), _id("AS"),
                            _id("TEXT"), _op(")")])
            else:
                out.append(ctx.param(t))
            i += 1
            continue
        if t.kind == "qid":
            if t.text[0] in "`[":
                out.append(Tok("qid", '"' + _ident_name(t).replace('"', '""') + '"', t.ws))
            elif _is(prev, "AS"):
                out.append(t)
            else:
                out.append(Tok("qid", '"' + _ident_name(t).replace('"', '""') + '"', t.ws))
            i += 1
            continue
        if t.kind == "blob":
            out.append(Tok("raw", "'\\x" + t.text[2:-1] + "'::bytea", t.ws))
            i += 1
            continue
        if t.kind == "op" and t.text == "==":
            out.append(Tok("op", "=", t.ws))
            i += 1
            continue
        if t.kind != "id":
            out.append(t)
            i += 1
            continue
        up = t.up
        if up == "CASE":
            case_stack.append(_is(nxt, "WHEN"))  # searched CASE (conditions) vs simple CASE x WHEN value
        elif up == "END" and case_stack:
            case_stack.pop()
        elif up == "WHEN" and case_stack and case_stack[-1]:
            # a lone operand as a condition ("CASE WHEN ? THEN", "CASE WHEN flag THEN"): SQLite tests it for
            # non-zero; PostgreSQL needs a boolean
            j = i + 1
            if j < n and toks[j].kind in ("param", "id", "qid") and not (toks[j].kind == "id"
                                                                         and toks[j].up in _NOT_COLUMNS):
                k = j + 1
                if k + 1 < n and _is_op(toks[k], ".") and toks[k + 1].kind in ("id", "qid"):
                    k += 2
                if k < n and _is(toks[k], "THEN"):
                    operand = translate_expr(toks[j:k], ctx)
                    out.extend([t, _op("(")] + operand + [_op("<>"), Tok("num", "0"), _op(")")])
                    i = k
                    continue
        # ---- function call
        if _is_op(nxt, "(") and not _is(prev, "AS") and up not in ("IN", "VALUES", "EXISTS", "AS", "ON", "USING",
                                                                       "OVER", "FILTER", "AND", "OR", "NOT", "WHEN",
                                                                       "THEN", "ELSE", "SELECT", "WHERE", "FROM",
                                                                       "JOIN", "BY", "INTO", "SET", "RETURNING",
                                                                       "HAVING", "LIMIT", "OFFSET", "IS", "LIKE",
                                                                       "BETWEEN", "CASE", "DISTINCT", "ALL", "ANY",
                                                                       "UNION", "EXCEPT", "INTERSECT", "WITH",
                                                                       "RECURSIVE", "CONFLICT", "DO", "UPDATE",
                                                                       "TABLE", "UNIQUE", "CHECK", "KEY", "DEFAULT",
                                                                       "PRIMARY", "REFERENCES", "WINDOW", "PARTITION",
                                                                       "ORDER", "GROUP", "ESCAPE", "COLLATE"):
            if prev is not None and prev.kind == "op" and prev.text == ".":
                raise _unsupported(f"schema-qualified function {t.text}", ctx.sql)
            end = _matching(toks, i + 1)
            inner = toks[i + 2:end]
            repl = _function(t, inner, ctx)
            if repl and t.ws:
                repl[0] = _copy_ws(repl[0], t.ws)
            out.extend(repl)
            i = end + 1
            continue
        if up in ("ROWID", "_ROWID_"):
            if not _is(prev, "AS"):
                out.append(Tok("id", ROWID, t.ws))
                i += 1
                continue
        if up in ("TRUE", "FALSE") and not (_is(prev, "IS") or (_is(prev, "NOT") and len(out) > 1
                                                                   and _is(out[-2], "IS"))):
            out.append(Tok("num", "1" if up == "TRUE" else "0", t.ws))
            i += 1
            continue
        if up == "CURRENT_TIMESTAMP":
            out.append(Tok("raw", "to_char(statement_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')",
                           t.ws))
            i += 1
            continue
        if up == "CURRENT_DATE":
            out.append(Tok("raw", "to_char(statement_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD')", t.ws))
            i += 1
            continue
        if up == "CURRENT_TIME":
            out.append(Tok("raw", "to_char(statement_timestamp() AT TIME ZONE 'UTC', 'HH24:MI:SS')", t.ws))
            i += 1
            continue
        if up in ("GLOB", "REGEXP", "MATCH", "COLLATE"):
            raise _unsupported(f"{up} is not translated", ctx.sql)
        if up == "LIKE":
            out.append(Tok("id", "ILIKE", t.ws))
            # find the end of the pattern operand; add ESCAPE '' unless an ESCAPE follows
            j = i + 1
            depth = 0
            has_escape = False
            while j < n:
                tj = toks[j]
                if _is_op(tj, "("):
                    depth += 1
                elif _is_op(tj, ")"):
                    if depth == 0:
                        break
                    depth -= 1
                elif depth == 0 and (_is_op(tj, ",") or tj.kind == "semi"
                                     or _is(tj, "AND", "OR", "THEN", "ELSE", "END", "WHEN", "ORDER", "GROUP", "LIMIT",
                                            "HAVING", "UNION", "EXCEPT", "INTERSECT", "RETURNING", "ON", "WHERE",
                                            "FROM", "AS", "OFFSET", "WINDOW", "IS", "NOT", "LIKE", "IN", "BETWEEN")):
                    break
                elif depth == 0 and _is(tj, "ESCAPE"):
                    has_escape = True
                    break
                j += 1
            operand = translate_expr(toks[i + 1:j], ctx)
            out.extend(operand)
            if not has_escape:
                out.extend([_id("ESCAPE"), Tok("str", "''")])
            i = j
            continue
        if up == "IS":
            # IS [NOT] <expr>  ->  IS [NOT] DISTINCT FROM, unless NULL/TRUE/FALSE/DISTINCT follows
            j = i + 1
            negate = False
            if _is(toks[j] if j < n else None, "NOT"):
                negate = True
                j += 1
            target = toks[j] if j < n else None
            if target is None or _is(target, "NULL", "TRUE", "FALSE", "DISTINCT", "UNKNOWN"):
                out.append(t)
                i += 1
                continue
            out.extend([Tok("id", "IS", t.ws)] + ([_id("DISTINCT")] if negate else [_id("NOT"), _id("DISTINCT")])
                       + [_id("FROM")])
            i = j
            continue
        if up == "IN" and _is_op(nxt, "(") and i + 2 < n and _is_op(toks[i + 2], ")"):
            # x IN () is false and x NOT IN () true for every x (NULL included), as in SQLite; the empty array
            # literal takes x's type
            if out and _is(out[-1], "NOT"):
                out.pop()
                out.append(Tok("raw", "<> ALL ('{}')", t.ws))
            else:
                out.append(Tok("raw", "= ANY ('{}')", t.ws))
            i += 3
            continue
        if up == "LIMIT":
            # LIMIT a, b -> LIMIT b OFFSET a ; LIMIT -1 -> LIMIT ALL
            j = i + 1
            depth = 0
            k = j
            while k < n:
                tk = toks[k]
                if _is_op(tk, "("):
                    depth += 1
                elif _is_op(tk, ")"):
                    if depth == 0:
                        break
                    depth -= 1
                elif depth == 0 and (tk.kind == "semi" or _is(tk, "OFFSET", "UNION", "EXCEPT", "INTERSECT",
                                                               "RETURNING", "FOR")):
                    break
                k += 1
            parts = _split_top(toks[j:k])
            if len(parts) == 2:
                out.extend([t] + translate_expr(parts[1], ctx) + [_id("OFFSET")] + translate_expr(parts[0], ctx))
            else:
                body = parts[0]
                if len(body) == 2 and _is_op(body[0], "-") and body[1].kind == "num" and body[1].text == "1":
                    out.extend([t, _id("ALL")])
                else:
                    out.extend([t] + translate_expr(body, ctx))
            i = k
            continue
        if up == "ORDER" and _is(nxt, "BY"):
            out.extend([t, nxt])
            j = i + 2
            depth = 0
            k = j
            while k < n:
                tk = toks[k]
                if _is_op(tk, "("):
                    depth += 1
                elif _is_op(tk, ")"):
                    if depth == 0:
                        break
                    depth -= 1
                elif depth == 0 and (tk.kind == "semi" or tk.up in _TERMINATORS_ORDER or _is(tk, "ROWS", "RANGE",
                                                                                            "GROUPS")):
                    break
                k += 1
            items = _split_top(toks[j:k])
            new_items = []
            for item in items:
                if not item:
                    raise _unsupported("empty ORDER BY item", ctx.sql)
                if any(_is(x, "NULLS") for x in item):
                    new_items.append(translate_expr(item, ctx))
                    continue
                if any(_is(x, "COLLATE") for x in item):
                    raise _unsupported("COLLATE in ORDER BY", ctx.sql)
                desc = _is(item[-1], "DESC")
                new_items.append(translate_expr(item, ctx) + [_id("NULLS"), _id("LAST" if desc else "FIRST")])
            out.extend(_join(new_items))
            i = k
            continue
        out.append(t)
        i += 1
    return out


def _copy_ws(tok: Tok, ws: str) -> Tok:
    return Tok(tok.kind, tok.text, ws)


def _function(name_tok: Tok, inner: List[Tok], ctx: _Ctx) -> List[Tok]:
    name = name_tok.text.lower()
    if name == "cast":
        # CAST(expr AS type)
        as_idx = None
        depth = 0
        for k, x in enumerate(inner):
            if _is_op(x, "("):
                depth += 1
            elif _is_op(x, ")"):
                depth -= 1
            elif depth == 0 and _is(x, "AS"):
                as_idx = k
        if as_idx is None:
            raise _unsupported("CAST without AS", ctx.sql)
        expr = translate_expr(inner[:as_idx], ctx)
        type_text = " ".join(x.text.upper() for x in inner[as_idx + 1:])
        aff = _affinity(type_text)
        if aff == "INTEGER":
            return [_id("cf_cast_int"), _op("(")] + expr + [_op(")")]
        if aff == "REAL":
            return [_id("cf_cast_real"), _op("(")] + expr + [_op(")")]
        if aff == "TEXT":
            return [_id("CAST"), _op("(")] + expr + [_id("AS"), _id("TEXT"), _op(")")]
        if aff == "BLOB":
            return [_id("CAST"), _op("(")] + expr + [_id("AS"), _id("BYTEA"), _op(")")]
        if type_text in ("NUMERIC", "DECIMAL"):
            return [_id("CAST"), _op("(")] + expr + [_id("AS"), _id("NUMERIC"), _op(")")]
        raise _unsupported(f"CAST to {type_text}", ctx.sql)
    distinct = bool(inner) and _is(inner[0], "DISTINCT")
    args_toks = inner[1:] if distinct else inner
    if len(args_toks) == 1 and _is_op(args_toks[0], "*"):
        if name != "count":
            raise _unsupported(f"{name}(*)", ctx.sql)
        return [_id(name_tok.text), _op("("), _op("*"), _op(")")]
    raw_args = _split_top(args_toks) if args_toks else []
    if raw_args == [[]]:
        raw_args = []
    args = [translate_expr(a, ctx) for a in raw_args]
    head = [_id("DISTINCT")] if distinct else []

    def call(fname: str, a: List[List[Tok]], pre: List[Tok] = ()) -> List[Tok]:
        return [_id(fname), _op("(")] + list(pre) + _join(a) + [_op(")")]

    if name in ("max", "min") and len(args) >= 2:
        if distinct:
            raise _unsupported(f"{name}(DISTINCT a, b)", ctx.sql)
        null_test: List[Tok] = []
        for k, a in enumerate(args):
            if k:
                null_test.append(_id("OR"))
            null_test.extend(_paren(a) + [_id("IS"), _id("NULL")])
        fn = "GREATEST" if name == "max" else "LEAST"
        return ([_id("CASE"), _id("WHEN")] + null_test + [_id("THEN"), _id("NULL"), _id("ELSE")]
                + call(fn, [_paren(a) for a in args]) + [_id("END")])
    if name in ("max", "min"):
        if len(args) != 1:
            raise _unsupported(f"{name}() arity", ctx.sql)
        return call(name, args, head)
    if name in PASSTHROUGH_FUNCTIONS:
        if name == "trim" and len(args) == 2:
            return call("btrim", args)
        return call(name, args, head)
    if name == "ifnull":
        if len(args) != 2:
            raise _unsupported("ifnull arity", ctx.sql)
        return call("COALESCE", args)
    if name == "iif":
        if len(args) != 3:
            raise _unsupported("iif arity", ctx.sql)
        return ([_id("CASE"), _id("WHEN")] + args[0] + [_id("THEN")] + args[1] + [_id("ELSE")] + args[2]
                + [_id("END")])
    if name == "instr":
        if len(args) != 2:
            raise _unsupported("instr arity", ctx.sql)
        return call("strpos", args)
    if name == "group_concat":
        if not args or len(args) > 2:
            raise _unsupported("group_concat arity", ctx.sql)
        sep = args[1] if len(args) == 2 else [Tok("str", "','")]
        return [_id("string_agg"), _op("(")] + head + _cast_text(args[0]) + [_op(",")] + sep + [_op(")")]
    if name == "total":
        if len(args) != 1:
            raise _unsupported("total arity", ctx.sql)
        return ([_id("COALESCE"), _op("("), _id("CAST"), _op("(")] + call("SUM", args, head)
                + [_id("AS"), _id("DOUBLE"), _id("PRECISION"), _op(")"), _op(","), Tok("num", "0.0"), _op(")")])
    if name == "round":
        if distinct or len(args) not in (1, 2):
            raise _unsupported("round arity", ctx.sql)
        num = [_id("CAST"), _op("(")] + args[0] + [_id("AS"), _id("NUMERIC"), _op(")")]
        inner_call = [_id("ROUND"), _op("(")] + num
        if len(args) == 2:
            inner_call += [_op(","), _id("CAST"), _op("(")] + args[1] + [_id("AS"), _id("INTEGER"), _op(")")]
        inner_call += [_op(")")]
        return [_id("CAST"), _op("(")] + inner_call + [_id("AS"), _id("DOUBLE"), _id("PRECISION"), _op(")")]
    if name in _DATE_FUNCS:
        return call(_DATE_FUNCS[name], [_cast_text(a) for a in args])
    if name == "strftime":
        if not args:
            raise _unsupported("strftime without a format", ctx.sql)
        return call("cf_strftime", [_cast_text(a) for a in args])
    if name in ("substr", "substring"):
        if len(args) not in (2, 3) or not (len(raw_args[1]) == 1 and raw_args[1][0].kind == "num"
                                           and re.fullmatch(r"[1-9]\d*", raw_args[1][0].text)):
            raise _unsupported("substr with a start that is not a positive integer literal", ctx.sql)
        if len(args) == 3 and not (len(raw_args[2]) == 1 and raw_args[2][0].kind in ("num", "param")):
            raise _unsupported("substr with a computed length", ctx.sql)
        return call("substr", args)
    if name == "json_extract":
        if len(args) != 2:
            raise _unsupported("json_extract with more than one path", ctx.sql)
        return call("cf_json_extract", [_cast_text(args[0]), args[1]])
    if name == "last_insert_rowid":
        ctx.needs_lastrowid = True
        return [Tok("param", "%(cf_lastrowid)s")]
    if name == "changes":
        ctx.needs_changes = True
        return [Tok("param", "%(cf_changes)s")]
    if name in ("likely", "unlikely") and len(args) == 1:
        return _paren(args[0])
    if name == "likelihood" and len(args) == 2:
        return _paren(args[0])
    raise _unsupported(f"function {name}() is not translated", ctx.sql)


# ═══════════════════════════════════════════════════════════════════════════════════════════════ select lists
_SELECT_END = {"FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "EXCEPT", "INTERSECT", "WINDOW",
               "OFFSET", "RETURNING", "INTO", "ON", "FOR"}


def _alias_select_lists(toks: List[Tok]) -> List[Tok]:
    """Give every top-level unaliased select expression its SQLite column name, quote mixed-case aliases."""
    out: List[Tok] = []
    i = 0
    n = len(toks)
    depth = 0
    while i < n:
        t = toks[i]
        if _is_op(t, "("):
            depth += 1
        elif _is_op(t, ")"):
            depth -= 1
        if depth == 0 and _is(t, "SELECT"):
            out.append(t)
            j = i + 1
            if j < n and _is(toks[j], "DISTINCT", "ALL"):
                out.append(toks[j])
                j += 1
            d = 0
            k = j
            while k < n:
                tk = toks[k]
                if _is_op(tk, "("):
                    d += 1
                elif _is_op(tk, ")"):
                    if d == 0:
                        break
                    d -= 1
                elif d == 0 and (tk.kind == "semi" or (tk.kind == "id" and tk.up in _SELECT_END)):
                    break
                k += 1
            items = _split_top(toks[j:k])
            new_items: List[List[Tok]] = []
            for item in items:
                new_items.append(_alias_item(item))
            for idx, item in enumerate(new_items):
                if idx:
                    out.append(_op(","))
                out.extend(item)
            i = k
            continue
        out.append(t)
        i += 1
    return out


def _alias_item(item: List[Tok]) -> List[Tok]:
    if not item:
        return item
    last = item[-1]
    # explicit alias: "... AS name" or "expr name"
    if len(item) >= 2 and _is(item[-2], "AS") and last.kind in ("id", "qid", "str"):
        name = last.text if last.kind == "id" else _ident_name(last)
        if last.kind == "str" or (last.kind == "id" and name != name.lower()):
            return item[:-1] + [Tok("qid", '"' + name.replace('"', '""') + '"', last.ws)]
        return item
    if _is_op(last, "*"):
        return item
    if len(item) == 1 and item[0].kind in ("id", "qid"):
        if item[0].kind == "id" and item[0].up in ("ROWID", "_ROWID_"):
            return item + [_id("AS"), Tok("qid", '"' + item[0].text + '"')]
        return item
    if len(item) == 3 and item[0].kind in ("id", "qid") and _is_op(item[1], ".") and item[2].kind in ("id", "qid"):
        if item[2].kind == "id" and item[2].up in ("ROWID", "_ROWID_"):
            return item + [_id("AS"), Tok("qid", '"' + item[2].text + '"')]
        return item
    if (len(item) >= 2 and last.kind == "id" and last.up not in _SELECT_END and last.up not in ("END", "NULL")
            and not _is_op(item[-2], ".") and (item[-2].kind in ("id", "qid", "num", "str", "param")
                                                or _is_op(item[-2], ")"))
            and not _is(item[-2], "CASE", "WHEN", "THEN", "ELSE", "AND", "OR", "NOT", "IS", "DISTINCT")):
        # bare alias: "expr name"
        if last.text != last.text.lower():
            return item[:-1] + [Tok("qid", '"' + last.text + '"', last.ws)]
        return item
    name = emit(item).strip()
    if len(name.encode("utf-8")) > 63:
        return item
    return item + [_id("AS"), Tok("qid", '"' + name.replace('"', '""') + '"')]


# ═══════════════════════════════════════════════════════════════════════════════════════════════ statements
_CACHE: Dict[str, Translation] = {}
_CACHE_LOCK = threading.Lock()
_CACHE_MAX = 8192


def translate(sql: str) -> Translation:
    """Translate one SQLite statement. Cached by text (translation is pure)."""
    hit = _CACHE.get(sql)
    if hit is not None:
        return hit
    result = _translate(sql)
    with _CACHE_LOCK:
        if len(_CACHE) >= _CACHE_MAX:
            _CACHE.clear()
        _CACHE[sql] = result
    return result


def split_script(script: str) -> List[str]:
    """Split an ``executescript`` script into statements (trigger bodies kept whole)."""
    toks = tokenize(script)
    stmts: List[List[Tok]] = [[]]
    in_trigger = False
    begin_depth = 0
    case_depth = 0
    for t in toks:
        cur = stmts[-1]
        if not cur and _is(t, "CREATE"):
            in_trigger = False
        if cur and _is(cur[0], "CREATE") and _is(t, "TRIGGER") and len(cur) <= 3:
            in_trigger = True
        if in_trigger and _is(t, "BEGIN"):
            begin_depth += 1
        if in_trigger and _is(t, "CASE"):
            case_depth += 1
        elif in_trigger and _is(t, "END") and case_depth:
            case_depth -= 1
            cur.append(t)
            continue
        if in_trigger and _is(t, "END") and begin_depth:
            begin_depth -= 1
            cur.append(t)
            if begin_depth == 0:
                in_trigger = False
            continue
        if t.kind == "semi" and not (in_trigger and begin_depth):
            stmts.append([])
            continue
        cur.append(t)
    return [emit(s) for s in stmts if s]


def _finish(out: List[Tok], ctx: _Ctx, **kw: Any) -> Translation:
    sql = _render(out)
    if ctx.mode == "named":
        names: Tuple[str, ...] = tuple(ctx.named)
        positional = False
    else:
        names = tuple(f"p{k}" for k in range(ctx.positional_count))
        positional = True
    return Translation(sql=sql, param_names=names, positional=positional, needs_lastrowid=ctx.needs_lastrowid,
                       needs_changes=ctx.needs_changes, **kw)


def _render(toks: Sequence[Tok]) -> str:
    parts = []
    prev = None
    for t in toks:
        text = t.text if t.kind == "param" else t.text.replace("%", "%%")
        parts.append(_sep(prev, t) + text)
        prev = t
    return "".join(parts)


def _strip_semis(toks: List[Tok], sql: str) -> List[Tok]:
    while toks and toks[-1].kind == "semi":
        toks = toks[:-1]
    if any(t.kind == "semi" for t in toks):
        is_trigger = len(toks) > 2 and _is(toks[0], "CREATE") and (
            _is(toks[1], "TRIGGER") or (_is(toks[1], "TEMP", "TEMPORARY") and _is(toks[2], "TRIGGER")))
        if not is_trigger or len(split_script(sql)) > 1:
            raise _MultipleStatements(sql)
    return toks


class _MultipleStatements(Exception):
    pass


def _translate(sql: str) -> Translation:
    toks = tokenize(sql)
    try:
        toks = _strip_semis(toks, sql)
    except _MultipleStatements:
        return Translation(kind="multiple")
    if not toks:
        return Translation(kind="noop")
    head = toks[0]
    up = head.up
    ctx = _Ctx(sql)
    if up in ("SELECT", "VALUES") or (up == "WITH" and not _with_writes(toks)):
        out = translate_expr(_alias_select_lists(toks), ctx)
        return _finish(out, ctx, kind="query", is_select=True)
    if up == "WITH":
        out = translate_expr(toks, ctx)
        return _finish(out, ctx, kind="dml", writes=True)
    if up in ("INSERT", "REPLACE"):
        return _translate_insert(toks, ctx)
    if up == "UPDATE":
        if _is(toks[1] if len(toks) > 1 else None, "OR"):
            raise _unsupported(f"UPDATE OR {toks[2].text}", sql)
        _refuse_update_limit(toks, sql)
        out = translate_expr(_alias_returning(toks), ctx)
        tr = _finish(out, ctx, kind="dml", writes=True, has_returning=_has_top(toks, "RETURNING"))
        try:
            table, k = _table_name(toks, 1)
            if _is(toks[k], "SET"):
                tr.target_table = table
                _map_params(tr, _set_params(toks, k + 1))
        except (IndexError, DialectUnsupported):
            pass
        return tr
    if up == "DELETE":
        _refuse_update_limit(toks, sql)
        out = translate_expr(_alias_returning(toks), ctx)
        return _finish(out, ctx, kind="dml", writes=True, has_returning=_has_top(toks, "RETURNING"))
    if up == "PRAGMA":
        return _translate_pragma(toks, sql)
    if up in ("BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"):
        return _translate_txn(toks, sql)
    if up == "CREATE":
        return _translate_create(toks, ctx)
    if up == "ALTER":
        return _translate_alter(toks, ctx)
    if up == "DROP":
        return _translate_drop(toks, ctx)
    if up == "ANALYZE":
        if len(toks) == 1:
            return Translation(kind="ddl", sql="ANALYZE", writes=False)
        if len(toks) == 2:
            return Translation(kind="ddl", sql="ANALYZE " + _qident(_ident_name(toks[1])), writes=False)
    raise _unsupported(f"statement {up or head.text} is not translated", sql)


def _with_writes(toks: List[Tok]) -> bool:
    depth = 0
    for t in toks:
        if _is_op(t, "("):
            depth += 1
        elif _is_op(t, ")"):
            depth -= 1
        elif depth == 0 and _is(t, "INSERT", "UPDATE", "DELETE", "REPLACE"):
            return True
    return False


def _has_top(toks: List[Tok], word: str) -> bool:
    depth = 0
    for t in toks:
        if _is_op(t, "("):
            depth += 1
        elif _is_op(t, ")"):
            depth -= 1
        elif depth == 0 and _is(t, word):
            return True
    return False


def _alias_returning(toks: List[Tok]) -> List[Tok]:
    return toks


def _refuse_update_limit(toks: List[Tok], sql: str) -> None:
    depth = 0
    for t in toks:
        if _is_op(t, "("):
            depth += 1
        elif _is_op(t, ")"):
            depth -= 1
        elif depth == 0 and _is(t, "LIMIT", "ORDER"):
            raise _unsupported("UPDATE/DELETE with ORDER BY/LIMIT", sql)


def _table_name(toks: List[Tok], i: int) -> Tuple[str, int]:
    """Parse ``[schema.]name`` at ``i``; returns (name, next index). ``main.`` / ``temp.`` prefixes dropped."""
    name = _ident_name(toks[i])
    if i + 2 < len(toks) and _is_op(toks[i + 1], "."):
        schema = name
        if schema not in ("main", "temp"):
            raise _unsupported(f"schema {schema}")
        return _ident_name(toks[i + 2]), i + 3
    return name, i + 1


def _value_params(columns: Optional[Tuple[str, ...]], rest: List[Tok]) -> List[Tuple[int, str]]:
    """(positional index in the statement, column) for every bare ``?`` in ``VALUES (...)`` groups."""
    out: List[Tuple[int, str]] = []
    if not rest or not _is(rest[0], "VALUES"):
        return out
    k = 1
    counter = 0
    while k < len(rest) and _is_op(rest[k], "("):
        end = _matching(rest, k)
        for c_idx, expr in enumerate(_split_top(rest[k + 1:end])):
            if len(expr) == 1 and expr[0].kind == "param" and expr[0].text == "?":
                if columns is None:
                    out.append((counter, f"#{c_idx}"))  # no column list: the adapter resolves table order
                elif c_idx < len(columns):
                    out.append((counter, columns[c_idx]))
            counter += sum(1 for t in expr if t.kind == "param" and t.text == "?")
        k = end + 1
        if k < len(rest) and _is_op(rest[k], ","):
            k += 1
        else:
            break
    return out


def _set_params(toks: List[Tok], start: int) -> List[Tuple[int, str]]:
    """(positional index, column) for ``col = ?`` assignments of the SET clause starting at ``start``."""
    out: List[Tuple[int, str]] = []
    before = sum(1 for t in toks[:start] if t.kind == "param" and t.text == "?")
    depth = 0
    end = len(toks)
    for k in range(start, len(toks)):
        t = toks[k]
        if _is_op(t, "("):
            depth += 1
        elif _is_op(t, ")"):
            depth -= 1
        elif depth == 0 and _is(t, "WHERE", "FROM", "RETURNING"):
            end = k
            break
    counter = before
    for item in _split_top(toks[start:end]):
        bare = (len(item) == 3 and item[0].kind in ("id", "qid") and _is_op(item[1], "=")
                and item[2].kind == "param" and item[2].text == "?")
        # col = COALESCE(?, ...) / IFNULL(?, ...): the parameter is what gets stored when it is not NULL
        fallback = (len(item) >= 6 and item[0].kind in ("id", "qid") and _is_op(item[1], "=")
                    and _is(item[2], "COALESCE", "IFNULL") and _is_op(item[3], "(")
                    and item[4].kind == "param" and item[4].text == "?" and _is_op(item[5], ","))
        if bare or fallback:
            out.append((counter, _ident_name(item[0])))
        counter += sum(1 for t in item if t.kind == "param" and t.text == "?")
    return out


def _upsert_params(rest: List[Tok]) -> List[Tuple[int, str]]:
    depth = 0
    for k, t in enumerate(rest):
        if _is_op(t, "("):
            depth += 1
        elif _is_op(t, ")"):
            depth -= 1
        elif depth == 0 and _is(t, "SET") and k >= 2 and _is(rest[k - 1], "UPDATE") and _is(rest[k - 2], "DO"):
            base = sum(1 for x in rest[:k] if x.kind == "param" and x.text == "?")
            return [(base + i - sum(1 for x in rest[:k] if x.kind == "param" and x.text == "?"), c)
                    for i, c in _set_params(rest, k + 1)]
    return []


def _map_params(tr: Translation, pairs: List[Tuple[int, str]]) -> None:
    if not tr.positional:
        return
    for index, column in pairs:
        if index < len(tr.param_names):
            tr.param_columns[f"p{index}"] = column


_NOT_COLUMNS = frozenset("""SELECT FROM WHERE AND OR NOT NULL IS IN LIKE ILIKE ESCAPE CASE WHEN THEN ELSE END DISTINCT
BETWEEN EXISTS TRUE FALSE AS CAST INTEGER TEXT REAL NUMERIC DOUBLE PRECISION BIGINT BYTEA NULLS FIRST LAST ASC DESC
ORDER BY LIMIT OFFSET ALL ANY SOME OVER PARTITION GROUP HAVING UNION EXCEPT INTERSECT RETURNING SET DO UPDATE
NOTHING CONFLICT ON EXCLUDED""".split())


def _qualify_upsert(toks: List[Tok], table: str) -> List[Tok]:
    """In ``ON CONFLICT ... DO UPDATE SET col = expr [WHERE cond]`` SQLite resolves a bare column to the existing
    row; PostgreSQL calls it ambiguous. Bare column references there are qualified with the target table."""
    start = None
    depth = 0
    for k, t in enumerate(toks):
        if _is_op(t, "("):
            depth += 1
        elif _is_op(t, ")"):
            depth -= 1
        elif depth == 0 and _is(t, "SET") and k >= 2 and _is(toks[k - 1], "UPDATE") and _is(toks[k - 2], "DO"):
            start = k + 1
            break
    if start is None:
        return toks
    out = list(toks[:start])
    depth = 0
    item_start = True
    sub_depth = None
    for k in range(start, len(toks)):
        t = toks[k]
        nxt = toks[k + 1] if k + 1 < len(toks) else None
        prev = toks[k - 1]
        if _is_op(t, "("):
            depth += 1
            if sub_depth is None and _is(nxt, "SELECT"):
                sub_depth = depth
        elif _is_op(t, ")"):
            if sub_depth == depth:
                sub_depth = None
            depth -= 1
        if depth == 0 and _is(t, "RETURNING"):
            out.extend(toks[k:])
            return out
        if depth == 0 and _is_op(t, ","):
            item_start = True
            out.append(t)
            continue
        if (t.kind == "id" and sub_depth is None and t.up not in _NOT_COLUMNS and not _is_op(nxt, "(")
                and not _is_op(nxt, ".") and not _is_op(prev, ".")
                and not (item_start and depth == 0 and _is_op(nxt, "="))):
            out.extend([Tok("id", _qident(table), t.ws), Tok("op", ".", ""), Tok("id", t.text, "")])
            item_start = False
            continue
        if not (t.kind == "id" and item_start):
            item_start = False if t.kind != "id" else item_start
        if _is(t, "WHERE") and depth == 0:
            item_start = False
        out.append(t)
    return out


def _translate_insert(toks: List[Tok], ctx: _Ctx) -> Translation:
    sql = ctx.sql
    i = 1
    action = None
    if toks[0].up == "REPLACE":
        action = "REPLACE"
    elif _is(toks[1], "OR"):
        action = toks[2].up
        i = 3
    if not _is(toks[i], "INTO"):
        raise _unsupported("INSERT without INTO", sql)
    table, i = _table_name(toks, i + 1)
    if _is(toks[i] if i < len(toks) else None, "AS"):
        raise _unsupported("INSERT table alias", sql)
    columns: Optional[Tuple[str, ...]] = None
    if i < len(toks) and _is_op(toks[i], "("):
        end = _matching(toks, i)
        columns = tuple(_ident_name(c[0]) for c in _split_top(toks[i + 1:end]))
        i = end + 1
    rest = toks[i:]
    has_returning = _has_top(rest, "RETURNING")
    if action in (None, "ABORT"):
        head = [Tok("id", "INSERT"), _id("INTO"), _id(_qident(table))]
        if columns is not None:
            head += _paren(_join([[_id(_qident(c))] for c in columns]))
        out = head + _qualify_upsert(translate_expr(_alias_select_lists(rest), ctx), table)
        tr = _finish(out, ctx, kind="dml", writes=True, insert_table=table, insert_columns=columns,
                     has_returning=has_returning, target_table=table)
        _map_params(tr, _value_params(columns, rest) + _upsert_params(rest))
        return tr
    if action == "IGNORE":
        if _has_top(rest, "CONFLICT"):
            raise _unsupported("INSERT OR IGNORE with an upsert clause", sql)
        body = translate_expr(_alias_select_lists(rest), ctx)
        # place ON CONFLICT DO NOTHING before a top-level RETURNING
        cut = len(body)
        depth = 0
        for k, t in enumerate(body):
            if _is_op(t, "("):
                depth += 1
            elif _is_op(t, ")"):
                depth -= 1
            elif depth == 0 and _is(t, "RETURNING"):
                cut = k
                break
        body = body[:cut] + [_id("ON"), _id("CONFLICT"), _id("DO"), _id("NOTHING")] + body[cut:]
        head = [Tok("id", "INSERT"), _id("INTO"), _id(_qident(table))]
        if columns is not None:
            head += _paren(_join([[_id(_qident(c))] for c in columns]))
        tr = _finish(head + body, ctx, kind="dml", writes=True, insert_table=table, insert_columns=columns,
                     has_returning=has_returning, target_table=table, or_ignore=True)
        _map_params(tr, _value_params(columns, rest))
        return tr
    if action == "REPLACE":
        if _has_top(rest, "CONFLICT") or has_returning:
            raise _unsupported("INSERT OR REPLACE with an upsert or RETURNING clause", sql)
        if rest and _is(rest[0], "VALUES"):
            groups = []
            k = 1
            while k < len(rest):
                if not _is_op(rest[k], "("):
                    raise _unsupported("REPLACE VALUES syntax", sql)
                end = _matching(rest, k)
                groups.append(rest[k + 1:end])
                k = end + 1
                if k < len(rest):
                    if not _is_op(rest[k], ","):
                        raise _unsupported("REPLACE VALUES syntax", sql)
                    k += 1
            rows: List[str] = []
            param_rows: Optional[List[List[str]]] = []
            for g in groups:
                exprs = _split_top(g)
                if param_rows is not None and all(len(e) == 1 and e[0].kind == "param" for e in exprs):
                    param_rows.append([ctx.param(e[0]).text[2:-2] for e in exprs])
                    rows.append("")
                else:
                    param_rows = None
                    rows.append(_render(_join([translate_expr(e, ctx) for e in exprs])))
            if param_rows is not None and len(param_rows) == len(groups):
                plan = ReplacePlan(table=table, columns=columns, rows_are_params=param_rows)
            else:
                # re-translate every row as an expression row (parameter numbering is positional, so restart)
                ctx2 = _Ctx(sql)
                rows = [_render(_join([translate_expr(e, ctx2) for e in _split_top(g)])) for g in groups]
                ctx = ctx2
                plan = ReplacePlan(table=table, columns=columns, rows=rows)
        elif rest and _is(rest[0], "SELECT", "WITH"):
            plan = ReplacePlan(table=table, columns=columns, select_sql=_render(translate_expr(rest, ctx)))
        elif rest and _is(rest[0], "DEFAULT"):
            raise _unsupported("REPLACE ... DEFAULT VALUES", sql)
        else:
            raise _unsupported("REPLACE syntax", sql)
        tr = _finish([], ctx, kind="replace", writes=True, insert_table=table, insert_columns=columns,
                     target_table=table)
        tr.sql = None
        tr.plan = plan
        return tr
    raise _unsupported(f"INSERT OR {action}", sql)


# ═══════════════════════════════════════════════════════════════════════════════════════════════ PRAGMA / txn
_PRAGMA_RE = re.compile(r"^\s*PRAGMA\s+(?:(main|temp)\s*\.\s*)?([A-Za-z_]+)\s*(?:\(\s*(.*?)\s*\)|=\s*(.*?))?\s*;?\s*$",
                        re.I | re.S)
SUPPORTED_PRAGMAS = frozenset({"table_info", "table_xinfo", "index_list", "index_info", "user_version",
                               "journal_mode", "synchronous", "busy_timeout", "foreign_keys", "wal_checkpoint",
                               "query_only", "page_size", "page_count", "freelist_count", "integrity_check",
                               "quick_check", "foreign_key_list", "cache_size", "temp_store", "mmap_size",
                               "wal_autocheckpoint", "recursive_triggers", "optimize"})


def _translate_pragma(toks: List[Tok], sql: str) -> Translation:
    m = _PRAGMA_RE.match(sql)
    if not m:
        raise _unsupported("PRAGMA syntax", sql)
    name = m.group(2).lower()
    if name not in SUPPORTED_PRAGMAS:
        raise _unsupported(f"PRAGMA {name}", sql)
    arg = m.group(3)
    value = m.group(4)

    def unq(v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.strip().rstrip(";").strip()
        if len(v) >= 2 and v[0] in "\"'`[" and v[-1] in "\"'`]":
            v = v[1:-1]
        return v

    arg = unq(arg)
    value = unq(value)
    if name in ("table_info", "table_xinfo", "index_list", "index_info", "foreign_key_list") and arg:
        arg = arg.lower()
    writes = value is not None or (arg is not None and name not in ("table_info", "table_xinfo", "index_list",
                                                                     "index_info", "foreign_key_list"))
    return Translation(kind="pragma", plan=PragmaPlan(name=name, arg=arg, value=value), writes=writes,
                       is_select=True)


def _translate_txn(toks: List[Tok], sql: str) -> Translation:
    up = toks[0].up
    words = [t.up or t.text for t in toks]
    if up == "BEGIN":
        mode = words[1] if len(words) > 1 and words[1] in ("DEFERRED", "IMMEDIATE", "EXCLUSIVE") else "DEFERRED"
        rest = words[2:] if len(words) > 1 and words[1] in ("DEFERRED", "IMMEDIATE", "EXCLUSIVE") else words[1:]
        if rest not in ([], ["TRANSACTION"]):
            raise _unsupported("BEGIN syntax", sql)
        return Translation(kind="txn", plan=TxnPlan("begin_immediate" if mode != "DEFERRED" else "begin"))
    if up in ("COMMIT", "END"):
        if words[1:] not in ([], ["TRANSACTION"]):
            raise _unsupported("COMMIT syntax", sql)
        return Translation(kind="txn", plan=TxnPlan("commit"))
    if up == "ROLLBACK":
        rest = words[1:]
        if rest and rest[0] == "TRANSACTION":
            rest = rest[1:]
        if not rest:
            return Translation(kind="txn", plan=TxnPlan("rollback"))
        if rest[0] == "TO":
            name_tok = toks[-1]
            return Translation(kind="txn", plan=TxnPlan("rollback_to", _ident_name(name_tok)))
        raise _unsupported("ROLLBACK syntax", sql)
    if up == "SAVEPOINT" and len(toks) == 2:
        return Translation(kind="txn", plan=TxnPlan("savepoint", _ident_name(toks[1])))
    if up == "RELEASE":
        return Translation(kind="txn", plan=TxnPlan("release", _ident_name(toks[-1])))
    raise _unsupported("transaction statement", sql)


# ═══════════════════════════════════════════════════════════════════════════════════════════════ DDL
def _money_columns(table: str) -> frozenset:
    try:
        from shared_lib.persistence.legacy_money import MONETARY_COLUMNS
    except ImportError:  # pragma: no cover - the module ships with this one
        return frozenset()
    return frozenset(MONETARY_COLUMNS.get(table, ()))


def money_type() -> str:
    """The PostgreSQL type of a monetary legacy REAL column (``legacy_money.PG_MONEY_TYPE``: unconstrained NUMERIC,
    which holds every finite binary64 exactly)."""
    try:
        from shared_lib.persistence.legacy_money import PG_MONEY_TYPE
    except ImportError:  # pragma: no cover
        return "NUMERIC"
    return PG_MONEY_TYPE


def _affinity(declared: str) -> str:
    """SQLite's column affinity rules (datatype3.html section 3.1)."""
    t = (declared or "").upper()
    if "INT" in t:
        return "INTEGER"
    if "CHAR" in t or "CLOB" in t or "TEXT" in t:
        return "TEXT"
    if "BLOB" in t or not t:
        return "BLOB" if t else "NONE"
    if "REAL" in t or "FLOA" in t or "DOUB" in t:
        return "REAL"
    return "NUMERIC"


def pg_type(table: str, column: str, declared: str) -> str:
    t = (declared or "").upper().strip()
    aff = _affinity(t)
    if aff == "INTEGER":
        return "BIGINT"
    if aff == "TEXT":
        return "TEXT"
    if aff == "BLOB":
        return "BYTEA"
    if aff == "NONE":
        return "TEXT"
    if aff == "REAL":
        return money_type() if column in _money_columns(table) else "DOUBLE PRECISION"
    base = t.split("(")[0].strip()
    if base in ("BOOLEAN", "BOOL"):
        return "BIGINT"
    if base in ("DATE", "DATETIME", "TIMESTAMP", "TIME", "JSON", "JSONB", "UUID"):
        return "TEXT"
    if base in ("NUMERIC", "DECIMAL"):
        return "NUMERIC"
    raise _unsupported(f"column type {declared!r} ({table}.{column})")


_COL_CONSTRAINT_STARTS = {"CONSTRAINT", "PRIMARY", "NOT", "NULL", "UNIQUE", "CHECK", "DEFAULT", "COLLATE",
                          "REFERENCES", "GENERATED", "AS"}


#: Column comment prefix recording a column's declared (mixed-case) SQLite name; PostgreSQL folds it to lower case,
#: SQLite reports it as declared (``SELECT *``, ``PRAGMA table_info``, ``dict(row)``).
DECLARED_NAME_PREFIX = "cf_declared:"


def _declared_name_sql(table: str, tok: Tok) -> Optional[str]:
    if tok.kind == "id":
        declared = tok.text
    elif tok.kind == "qid":
        declared = tok.text[1:-1]
    else:
        return None
    if declared == declared.lower():
        return None
    return (f"COMMENT ON COLUMN {_qident(table)}.{_qident(declared.lower())} IS "
            f"'{DECLARED_NAME_PREFIX}{declared.replace(chr(39), chr(39) * 2)}'")


def _translate_column(table: str, col_toks: List[Tok], ctx: _Ctx, *, alter: bool = False
                      ) -> Tuple[str, str, bool, Optional[str]]:
    """Returns (column name, PostgreSQL definition, is_rowid_alias, pk marker)."""
    name = _ident_name(col_toks[0])
    k = 1
    type_toks: List[Tok] = []
    while k < len(col_toks) and not (col_toks[k].kind == "id" and col_toks[k].up in _COL_CONSTRAINT_STARTS):
        if _is_op(col_toks[k], "("):
            end = _matching(col_toks, k)
            type_toks.extend(col_toks[k:end + 1])
            k = end + 1
            continue
        type_toks.append(col_toks[k])
        k += 1
    declared = " ".join(t.text for t in type_toks).replace(" (", "(").replace("( ", "(").replace(" )", ")")
    parts: List[str] = []
    rowid_alias = False
    pk = None
    rest = col_toks[k:]
    j = 0
    while j < len(rest):
        t = rest[j]
        u = t.up
        if u == "CONSTRAINT":
            j += 2
            continue
        if u == "PRIMARY":
            if not _is(rest[j + 1] if j + 1 < len(rest) else None, "KEY"):
                raise _unsupported("PRIMARY without KEY", ctx.sql)
            j += 2
            order = None
            if j < len(rest) and _is(rest[j], "ASC", "DESC"):
                order = rest[j].up
                j += 1
            _refuse_conflict_clause(rest, j, ctx)
            autoinc = False
            if j < len(rest) and _is(rest[j], "AUTOINCREMENT"):
                autoinc = True
                j += 1
            if declared.upper() == "INTEGER" and order != "DESC":
                rowid_alias = True
                parts.append("GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY")
            else:
                if autoinc:
                    raise _unsupported("AUTOINCREMENT on a non-INTEGER primary key", ctx.sql)
                parts.append("PRIMARY KEY")
            pk = "pk"
            continue
        if u == "NOT" and _is(rest[j + 1] if j + 1 < len(rest) else None, "NULL"):
            parts.append("NOT NULL")
            j += 2
            _refuse_conflict_clause(rest, j, ctx)
            continue
        if u == "NULL":
            j += 1
            continue
        if u == "UNIQUE":
            parts.append("UNIQUE")
            j += 1
            _refuse_conflict_clause(rest, j, ctx)
            continue
        if u == "CHECK":
            end = _matching(rest, j + 1)
            parts.append("CHECK (" + _render(translate_expr(rest[j + 2:end], ctx)) + ")")
            j = end + 1
            continue
        if u == "DEFAULT":
            v = rest[j + 1]
            if _is_op(v, "("):
                end = _matching(rest, j + 1)
                parts.append("DEFAULT (" + _render(translate_expr(rest[j + 2:end], ctx)) + ")")
                j = end + 1
            elif _is_op(v, "-", "+"):
                parts.append("DEFAULT " + v.text + rest[j + 2].text)
                j += 3
            else:
                parts.append("DEFAULT " + _render(translate_expr([v], ctx)))
                j += 2
            continue
        if u == "COLLATE":
            coll = rest[j + 1].text.upper()
            if coll != "BINARY":
                raise _unsupported(f"COLLATE {coll}", ctx.sql)
            j += 2
            continue
        if u == "REFERENCES":
            # foreign keys are declarative in the legacy SQLite runtime (foreign_keys is never enabled)
            j += 1
            while j < len(rest):
                if rest[j].kind == "id" and rest[j].up in _COL_CONSTRAINT_STARTS - {"NOT", "NULL"}:
                    break
                if _is(rest[j], "NOT") and _is(rest[j + 1] if j + 1 < len(rest) else None, "NULL"):
                    break
                if _is_op(rest[j], "("):
                    j = _matching(rest, j) + 1
                    continue
                j += 1
            continue
        if u in ("GENERATED", "AS"):
            if u == "GENERATED":
                j += 2  # GENERATED ALWAYS
            if not _is(rest[j], "AS"):
                raise _unsupported("generated column syntax", ctx.sql)
            end = _matching(rest, j + 1)
            parts.append("GENERATED ALWAYS AS (" + _render(translate_expr(rest[j + 2:end], ctx)) + ") STORED")
            j = end + 1
            if j < len(rest) and _is(rest[j], "STORED", "VIRTUAL"):
                j += 1
            continue
        raise _unsupported(f"column constraint {t.text}", ctx.sql)
    if rowid_alias:
        pgt = "BIGINT"
    else:
        pgt = pg_type(table, name, declared)
    return name, " ".join([_qident(name), pgt] + parts), rowid_alias, pk


def _refuse_conflict_clause(rest: List[Tok], j: int, ctx: _Ctx) -> None:
    if j < len(rest) and _is(rest[j], "ON") and _is(rest[j + 1] if j + 1 < len(rest) else None, "CONFLICT"):
        raise _unsupported("column conflict clause", ctx.sql)


def _translate_create(toks: List[Tok], ctx: _Ctx) -> Translation:
    sql = ctx.sql
    i = 1
    temp = False
    if _is(toks[i], "TEMP", "TEMPORARY"):
        temp = True
        i += 1
    unique = False
    if _is(toks[i], "UNIQUE"):
        unique = True
        i += 1
    kind = toks[i].up
    i += 1
    if_not_exists = False
    if _is(toks[i], "IF"):
        if not (_is(toks[i + 1], "NOT") and _is(toks[i + 2], "EXISTS")):
            raise _unsupported("IF syntax", sql)
        if_not_exists = True
        i += 3
    if kind == "TABLE":
        return _create_table(toks, i, temp, if_not_exists, ctx)
    if kind == "INDEX":
        return _create_index(toks, i, unique, if_not_exists, ctx)
    if kind == "TRIGGER":
        return _create_trigger(toks, i, temp, if_not_exists, ctx)
    if kind == "VIEW":
        name, i = _table_name(toks, i)
        if _is_op(toks[i], "("):
            raise _unsupported("CREATE VIEW with a column list", sql)
        if not _is(toks[i], "AS"):
            raise _unsupported("CREATE VIEW syntax", sql)
        body = translate_expr(_alias_select_lists(toks[i + 1:]), ctx)
        if ctx.mode:
            raise _unsupported("parameters in CREATE VIEW", sql)
        view_sql = f"CREATE {'TEMP ' if temp else ''}VIEW {_qident(name)} AS {_render(body)}"
        return Translation(kind="view", plan=ViewPlan(name, if_not_exists, view_sql), writes=True,
                           registry=[("upsert", "view", name, name, sql)], catalog_change=True)
    raise _unsupported(f"CREATE {kind}", sql)


def _create_table(toks: List[Tok], i: int, temp: bool, if_not_exists: bool, ctx: _Ctx) -> Translation:
    sql = ctx.sql
    table, i = _table_name(toks, i)
    if _is(toks[i], "AS"):
        raise _unsupported("CREATE TABLE ... AS SELECT", sql)
    if not _is_op(toks[i], "("):
        raise _unsupported("CREATE TABLE syntax", sql)
    end = _matching(toks, i)
    tail = [t.up for t in toks[end + 1:]]
    if tail and tail not in (["STRICT"],):
        raise _unsupported(f"table options {' '.join(tail)}", sql)
    defs = _split_top(toks[i + 1:end])
    cols: List[str] = []
    constraints: List[str] = []
    rowid_alias: Optional[str] = None
    col_types: Dict[str, str] = {}
    comments: List[str] = []
    for d in defs:
        if not d:
            raise _unsupported("empty column definition", sql)
        u = d[0].up
        nxt1 = d[1] if len(d) > 1 else None
        if (u in ("CONSTRAINT", "FOREIGN") or (u == "PRIMARY" and _is(nxt1, "KEY"))
                or (u in ("UNIQUE", "CHECK") and _is_op(nxt1, "("))):
            j = 0
            if u == "CONSTRAINT":
                j = 2
            kw = d[j].up
            if kw == "PRIMARY":
                end2 = _matching(d, j + 2)
                pkcols = [_ident_name(c[0]) for c in _split_top(d[j + 3:end2])]
                for c in _split_top(d[j + 3:end2]):
                    if len(c) > 1 and not (len(c) == 2 and _is(c[1], "ASC", "DESC")):
                        raise _unsupported("expression in PRIMARY KEY", sql)
                _refuse_conflict_clause(d, end2 + 1, ctx)
                constraints.append("PRIMARY KEY (" + ", ".join(_qident(c) for c in pkcols) + ")")
                if len(pkcols) == 1 and col_types.get(pkcols[0], "").upper() == "INTEGER":
                    rowid_alias = pkcols[0]
            elif kw == "UNIQUE":
                end2 = _matching(d, j + 1)
                ucols = [_ident_name(c[0]) for c in _split_top(d[j + 2:end2])]
                _refuse_conflict_clause(d, end2 + 1, ctx)
                constraints.append("UNIQUE (" + ", ".join(_qident(c) for c in ucols) + ")")
            elif kw == "CHECK":
                end2 = _matching(d, j + 1)
                constraints.append("CHECK (" + _render(translate_expr(d[j + 2:end2], ctx)) + ")")
            elif kw == "FOREIGN":
                continue  # declarative only, see the module note
            else:
                raise _unsupported(f"table constraint {kw}", sql)
            continue
        name, definition, is_alias, _pk = _translate_column(table, d, ctx)
        declared_sql = _declared_name_sql(table, d[0])
        if declared_sql:
            comments.append(declared_sql)
        k = 1
        decl = []
        while k < len(d) and not (d[k].kind == "id" and d[k].up in _COL_CONSTRAINT_STARTS):
            decl.append(d[k].text)
            k += 1
        col_types[name] = " ".join(decl)
        if is_alias:
            rowid_alias = name
        cols.append(definition)
    if ctx.mode:
        raise _unsupported("parameters in CREATE TABLE", sql)
    if rowid_alias and not any(c.startswith(_qident(rowid_alias) + " BIGINT GENERATED") for c in cols):
        # table-level PRIMARY KEY(id) with id INTEGER: make the column an identity
        cols = [c.replace(_qident(rowid_alias) + " BIGINT", _qident(rowid_alias) + " BIGINT GENERATED BY DEFAULT AS "
                          "IDENTITY", 1) if c.startswith(_qident(rowid_alias) + " BIGINT") else c for c in cols]
    if rowid_alias:
        cols.append(f"{ROWID} BIGINT GENERATED ALWAYS AS ({_qident(rowid_alias)}) STORED")
    else:
        cols.append(f"{ROWID} BIGINT GENERATED BY DEFAULT AS IDENTITY")
    body = ",\n  ".join(cols + constraints)
    pg = f"CREATE {'TEMP ' if temp else ''}TABLE {'IF NOT EXISTS ' if if_not_exists else ''}{_qident(table)} (\n  {body})"
    registry = [] if temp else [("create_table", "table", table, table, sql.strip().rstrip(";").strip())]
    return Translation(kind="ddl", sql=pg, writes=True, registry=registry, catalog_change=True, post_sql=comments)


def _create_index(toks: List[Tok], i: int, unique: bool, if_not_exists: bool, ctx: _Ctx) -> Translation:
    sql = ctx.sql
    name, i = _table_name(toks, i)
    if not _is(toks[i], "ON"):
        raise _unsupported("CREATE INDEX syntax", sql)
    table, i = _table_name(toks, i + 1)
    if not _is_op(toks[i], "("):
        raise _unsupported("CREATE INDEX syntax", sql)
    end = _matching(toks, i)
    items = []
    for item in _split_top(toks[i + 1:end]):
        if any(_is(x, "COLLATE") for x in item):
            raise _unsupported("COLLATE in an index", sql)
        order = ""
        if item and _is(item[-1], "ASC", "DESC"):
            order = " " + item[-1].up
            item = item[:-1]
        if len(item) == 1 and item[0].kind in ("id", "qid"):
            items.append(_qident(_ident_name(item[0])) + order)
        else:
            items.append("(" + _render(translate_expr(item, ctx)) + ")" + order)
    rest = toks[end + 1:]
    where = ""
    if rest:
        if not _is(rest[0], "WHERE"):
            raise _unsupported("CREATE INDEX trailing syntax", sql)
        where = " WHERE " + _render(translate_expr(rest[1:], ctx))
    if ctx.mode:
        raise _unsupported("parameters in CREATE INDEX", sql)
    pg = (f"CREATE {'UNIQUE ' if unique else ''}INDEX {'IF NOT EXISTS ' if if_not_exists else ''}{_qident(name)} "
          f"ON {_qident(table)} ({', '.join(items)}){where}")
    return Translation(kind="ddl", sql=pg, writes=True, registry=[("create", "index", name, table,
                                                                    sql.strip().rstrip(";").strip())],
                       catalog_change=True)


def trigger_function_name(name: str) -> str:
    base = f"cf_trg_{name}"
    if len(base) <= 63:
        return base
    return base[:50] + "_" + hashlib.sha1(name.encode()).hexdigest()[:12]


def _create_trigger(toks: List[Tok], i: int, temp: bool, if_not_exists: bool, ctx: _Ctx) -> Translation:
    sql = ctx.sql
    if temp:
        raise _unsupported("TEMP trigger", sql)
    name, i = _table_name(toks, i)
    timing = "BEFORE"
    if _is(toks[i], "BEFORE", "AFTER"):
        timing = toks[i].up
        i += 1
    elif _is(toks[i], "INSTEAD"):
        raise _unsupported("INSTEAD OF trigger", sql)
    event = toks[i].up
    i += 1
    of_cols: List[str] = []
    if event == "UPDATE" and _is(toks[i], "OF"):
        i += 1
        while True:
            of_cols.append(_ident_name(toks[i]))
            i += 1
            if _is_op(toks[i], ","):
                i += 1
                continue
            break
    if event not in ("INSERT", "UPDATE", "DELETE"):
        raise _unsupported(f"trigger event {event}", sql)
    if not _is(toks[i], "ON"):
        raise _unsupported("trigger syntax", sql)
    table, i = _table_name(toks, i + 1)
    if _is(toks[i], "FOR"):
        i += 3  # FOR EACH ROW
    when: List[Tok] = []
    if _is(toks[i], "WHEN"):
        j = i + 1
        depth = 0
        while not (depth == 0 and _is(toks[j], "BEGIN")):
            if _is_op(toks[j], "("):
                depth += 1
            elif _is_op(toks[j], ")"):
                depth -= 1
            j += 1
        when = toks[i + 1:j]
        i = j
    if not _is(toks[i], "BEGIN") or not _is(toks[-1], "END"):
        raise _unsupported("trigger body", sql)
    body_stmts = [s for s in _split_top(toks[i + 1:-1], ";") if s]
    ret = "NULL" if timing == "AFTER" else ("OLD" if event == "DELETE" else "NEW")
    lines = []
    if when:
        lines.append(f"IF ({_render(translate_expr(when, ctx))}) IS NOT TRUE THEN RETURN {ret}; END IF;")
    self_write = False
    for st in body_stmts:
        if _is(st[0], "SELECT") and len(st) >= 4 and _is(st[1], "RAISE"):
            end = _matching(st, 2)
            if end != len(st) - 1:
                raise _unsupported("RAISE with a FROM/WHERE clause", sql)
            args = _split_top(st[3:end])
            mode = args[0][0].up
            if mode == "IGNORE":
                if timing != "BEFORE":
                    raise _unsupported("RAISE(IGNORE) in an AFTER trigger", sql)
                lines.append("RETURN NULL;")
                continue
            if mode not in ("ABORT", "FAIL", "ROLLBACK") or len(args) != 2 or len(args[1]) != 1 \
                    or args[1][0].kind != "str":
                raise _unsupported("RAISE form", sql)
            message = args[1][0].text.replace("%", "%%")
            lines.append(f"RAISE EXCEPTION USING ERRCODE = '{TRIGGER_ABORT_SQLSTATE}', MESSAGE = {message};")
            continue
        if _is(st[0], "INSERT", "UPDATE", "DELETE", "REPLACE"):
            inner = _translate(emit(st))
            if inner.kind != "dml" or inner.param_names:
                raise _unsupported("trigger body statement", sql)
            target = inner.insert_table
            if target is None:
                k = 1
                if _is(st[0], "DELETE"):
                    k = 2
                target = _ident_name(st[k])
            if target == table:
                self_write = True
            lines.append(inner.sql + ";")
            continue
        raise _unsupported("trigger body statement", sql)
    if ctx.mode:
        raise _unsupported("parameters in CREATE TRIGGER", sql)
    if self_write:
        lines.insert(0, f"IF pg_trigger_depth() > 1 THEN RETURN {ret}; END IF;")
    fname = trigger_function_name(name)
    fn = (f"CREATE OR REPLACE FUNCTION {fname}() RETURNS trigger LANGUAGE plpgsql AS $cf$\nBEGIN\n  "
          + "\n  ".join(lines) + f"\n  RETURN {ret};\nEND\n$cf$")
    ev = event + (" OF " + ", ".join(_qident(c) for c in of_cols) if of_cols else "")
    trg = f"CREATE TRIGGER {_qident(name)} {timing} {ev} ON {_qident(table)} FOR EACH ROW EXECUTE FUNCTION {fname}()"
    return Translation(kind="trigger", plan=TriggerPlan(name, table, if_not_exists, fname, fn, trg), writes=True,
                       registry=[("create", "trigger", name, table, sql.strip().rstrip(";").strip())],
                       catalog_change=True)


def _translate_alter(toks: List[Tok], ctx: _Ctx) -> Translation:
    sql = ctx.sql
    if not _is(toks[1], "TABLE"):
        raise _unsupported("ALTER syntax", sql)
    table, i = _table_name(toks, 2)
    op = toks[i].up
    if op == "ADD":
        i += 1
        if _is(toks[i], "COLUMN"):
            i += 1
        col_toks = toks[i:]
        name, definition, is_alias, pk = _translate_column(table, col_toks, ctx, alter=True)
        if is_alias or pk:
            raise _unsupported("ADD COLUMN with PRIMARY KEY", sql)
        if ctx.mode:
            raise _unsupported("parameters in ALTER TABLE", sql)
        declared_sql = _declared_name_sql(table, col_toks[0])
        return Translation(kind="ddl", sql=f"ALTER TABLE {_qident(table)} ADD COLUMN {definition}", writes=True,
                           registry=[("add_column", table, emit(col_toks))], catalog_change=True,
                           post_sql=[declared_sql] if declared_sql else [])
    if op == "RENAME":
        if _is(toks[i + 1], "TO"):
            new = _ident_name(toks[i + 2])
            # a rebuilt table (CREATE x_new ...; RENAME TO x) takes the column types of its FINAL name: the adapter
            # re-types REAL columns that legacy_money classifies as monetary for ``new``
            return Translation(kind="ddl", sql=f"ALTER TABLE {_qident(table)} RENAME TO {_qident(new)}", writes=True,
                               registry=[("rename_table", table, new)], catalog_change=True,
                               plan=RetypeMoney(new))
        j = i + 1
        if _is(toks[j], "COLUMN"):
            j += 1
        old = _ident_name(toks[j])
        if not _is(toks[j + 1], "TO"):
            raise _unsupported("RENAME syntax", sql)
        new = _ident_name(toks[j + 2])
        return Translation(kind="ddl", sql=f"ALTER TABLE {_qident(table)} RENAME COLUMN {_qident(old)} TO "
                                           f"{_qident(new)}", writes=True,
                           registry=[("rename_column", table, old, new)], catalog_change=True)
    if op == "DROP":
        j = i + 1
        if _is(toks[j], "COLUMN"):
            j += 1
        col = _ident_name(toks[j])
        return Translation(kind="ddl", sql=f"ALTER TABLE {_qident(table)} DROP COLUMN {_qident(col)}", writes=True,
                           registry=[("drop_column", table, col)], catalog_change=True)
    raise _unsupported(f"ALTER TABLE {op}", sql)


def _translate_drop(toks: List[Tok], ctx: _Ctx) -> Translation:
    sql = ctx.sql
    kind = toks[1].up
    i = 2
    if_exists = False
    if _is(toks[i], "IF"):
        if_exists = True
        i += 2
    name, i = _table_name(toks, i)
    if i != len(toks):
        raise _unsupported("DROP syntax", sql)
    ie = "IF EXISTS " if if_exists else ""
    if kind == "TABLE":
        return Translation(kind="ddl", sql=f"DROP TABLE {ie}{_qident(name)}", writes=True,
                           registry=[("drop_table", name)], catalog_change=True)
    if kind == "INDEX":
        return Translation(kind="ddl", sql=f"DROP INDEX {ie}{_qident(name)}", writes=True,
                           registry=[("drop", name)], catalog_change=True)
    if kind == "VIEW":
        return Translation(kind="ddl", sql=f"DROP VIEW {ie}{_qident(name)}", writes=True,
                           registry=[("drop", name)], catalog_change=True)
    if kind == "TRIGGER":
        return Translation(kind="drop_trigger", plan=DropTriggerPlan(name, if_exists), writes=True,
                           registry=[("drop", name)], catalog_change=True)
    raise _unsupported(f"DROP {kind}", sql)


def is_implicit_begin_statement(sql: str) -> bool:
    """Python's sqlite3 (legacy transaction control) opens a transaction before INSERT/UPDATE/DELETE/REPLACE,
    judged by the statement's first word after whitespace -- mirrored exactly."""
    s = sql.lstrip(" \t\r\n")
    head = s[:7].lower()
    return head.startswith(("insert", "update", "delete")) or head.startswith("replace")


# ═══════════════════════════════════════════════════════════════════════════════════════════════ bootstrap
#: Installed once per PostgreSQL database (idempotent). Registry + sqlite_master view + SQLite function ports.
BOOTSTRAP_SQL = r"""
CREATE TABLE IF NOT EXISTS cf_legacy_schema (
    name TEXT PRIMARY KEY, type TEXT NOT NULL, tbl_name TEXT NOT NULL, sql TEXT);
CREATE TABLE IF NOT EXISTS cf_legacy_meta (key TEXT PRIMARY KEY, value TEXT);

CREATE OR REPLACE VIEW sqlite_master AS
  SELECT 'table'::text AS type, c.relname::text AS name, c.relname::text AS tbl_name, 0::bigint AS rootpage,
         r.sql AS sql
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN cf_legacy_schema r ON r.name = c.relname AND r.type = 'table'
   WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p') AND c.relname NOT LIKE 'cf\_legacy\_%'
  UNION ALL
  SELECT 'index', ic.relname::text, tc.relname::text, 0, r.sql
    FROM pg_index x JOIN pg_class ic ON ic.oid = x.indexrelid JOIN pg_class tc ON tc.oid = x.indrelid
    JOIN pg_namespace n ON n.oid = tc.relnamespace
    LEFT JOIN cf_legacy_schema r ON r.name = ic.relname AND r.type = 'index'
   WHERE n.nspname = current_schema() AND tc.relname NOT LIKE 'cf\_legacy\_%'
  UNION ALL
  SELECT 'view', c.relname::text, c.relname::text, 0, r.sql
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN cf_legacy_schema r ON r.name = c.relname AND r.type = 'view'
   WHERE n.nspname = current_schema() AND c.relkind = 'v' AND c.relname NOT IN ('sqlite_master', 'sqlite_schema')
  UNION ALL
  SELECT 'trigger', t.tgname::text, c.relname::text, 0, r.sql
    FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN cf_legacy_schema r ON r.name = t.tgname AND r.type = 'trigger'
   WHERE n.nspname = current_schema() AND NOT t.tgisinternal;
CREATE OR REPLACE VIEW sqlite_schema AS SELECT * FROM sqlite_master;

-- ---------------------------------------------------------------- CAST(x AS INTEGER / REAL) with SQLite semantics
CREATE OR REPLACE FUNCTION cf_cast_int(x text) RETURNS bigint LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT COALESCE((substring(btrim(x, E' \t\r\n') from '^[+-]?[0-9]+'))::numeric, 0)::bigint $$;
CREATE OR REPLACE FUNCTION cf_cast_int(x bigint) RETURNS bigint LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT x $$;
CREATE OR REPLACE FUNCTION cf_cast_int(x integer) RETURNS bigint LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT x::bigint $$;
CREATE OR REPLACE FUNCTION cf_cast_int(x double precision) RETURNS bigint LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT trunc(x)::bigint $$;
CREATE OR REPLACE FUNCTION cf_cast_int(x numeric) RETURNS bigint LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT trunc(x)::bigint $$;
CREATE OR REPLACE FUNCTION cf_cast_real(x text) RETURNS double precision LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT COALESCE((substring(btrim(x, E' \t\r\n') from '^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?'))::double precision, 0.0) $$;
CREATE OR REPLACE FUNCTION cf_cast_real(x double precision) RETURNS double precision LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT x $$;
CREATE OR REPLACE FUNCTION cf_cast_real(x numeric) RETURNS double precision LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT x::double precision $$;
CREATE OR REPLACE FUNCTION cf_cast_real(x bigint) RETURNS double precision LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT x::double precision $$;
CREATE OR REPLACE FUNCTION cf_cast_real(x integer) RETURNS double precision LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT x::double precision $$;

-- ---------------------------------------------------------------- SUM(<comparison>): SQLite sums 0/1
CREATE OR REPLACE FUNCTION cf_sum_bool_step(acc bigint, v boolean) RETURNS bigint LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN v IS NULL THEN acc ELSE COALESCE(acc, 0) + v::int END $$;
CREATE OR REPLACE AGGREGATE sum(boolean) (SFUNC = cf_sum_bool_step, STYPE = bigint);

-- ---------------------------------------------------------------- json_extract (simple $.a.b / $[n] paths)
CREATE OR REPLACE FUNCTION cf_json_extract(doc text, path text) RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE parts text[] := '{}'; m text[]; rest text; v jsonb;
BEGIN
  IF doc IS NULL OR path IS NULL THEN RETURN NULL; END IF;
  IF left(path, 1) <> '$' THEN
    RAISE EXCEPTION 'cf_sqlite: JSON path % is not supported', path USING ERRCODE = '0A000';
  END IF;
  rest := substr(path, 2);
  WHILE rest <> '' LOOP
    m := regexp_match(rest, '^\.([A-Za-z_][A-Za-z0-9_]*)');
    IF m IS NOT NULL THEN parts := parts || m[1]; rest := substr(rest, length(m[1]) + 2); CONTINUE; END IF;
    m := regexp_match(rest, '^\[([0-9]+)\]');
    IF m IS NOT NULL THEN parts := parts || m[1]; rest := substr(rest, length(m[1]) + 3); CONTINUE; END IF;
    RAISE EXCEPTION 'cf_sqlite: JSON path % is not supported', path USING ERRCODE = '0A000';
  END LOOP;
  v := doc::jsonb #> parts;
  IF v IS NULL OR jsonb_typeof(v) = 'null' THEN RETURN NULL; END IF;
  IF jsonb_typeof(v) = 'string' THEN RETURN v #>> '{}'; END IF;
  IF jsonb_typeof(v) = 'boolean' THEN RETURN CASE WHEN v = 'true'::jsonb THEN '1' ELSE '0' END; END IF;
  RETURN v::text;
END $$;

-- ---------------------------------------------------------------- SQLite date and time functions
-- cf_sqlite_ts(time-value, modifiers) -> UTC timestamp (millisecond precision) or NULL, with SQLite semantics.
CREATE OR REPLACE FUNCTION cf_sqlite_ts(p_ts text, p_mods text[], OUT ts timestamp, OUT subsec boolean)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  s text; m text[]; md text; i int := 1; n int; num numeric; y int; mo int; d int; hh int; mi int; sec numeric;
  amount numeric; unit text; whole int; frac numeric; wd int; cur int; tz text; is_num boolean := false;
BEGIN
  subsec := false;
  ts := NULL;
  IF p_ts IS NULL THEN RETURN; END IF;
  n := COALESCE(array_length(p_mods, 1), 0);
  FOR k IN 1..n LOOP IF p_mods[k] IS NULL THEN RETURN; END IF; END LOOP;
  s := btrim(p_ts);
  IF lower(s) = 'now' THEN
    ts := date_trunc('milliseconds', statement_timestamp() AT TIME ZONE 'UTC');
  ELSIF s ~ '^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$' THEN
    num := s::numeric;
    is_num := true;
    IF n >= 1 AND lower(btrim(p_mods[1])) = 'unixepoch' THEN
      ts := timestamp '1970-01-01' + num * interval '1 second';
      i := 2;
    ELSIF n >= 1 AND lower(btrim(p_mods[1])) = 'auto' THEN
      IF num >= 0 AND num < 5373484.5 THEN
        ts := timestamp '1970-01-01' + (num - 2440587.5) * interval '1 day';
      ELSE
        ts := timestamp '1970-01-01' + num * interval '1 second';
      END IF;
      i := 2;
    ELSE
      IF n >= 1 AND lower(btrim(p_mods[1])) = 'julianday' THEN i := 2; END IF;
      ts := timestamp '1970-01-01' + (num - 2440587.5) * interval '1 day';
    END IF;
  ELSE
    m := regexp_match(s, '^(?:([0-9]{4})-([0-9]{2})-([0-9]{2}))?(?:(?:^|[T ]+)([0-9]{2}):([0-9]{2})(?::([0-9]{2}(?:\.[0-9]*)?))?)?\s*([zZ]|[+-][0-9]{2}:?[0-9]{2})?$');
    IF m IS NULL OR (m[1] IS NULL AND m[4] IS NULL) THEN RETURN; END IF;
    IF m[1] IS NOT NULL THEN y := m[1]::int; mo := m[2]::int; d := m[3]::int; ELSE y := 2000; mo := 1; d := 1; END IF;
    IF mo < 1 OR mo > 12 OR d < 1 OR d > 31 THEN RETURN; END IF;
    hh := COALESCE(m[4]::int, 0); mi := COALESCE(m[5]::int, 0); sec := COALESCE(m[6]::numeric, 0);
    IF hh > 24 OR mi > 59 OR sec >= 60 THEN RETURN; END IF;
    ts := make_timestamp(y, mo, 1, 0, 0, 0) + (d - 1) * interval '1 day' + hh * interval '1 hour'
          + mi * interval '1 minute' + sec * interval '1 second';
    tz := m[7];
    IF tz IS NOT NULL AND upper(tz) <> 'Z' THEN
      tz := replace(tz, ':', '');
      ts := ts - (substr(tz, 1, 1) || '1')::int * (substr(tz, 2, 2)::int * interval '1 hour'
                                                  + substr(tz, 4, 2)::int * interval '1 minute');
    END IF;
  END IF;
  -- SQLite keeps milliseconds
  ts := timestamp '1970-01-01' + round(extract(epoch from ts) * 1000) / 1000 * interval '1 second';
  WHILE i <= n LOOP
    md := lower(btrim(p_mods[i]));
    i := i + 1;
    m := regexp_match(md, '^([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))\s+(day|hour|minute|second|month|year)s?$');
    IF m IS NOT NULL THEN
      amount := m[1]::numeric; unit := m[2];
      IF unit IN ('day', 'hour', 'minute', 'second') THEN
        ts := ts + amount * ('1 ' || unit)::interval;
      ELSE
        IF unit = 'year' THEN amount := amount * 12; END IF;
        whole := trunc(amount)::int; frac := amount - whole;
        y := extract(year from ts)::int; mo := extract(month from ts)::int; d := extract(day from ts)::int;
        cur := y * 12 + (mo - 1) + whole;
        ts := make_timestamp(floor(cur / 12.0)::int, (cur - floor(cur / 12.0)::int * 12) + 1, 1, 0, 0, 0)
              + (d - 1) * interval '1 day' + (ts - date_trunc('day', ts));
        IF frac <> 0 THEN ts := ts + frac * 30 * interval '1 day'; END IF;
      END IF;
      CONTINUE;
    END IF;
    IF md = 'start of day' THEN ts := date_trunc('day', ts); CONTINUE; END IF;
    IF md = 'start of month' THEN ts := date_trunc('month', ts); CONTINUE; END IF;
    IF md = 'start of year' THEN ts := date_trunc('year', ts); CONTINUE; END IF;
    IF md IN ('subsec', 'subsecond') THEN subsec := true; CONTINUE; END IF;
    IF md = 'utc' THEN CONTINUE; END IF;  -- every legacy time value is already UTC
    m := regexp_match(md, '^weekday ([0-6])$');
    IF m IS NOT NULL THEN
      wd := m[1]::int; cur := extract(dow from ts)::int;
      ts := ts + ((wd - cur + 7) % 7) * interval '1 day';
      CONTINUE;
    END IF;
    IF md IN ('unixepoch', 'julianday', 'auto') THEN ts := NULL; RETURN; END IF;  -- only valid first, as SQLite
    RAISE EXCEPTION 'cf_sqlite: date modifier % is not supported', md USING ERRCODE = '0A000';
  END LOOP;
  IF ts IS NOT NULL AND (extract(year from ts) < 0 OR extract(year from ts) > 9999) THEN ts := NULL; END IF;
END $$;

CREATE OR REPLACE FUNCTION cf_datetime(VARIADIC a text[] DEFAULT '{now}') RETURNS text LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN r.subsec THEN to_char(r.ts, 'YYYY-MM-DD HH24:MI:SS.MS') ELSE to_char(r.ts, 'YYYY-MM-DD HH24:MI:SS') END
    FROM cf_sqlite_ts(a[1], a[2:]) r $$;
CREATE OR REPLACE FUNCTION cf_date(VARIADIC a text[] DEFAULT '{now}') RETURNS text LANGUAGE sql STABLE AS $$
  SELECT to_char(r.ts, 'YYYY-MM-DD') FROM cf_sqlite_ts(a[1], a[2:]) r $$;
CREATE OR REPLACE FUNCTION cf_time(VARIADIC a text[] DEFAULT '{now}') RETURNS text LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN r.subsec THEN to_char(r.ts, 'HH24:MI:SS.MS') ELSE to_char(r.ts, 'HH24:MI:SS') END
    FROM cf_sqlite_ts(a[1], a[2:]) r $$;
CREATE OR REPLACE FUNCTION cf_julianday(VARIADIC a text[] DEFAULT '{now}') RETURNS double precision LANGUAGE sql STABLE AS $$
  SELECT (extract(epoch from r.ts) / 86400 + 2440587.5)::double precision FROM cf_sqlite_ts(a[1], a[2:]) r $$;
CREATE OR REPLACE FUNCTION cf_unixepoch(VARIADIC a text[] DEFAULT '{now}') RETURNS bigint LANGUAGE sql STABLE AS $$
  SELECT floor(extract(epoch from r.ts))::bigint FROM cf_sqlite_ts(a[1], a[2:]) r $$;

CREATE OR REPLACE FUNCTION cf_strftime(fmt text, VARIADIC a text[] DEFAULT '{now}') RETURNS text
LANGUAGE plpgsql STABLE AS $$
DECLARE r record; out text := ''; i int := 1; c text; spec text; L int; doy int; dow int;
BEGIN
  IF fmt IS NULL THEN RETURN NULL; END IF;
  SELECT * INTO r FROM cf_sqlite_ts(a[1], a[2:]);
  IF r.ts IS NULL THEN RETURN NULL; END IF;
  L := length(fmt);
  WHILE i <= L LOOP
    c := substr(fmt, i, 1);
    IF c <> '%' THEN out := out || c; i := i + 1; CONTINUE; END IF;
    IF i = L THEN RETURN NULL; END IF;
    spec := substr(fmt, i + 1, 1);
    i := i + 2;
    doy := extract(doy from r.ts)::int; dow := extract(dow from r.ts)::int;
    out := out || CASE spec
      WHEN 'd' THEN to_char(r.ts, 'DD')
      WHEN 'e' THEN lpad(extract(day from r.ts)::int::text, 2, ' ')
      WHEN 'f' THEN to_char(r.ts, 'SS.MS')
      WHEN 'F' THEN to_char(r.ts, 'YYYY-MM-DD')
      WHEN 'H' THEN to_char(r.ts, 'HH24')
      WHEN 'I' THEN to_char(r.ts, 'HH12')
      WHEN 'j' THEN to_char(r.ts, 'DDD')
      WHEN 'J' THEN (extract(epoch from r.ts) / 86400 + 2440587.5)::double precision::text
      WHEN 'k' THEN lpad(extract(hour from r.ts)::int::text, 2, ' ')
      WHEN 'l' THEN lpad(to_char(r.ts, 'FMHH12'), 2, ' ')
      WHEN 'm' THEN to_char(r.ts, 'MM')
      WHEN 'M' THEN to_char(r.ts, 'MI')
      WHEN 'p' THEN to_char(r.ts, 'AM')
      WHEN 'P' THEN lower(to_char(r.ts, 'AM'))
      WHEN 'R' THEN to_char(r.ts, 'HH24:MI')
      WHEN 's' THEN floor(extract(epoch from r.ts))::bigint::text
      WHEN 'S' THEN to_char(r.ts, 'SS')
      WHEN 'T' THEN to_char(r.ts, 'HH24:MI:SS')
      WHEN 'u' THEN extract(isodow from r.ts)::int::text
      WHEN 'w' THEN dow::text
      WHEN 'W' THEN lpad(((doy - 1 + 7 - ((dow + 6) % 7)) / 7)::text, 2, '0')
      WHEN 'U' THEN lpad(((doy - 1 + 7 - dow) / 7)::text, 2, '0')
      WHEN 'V' THEN to_char(r.ts, 'IW')
      WHEN 'G' THEN to_char(r.ts, 'IYYY')
      WHEN 'g' THEN to_char(r.ts, 'IY')
      WHEN 'Y' THEN to_char(r.ts, 'YYYY')
      WHEN '%' THEN '%'
      ELSE NULL END;
    IF out IS NULL THEN RETURN NULL; END IF;
  END LOOP;
  RETURN out;
END $$;
"""

#: Bumped whenever BOOTSTRAP_SQL changes (stored in cf_legacy_meta so an older database is re-bootstrapped).
BOOTSTRAP_VERSION = "2"


__all__ = [
    "BOOTSTRAP_SQL", "BOOTSTRAP_VERSION", "DECLARED_NAME_PREFIX", "DialectUnsupported", "DropTriggerPlan", "LEGACY_WRITE_LOCK_KEY",
    "PASSTHROUGH_FUNCTIONS", "PragmaPlan", "ROWID", "ReplacePlan", "SUPPORTED_PRAGMAS", "TRIGGER_ABORT_SQLSTATE",
    "Translation", "TriggerPlan", "TxnPlan", "ViewPlan", "emit", "is_implicit_begin_statement", "pg_type",
    "split_script", "tokenize", "translate", "translate_expr", "trigger_function_name", "unsupported_events",
]
