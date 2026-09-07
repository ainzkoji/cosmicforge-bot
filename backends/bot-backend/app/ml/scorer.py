"""
MLEntryScorer — ML Entry Quality Scoring Module (Phase 3 Step 5D-2)

Architecture: ML as additive scoring layer (Option C).
The scorer ADDS rejection criteria; it cannot override existing safety gates.
The rule engine (PolicyEngine) retains full veto power at all times.

Safety properties:
- ML_ENABLED=False → entire module is bypassed; bot behaves identically to no-ML
- ML_SHADOW_MODE=True → scores are logged, but NEVER used to block entries
- Model loading failure → graceful degradation (score() returns None, no crash)
- Model is loaded ONCE at startup, never reloaded mid-session
- No online learning; model artifact is read-only during live trading
- If ML service is unavailable → bot continues without ML scoring
- Rollback: set ML_ENABLED=False in .env → instant bypass, no restart needed
  (settings are read at scorer construction time, so a bot restart IS required
   to pick up env-var changes; for instant rollback, set ML_ENABLED=False and
   restart the bot — takes <5 seconds)
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from shared_lib.ml.contract import (
    ML_CONTRACT_VERSION,
    ML_FEATURE_COLUMNS,
    ML_FEATURE_SCHEMA_HASH,
    compute_feature_value,
)
from shared_lib.persistence.db import DB
from shared_lib.persistence.ml_runtime_status import upsert_ml_runtime_status

logger = logging.getLogger(__name__)

FEATURE_COLUMNS = list(ML_FEATURE_COLUMNS)
APPROVED_RUNTIME_MODEL_DIR = "production"

# ML action labels stored in decision_traces
ACTION_ALLOW = "ALLOW"     # Score >= threshold, entry allowed
ACTION_BLOCK = "BLOCK"     # Score < threshold AND shadow_mode=False → entry blocked
ACTION_SHADOW = "SHADOW"   # Score < threshold BUT shadow_mode=True → logged only
ACTION_SKIP = "SKIP"       # Not scored (HOLD cycle, ML disabled, model unavailable)


class MLEntryScorer:
    """
    Loads a trained LightGBM entry quality model and scores candidate entries.

    Thread-safe: a single instance is shared across all symbol threads.
    The model artifact and encoders are loaded once at construction / explicit
    load() call and never modified during the session.
    """

    def __init__(
        self,
        model_path: str = "",
        encoders_path: str = "",
        metadata_path: str = "",
        score_threshold: float = 0.30,
        shadow_mode: bool = True,
        enabled: bool = False,
        log_dir: str = "models/logs",
        hard_block_floor: float = 0.0,
    ):
        self._model_path = model_path
        self._encoders_path = encoders_path
        self._metadata_path = metadata_path
        self._threshold = score_threshold
        self._shadow_mode = shadow_mode
        self._enabled = enabled
        self._log_dir = Path(log_dir)
        # Phase 1 controlled activation floor.  When > 0.0, scores below this value
        # are hard-blocked even while shadow_mode=True.  This lets the system reject
        # extreme low-quality signals without requiring full threshold validation.
        # Set ML_HARD_BLOCK_FLOOR=0.10 in .env to enable; 0.0 = disabled (pure shadow).
        self._hard_block_floor = max(0.0, float(hard_block_floor))

        self._model = None
        self._encoders = None       # dict: col → fitted OrdinalEncoder
        self._metadata: Dict[str, Any] = {}
        self._model_version: Optional[str] = None
        self._loaded = False
        self._load_error: Optional[str] = None
        self._contract_version = ML_CONTRACT_VERSION
        self._schema_hash = ML_FEATURE_SCHEMA_HASH
        self._last_score_timestamp: Optional[str] = None

        self._lock = threading.Lock()
        self._log_file: Optional[Any] = None
        self._status_db = DB()

        if self._enabled:
            self._load()
            self._open_log()
        self._persist_status()

    def _persist_status(self) -> None:
        try:
            upsert_ml_runtime_status(
                self._status_db,
                enabled=self._enabled,
                shadow_mode=self._shadow_mode,
                loaded=self._loaded,
                model_version=self._model_version,
                model_path=self._model_path or None,
                metadata_path=self._metadata_path or None,
                encoders_path=self._encoders_path or None,
                threshold=self._threshold,
                hard_block_floor=self._hard_block_floor,
                contract_version=self._contract_version,
                schema_hash=self._schema_hash,
                last_score_timestamp=self._last_score_timestamp,
                load_error=self._load_error,
            )
        except Exception:
            logger.debug("[MLScorer] Failed to persist runtime status", exc_info=True)

    # ── Loading ───────────────────────────────────────────────────────────────

    def _load(self) -> None:
        """Load model artifact + encoders from disk. Errors are non-fatal."""
        try:
            import joblib  # type: ignore
        except ImportError:
            self._load_error = "joblib not installed (pip install joblib)"
            logger.warning("[MLScorer] %s — ML scoring disabled", self._load_error)
            return

        if not self._model_path:
            self._load_error = "ML_MODEL_PATH not configured"
            logger.warning("[MLScorer] %s — ML scoring disabled", self._load_error)
            return

        model_file = Path(self._model_path)
        if not model_file.exists():
            self._load_error = f"Model file not found: {model_file}"
            logger.warning("[MLScorer] %s — ML scoring disabled", self._load_error)
            return
        if not _is_approved_runtime_model_path(model_file):
            self._load_error = (
                "Model path is not in an approved runtime directory "
                f"(expected models/{APPROVED_RUNTIME_MODEL_DIR}): {model_file}"
            )
            logger.warning("[MLScorer] %s — ML scoring disabled", self._load_error)
            return

        try:
            with self._lock:
                self._model = joblib.load(model_file)
                logger.info("[MLScorer] Loaded model from %s", model_file)

                if self._encoders_path and Path(self._encoders_path).exists():
                    self._encoders = joblib.load(self._encoders_path)
                    logger.info("[MLScorer] Loaded encoders from %s", self._encoders_path)
                else:
                    self._encoders = {}
                    logger.warning("[MLScorer] Encoders path missing — categoricals will be NaN")

                if self._metadata_path and Path(self._metadata_path).exists():
                    with open(self._metadata_path, encoding="utf-8") as f:
                        self._metadata = json.load(f)

                metadata_features = list(self._metadata.get("feature_columns") or [])
                metadata_schema_hash = self._metadata.get("schema_hash")
                metadata_contract_version = self._metadata.get("contract_version")
                if metadata_features != FEATURE_COLUMNS:
                    raise ValueError(
                        "Artifact feature_columns do not match runtime ML contract"
                    )
                if metadata_schema_hash != ML_FEATURE_SCHEMA_HASH:
                    raise ValueError(
                        "Artifact schema_hash does not match runtime ML contract"
                    )
                if metadata_contract_version and metadata_contract_version != ML_CONTRACT_VERSION:
                    raise ValueError(
                        "Artifact contract_version does not match runtime ML contract"
                    )

                # Derive version from model filename stem
                self._model_version = model_file.stem
                self._loaded = True
                self._load_error = None
                self._persist_status()

        except Exception as exc:
            self._load_error = str(exc)
            self._model = None
            self._encoders = None
            self._loaded = False
            self._persist_status()
            logger.error("[MLScorer] Failed to load model: %s", exc, exc_info=True)

    def _open_log(self) -> None:
        """Open the prediction JSONL log file for appending."""
        try:
            self._log_dir.mkdir(parents=True, exist_ok=True)
            date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
            log_path = self._log_dir / f"predictions_{date_str}.jsonl"
            self._log_file = open(log_path, "a", encoding="utf-8")  # noqa: WPS515
            logger.info("[MLScorer] Prediction log: %s", log_path)
        except Exception as exc:
            logger.warning("[MLScorer] Could not open prediction log: %s", exc)
            self._log_file = None

    # ── Feature extraction ────────────────────────────────────────────────────

    def build_feature_vector(
        self,
        *,
        # From DecisionTrace fields
        symbol: str,
        timeframe: str,
        ts: str,                        # ISO timestamp string
        regime_state: str,
        regime_confidence: float,
        last_price: float,
        mark_price: float,
        margin_level: float,
        drawdown_pct: float,
        open_positions_count: int,
        portfolio_risk_used: float,
        final_confidence: float,
        chosen_strategy: Optional[str],
        side: str = "",             # "LONG" or "SHORT" — direction of the entry
        # Frozen indicator snapshot
        adx: Optional[float] = None,
        atr_pct: Optional[float],
        ma_slope: Optional[float],
        compression_ratio: Optional[float],
        breakout_pressure: Optional[float],
        buy_score: Optional[float],
        sell_score: Optional[float],
        threshold: Optional[float],
        active_strategy_count: Optional[int],
        htf_opposed: Optional[bool],
        open_price: Optional[float] = None,
        stop_loss_price: Optional[float] = None,
        tp_plan: Optional[float] = None,
        # Adaptive engine state
        aggressiveness_score: Optional[float] = None,
        confidence_gate_modifier: Optional[float] = None,
        size_multiplier: Optional[float] = None,
        rolling_win_rate: Optional[float] = None,
        rolling_expectancy: Optional[float] = None,
        loss_streak: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Build a feature dict matching FEATURE_COLUMNS.
        NaN is used for missing numeric values (LightGBM handles NaN natively).
        """
        raw: Dict[str, Any] = {
            "symbol": symbol,
            "timeframe": timeframe,
            "trace_ts": ts,
            "ts": ts,
            "regime_state": regime_state,
            "regime_confidence": regime_confidence,
            "last_price": last_price,
            "mark_price": mark_price,
            "margin_level": margin_level,
            "drawdown_pct": drawdown_pct,
            "open_positions_count": open_positions_count,
            "portfolio_risk_used": portfolio_risk_used,
            "confidence": final_confidence,
            "chosen_strategy": chosen_strategy,
            "side": side,
            "adx": adx,
            "atr_pct": atr_pct,
            "ma_slope": ma_slope,
            "compression_ratio": compression_ratio,
            "breakout_pressure": breakout_pressure,
            "buy_score": buy_score,
            "sell_score": sell_score,
            "threshold": threshold,
            "active_strategy_count": active_strategy_count,
            "htf_opposed": htf_opposed,
            "open_price": open_price if open_price is not None else last_price,
            "stop_loss_price": stop_loss_price,
            "tp_plan": tp_plan,
            "aggressiveness_score": aggressiveness_score,
            "confidence_gate_modifier": confidence_gate_modifier,
            "size_multiplier": size_multiplier,
            "rolling_win_rate": rolling_win_rate,
            "rolling_expectancy": rolling_expectancy,
            "loss_streak": loss_streak,
        }
        return {col: compute_feature_value(raw, col) for col in FEATURE_COLUMNS}

        nan = float("nan")

        # ── Regime one-hot ─────────────────────────────────────────────────
        fv: Dict[str, Any] = {}
        regime_clean = (regime_state or "").upper().strip()
        for rv in _REGIME_VALUES:
            fv[f"regime_{rv}"] = 1 if regime_clean == rv else 0

        # ── Regime scalar ──────────────────────────────────────────────────
        fv["regime_confidence"] = float(regime_confidence) if regime_confidence is not None else nan
        fv["adx"] = float(adx) if adx is not None else nan
        fv["atr_pct"] = float(atr_pct) if atr_pct is not None else nan
        fv["ma_slope"] = float(ma_slope) if ma_slope is not None else nan
        fv["compression_ratio"] = float(compression_ratio) if compression_ratio is not None else nan
        fv["breakout_pressure"] = float(breakout_pressure) if breakout_pressure is not None else nan

        # ── Ensemble ───────────────────────────────────────────────────────
        bs = float(buy_score) if buy_score is not None else nan
        ss = float(sell_score) if sell_score is not None else nan
        thr = float(threshold) if threshold is not None else nan
        conf = float(final_confidence) if final_confidence is not None else nan

        fv["buy_score"] = bs
        fv["sell_score"] = ss
        fv["threshold"] = thr
        fv["confidence"] = conf
        fv["active_strategy_count"] = int(active_strategy_count) if active_strategy_count is not None else nan
        fv["htf_opposed"] = (1 if htf_opposed else 0) if htf_opposed is not None else nan
        fv["consensus_gap"] = (bs - ss) if not (math.isnan(bs) or math.isnan(ss)) else nan
        fv["threshold_gap"] = (conf - thr) if not (math.isnan(conf) or math.isnan(thr)) else nan

        # ── Adaptive engine state ──────────────────────────────────────────
        fv["aggressiveness_score"] = float(aggressiveness_score) if aggressiveness_score is not None else nan
        fv["confidence_gate_modifier"] = float(confidence_gate_modifier) if confidence_gate_modifier is not None else nan
        fv["size_multiplier"] = float(size_multiplier) if size_multiplier is not None else nan
        fv["rolling_win_rate"] = float(rolling_win_rate) if rolling_win_rate is not None else nan
        fv["rolling_expectancy"] = float(rolling_expectancy) if rolling_expectancy is not None else nan
        fv["loss_streak"] = float(loss_streak) if loss_streak is not None else nan

        # ── Risk context ───────────────────────────────────────────────────
        fv["drawdown_pct"] = float(drawdown_pct)
        fv["portfolio_risk_used"] = float(portfolio_risk_used)
        fv["open_positions_count"] = int(open_positions_count)
        fv["margin_level"] = float(margin_level) if margin_level is not None else nan

        # ── Temporal ───────────────────────────────────────────────────────
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            fv["hour_utc"] = dt.hour
            fv["day_of_week"] = dt.weekday()  # 0=Monday … 6=Sunday
            fv["is_weekend"] = 1 if dt.weekday() >= 5 else 0
        except Exception:
            fv["hour_utc"] = nan
            fv["day_of_week"] = nan
            fv["is_weekend"] = nan

        # ── Price context ──────────────────────────────────────────────────
        # last_price and mark_price are set to NaN at inference to prevent the model from
        # using price-level memorisation learned during backtest training (Stage B fix 2026-04-05).
        # price_spread_pct is still computed from the real values for signal quality context
        # but the raw prices themselves must not reach the model as features.
        lp = float(last_price) if last_price else nan
        mp = float(mark_price) if mark_price else nan
        fv["last_price"] = nan   # NEUTRALISED — leaky feature (backtest price memorisation)
        fv["mark_price"] = nan   # NEUTRALISED — leaky feature
        if not math.isnan(lp) and lp > 0 and not math.isnan(mp):
            fv["price_spread_pct"] = abs(mp - lp) / lp * 100.0
        else:
            fv["price_spread_pct"] = nan

        # ── Categorical ────────────────────────────────────────────────────
        fv["symbol"] = symbol or ""
        fv["timeframe"] = timeframe or ""
        # side is stored as binary int in training (1=LONG/BUY, 0=SHORT/SELL)
        fv["side"] = 1 if (side or "").upper() in ("LONG", "BUY") else 0
        fv["chosen_strategy"] = chosen_strategy or ""

        return fv

    # ── Encoding ──────────────────────────────────────────────────────────────

    def _encode_features(self, fv: Dict[str, Any]):
        """
        Convert feature dict → ordered list of values matching FEATURE_COLUMNS.
        Apply OrdinalEncoder to categorical columns; unknown categories → NaN.
        Returns a list of floats/ints suitable for model.predict_proba().
        """
        import numpy as np
        import warnings

        # "side" is binary int (0/1) in training — do NOT apply OrdinalEncoder to it
        categorical_cols = {"symbol", "timeframe", "chosen_strategy"}
        row = []
        for col in FEATURE_COLUMNS:
            val = fv.get(col, float("nan"))
            if col in categorical_cols and self._encoders and col in self._encoders:
                enc = self._encoders[col]
                try:
                    # OrdinalEncoder expects 2D array.  Suppress the sklearn feature-name
                    # warning (encoder was fitted with DataFrame; inference uses plain array).
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        encoded = enc.transform([[val]])[0][0]
                    val = float(encoded) if encoded is not None else float("nan")
                except Exception:
                    val = float("nan")
            elif col in categorical_cols:
                # No encoder available — pass NaN (LightGBM handles it)
                val = float("nan")
            else:
                val = float(val) if val is not None and not (isinstance(val, float) and math.isnan(val)) else float("nan")
                # Re-pack NaN as actual float nan for numpy
                if isinstance(fv.get(col), float) and math.isnan(fv[col]):
                    val = float("nan")
            row.append(val)

        return np.array(row, dtype=np.float64).reshape(1, -1)

    # ── Scoring ───────────────────────────────────────────────────────────────

    def score(self, feature_vector: Dict[str, Any]) -> Optional[float]:
        """
        Score a candidate entry.

        Returns:
            float [0.0, 1.0] — entry quality probability estimate, OR
            None — if ML is disabled, model unavailable, or an error occurs.

        The caller is responsible for checking shadow_mode and deciding whether
        to act on the score.  This method NEVER directly blocks entries.
        """
        if not self._enabled:
            return None
        if not self._loaded or self._model is None:
            return None

        try:
            import warnings
            X = self._encode_features(feature_vector)
            # predict_proba returns [[p_class0, p_class1]].
            # Suppress sklearn/LightGBM feature-name warnings (inference uses plain numpy array;
            # model was fitted with named DataFrame columns — functionally identical).
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                proba = self._model.predict_proba(X)
            score = float(proba[0][1])
            self._last_score_timestamp = datetime.now(timezone.utc).isoformat()
            self._persist_status()
            # Clamp to [0, 1] for robustness
            return max(0.0, min(1.0, score))
        except Exception as exc:
            logger.warning("[MLScorer] score() failed for symbol=%s: %s",
                           feature_vector.get("symbol", "?"), exc)
            return None

    def get_action(self, score: Optional[float]) -> str:
        """
        Determine the ML action label given a score.

        Returns one of ACTION_ALLOW / ACTION_BLOCK / ACTION_SHADOW / ACTION_SKIP.

        Priority order:
          1. None score              -> SKIP
          2. score >= threshold      -> ALLOW
          3. score <  hard_block_floor (> 0) -> BLOCK  [Phase 1: floor-block even in shadow_mode]
          4. shadow_mode=True        -> SHADOW
          5. otherwise               -> BLOCK
        """
        if score is None:
            return ACTION_SKIP
        if score >= self._threshold:
            return ACTION_ALLOW
        # Phase 1 floor-block: enforced even in shadow_mode for extreme outliers.
        # Set ML_HARD_BLOCK_FLOOR > 0.0 in .env to activate.
        if self._hard_block_floor > 0.0 and score < self._hard_block_floor:
            return ACTION_BLOCK
        # Score below threshold but above floor
        if self._shadow_mode:
            return ACTION_SHADOW
        return ACTION_BLOCK

    def should_block(self, score: Optional[float]) -> bool:
        """
        Returns True ONLY if the scorer is active, shadow_mode is OFF,
        and the score is below threshold.  False in all other cases.
        """
        return self.get_action(score) == ACTION_BLOCK

    # ── Prediction logging ────────────────────────────────────────────────────

    def log_prediction(
        self,
        *,
        trace_id: Optional[str],
        symbol: str,
        score: Optional[float],
        action: str,
        model_version: Optional[str],
        threshold: float,
        feature_vector: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Append a prediction record to the daily JSONL log file.
        Each line: one JSON object.  Never raises; logging failures are silent.
        """
        if self._log_file is None:
            return
        try:
            record = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "trace_id": trace_id,
                "symbol": symbol,
                "score": score,
                "action": action,
                "model_version": model_version,
                "threshold": threshold,
                "shadow_mode": self._shadow_mode,
                # Lightweight feature hash for dataset reconstruction
                "features_hash": _dict_hash(feature_vector) if feature_vector else None,
            }
            line = json.dumps(record) + "\n"
            with self._lock:
                self._log_file.write(line)
                self._log_file.flush()
        except Exception as exc:
            logger.debug("[MLScorer] log_prediction failed: %s", exc)

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def shadow_mode(self) -> bool:
        return self._shadow_mode

    @property
    def loaded(self) -> bool:
        return self._loaded

    @property
    def model_version(self) -> Optional[str]:
        return self._model_version

    @property
    def threshold(self) -> float:
        return self._threshold

    def status(self) -> Dict[str, Any]:
        """Return scorer status dict for health monitoring."""
        return {
            "enabled": self._enabled,
            "shadow_mode": self._shadow_mode,
            "hard_block_floor": self._hard_block_floor,
            "loaded": self._loaded,
            "model_version": self._model_version,
            "threshold": self._threshold,
            "load_error": self._load_error,
            "contract_version": self._contract_version,
            "schema_hash": self._schema_hash,
            "feature_columns": FEATURE_COLUMNS,
            "metadata_path": self._metadata_path or None,
            "model_path": self._model_path or None,
            "encoders_path": self._encoders_path or None,
            "last_score_timestamp": self._last_score_timestamp,
        }

    def close(self) -> None:
        """Flush and close the prediction log file."""
        if self._log_file is not None:
            try:
                self._log_file.flush()
                self._log_file.close()
            except Exception:
                pass
            self._log_file = None


# ── Module-level singleton ─────────────────────────────────────────────────────

_scorer: Optional[MLEntryScorer] = None
_scorer_lock = threading.Lock()


def get_ml_scorer() -> MLEntryScorer:
    """
    Return the module-level MLEntryScorer singleton.

    The scorer is constructed from settings on first call and cached.
    This ensures the model is loaded exactly once at bot startup, never
    reloaded or modified during a live trading session.

    If settings cannot be imported (e.g. unit tests), a no-op disabled scorer
    is returned.
    """
    global _scorer
    if _scorer is not None:
        return _scorer
    with _scorer_lock:
        if _scorer is not None:
            return _scorer
        try:
            from app.core.config import settings
            _scorer = MLEntryScorer(
                model_path=settings.ML_MODEL_PATH,
                encoders_path=settings.ML_ENCODERS_PATH,
                metadata_path=settings.ML_METADATA_PATH,
                score_threshold=settings.ML_SCORE_THRESHOLD,
                shadow_mode=settings.ML_SHADOW_MODE,
                enabled=settings.ML_ENABLED,
                log_dir=settings.ML_LOG_DIR,
                hard_block_floor=getattr(settings, "ML_HARD_BLOCK_FLOOR", 0.0),
            )
        except Exception as exc:
            logger.warning("[MLScorer] Failed to construct from settings: %s — using disabled scorer", exc)
            _scorer = MLEntryScorer(enabled=False)
    return _scorer


# ── Helpers ───────────────────────────────────────────────────────────────────

def _dict_hash(d: Optional[Dict]) -> Optional[str]:
    """Lightweight deterministic hash of a feature dict for log reconstruction."""
    if d is None:
        return None
    try:
        import hashlib
        canonical = json.dumps(d, sort_keys=True, default=str)
        return hashlib.md5(canonical.encode()).hexdigest()[:12]
    except Exception:
        return None


def _is_approved_runtime_model_path(model_file: Path) -> bool:
    """
    Runtime safety rail: live/shadow scorer artifacts must live under
    models/production. Dataset files, legacy backtest artifacts, and experiment
    outputs are intentionally outside that path so they cannot be loaded by
    accident if ML_ENABLED is turned on later.
    """
    parts = {part.lower() for part in model_file.parts}
    return "models" in parts and APPROVED_RUNTIME_MODEL_DIR in parts
