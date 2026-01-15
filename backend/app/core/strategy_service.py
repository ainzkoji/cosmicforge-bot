import json
import uuid
import logging
from typing import Optional, List, Dict, Any
from app.persistence.db import DB, utc_now_iso

logger = logging.getLogger(__name__)

# =========================
# Domain Models
# =========================
class Strategy:
    def __init__(self, data: Dict[str, Any]):
        self.id = data["id"]
        self.owner_id = data.get("owner_id")
        self.visibility = data["visibility"]
        self.status = data["status"]
        self.name = data["name"]
        self.description = data.get("description", "")
        self.market_types = json.loads(data["market_types"]) if isinstance(data["market_types"], str) else data["market_types"]
        self.tags = json.loads(data["tags"]) if isinstance(data["tags"], str) else data["tags"]
        self.entitlement_tier = data.get("entitlement_tier", "free")
        self.created_at = data["created_at"]
        self.updated_at = data["updated_at"]

# =========================
# Service
# =========================
class StrategyService:
    def __init__(self, db: DB):
        self.db = db

    def list_strategies(self, user_id: str = None, filters: Dict[str, Any] = None, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """
        List strategies visible to the user (Official + Community + Own Private).
        """
        filters = filters or {}
        params = []
        
        # Base query: Official OR Community OR (Private AND Owned by User)
        # We start with visibility check
        where_clauses = [
            "((s.visibility = 'official' OR s.visibility = 'community') OR (s.owner_id = ?))"
        ]
        params.append(user_id if user_id else "")

        # Filters
        if filters.get("market_type"):
            # market_types is JSON array, use LIKE for simple check (sqlite json support varies)
            where_clauses.append("s.market_types LIKE ?")
            params.append(f'%"{filters["market_type"]}"%')
            
        if filters.get("tag"):
            where_clauses.append("s.tags LIKE ?")
            params.append(f'%"{filters["tag"]}"%')

        if filters.get("risk_style"):
            where_clauses.append("s.recommended_risk_style = ?")
            params.append(filters["risk_style"])

        where_clause = " AND ".join(where_clauses)

        query = f"""
            SELECT s.*, MAX(sv.version_number) as latest_version
            FROM strategies s
            LEFT JOIN strategy_versions sv ON s.id = sv.strategy_id
            WHERE {where_clause}
            GROUP BY s.id 
            ORDER BY s.updated_at DESC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])

        with self.db.connect() as conn:
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                # Convert sqlite3.Row to dict
                strategy = {key: row[key] for key in row.keys()}
                # Parse JSON fields
                try:
                    strategy["market_types"] = json.loads(strategy["market_types"] or "[]")
                    strategy["timeframes"] = json.loads(strategy["timeframes"] or "[]")
                    strategy["tags"] = json.loads(strategy["tags"] or "[]")
                    strategy["metrics_json"] = json.loads(strategy["metrics_json"] or "{}")
                    strategy["constraints_json"] = json.loads(strategy["constraints_json"] or "{}")
                except:
                    pass
                results.append(strategy)
            
            # MERGE WITH SYSTEM STRATEGIES (StrategyRegistry)
            # This ensures hardcoded python strategies appear in the catalog
            from app.strategy.strategy_framework import StrategyRegistry
            
            system_strategies = StrategyRegistry.list_all()
            for sys_strat in system_strategies:
                # Check filters
                if filters and filters.get("market_type") and "FUTURE" not in ["FUTURE"]: # TODO: actual type check
                     pass # Simplified for now
                
                # Check if already in DB results (override or skip?)
                # We assume system IDs are unique.
                if any(r["id"] == sys_strat.strategy_id for r in results):
                    continue
                    
                schema = sys_strat.get_parameter_schema()
                
                # Convert to dict format matching DB row
                results.append({
                    "id": sys_strat.strategy_id,
                    "owner_id": "system",
                    "visibility": "official",
                    "status": "active",
                    "name": sys_strat.name,
                    "description": sys_strat.description,
                    "market_types": ["FUTURE", "SPOT"], # Default to both
                    "timeframes": ["1m", "5m", "15m", "1h", "4h", "1d"],
                    "tags": [sys_strat.family.value],
                    "entitlement_tier": "free",
                    "recommended_risk_style": "balanced",
                    "constraints_json": {},
                    "metrics_json": {},
                    "created_at": utc_now_iso(),
                    "updated_at": utc_now_iso(),
                    "latest_version": 1,
                    # We inject schema here so frontend can use it immediately for system strategies
                    "param_schema_json": schema 
                })

            return results

    def get_strategy(self, strategy_id: str, user_id: str = None) -> Optional[Dict[str, Any]]:
        with self.db.connect() as conn:
            # 1. Get Strategy Metadata
            row = conn.execute("SELECT * FROM strategies WHERE id = ?", (strategy_id,)).fetchone()
            
            # If not in DB, check System Registry
            if not row:
                from app.strategy.strategy_framework import StrategyRegistry
                sys_strat = StrategyRegistry.get(strategy_id)
                if sys_strat:
                     # Construct system strategy dict
                     schema = sys_strat.get_parameter_schema()
                     return {
                        "id": sys_strat.strategy_id,
                        "owner_id": "system",
                        "visibility": "official",
                        "status": "active",
                        "name": sys_strat.name,
                        "description": sys_strat.description,
                        "market_types": ["FUTURE", "SPOT"],
                        "timeframes": ["1m", "5m", "15m", "1h", "4h", "1d"],
                        "tags": [sys_strat.family.value],
                        "entitlement_tier": "free",
                        "recommended_risk_style": "balanced",
                        "constraints_json": {},
                        "metrics_json": {},
                        "created_at": utc_now_iso(),
                        "updated_at": utc_now_iso(),
                        "versions": [{
                            "id": f"{sys_strat.strategy_id}_v1",
                            "version_number": 1,
                            "spec_json": {}, # Logic is code-based
                            "param_schema_json": schema,
                            "changelog": "System Strategy",
                            "created_at": utc_now_iso()
                        }]
                     }
                return None
            
            strategy = dict(row)
            
            # Access Control: If private and not owner, deny
            if strategy["visibility"] == "private" and strategy["owner_id"] != user_id:
                return None

            # Parse JSON
            try:
                strategy["market_types"] = json.loads(strategy["market_types"])
                strategy["timeframes"] = json.loads(strategy["timeframes"])
                strategy["tags"] = json.loads(strategy["tags"])
            except:
                pass

            # 2. Get Versions
            v_rows = conn.execute(
                "SELECT * FROM strategy_versions WHERE strategy_id = ? ORDER BY version_number DESC", 
                (strategy_id,)
            ).fetchall()
            
            versions = []
            for v in v_rows:
                ver = dict(v)
                try:
                    ver["spec_json"] = json.loads(ver["spec_json"])
                    ver["param_schema_json"] = json.loads(ver["param_schema_json"])
                except:
                    pass
                versions.append(ver)

            strategy["versions"] = versions
            return strategy

    def create_strategy(self, user_id: str, data: Dict[str, Any]) -> str:
        """
        Create a new strategy (Draft).
        """
        strategy_id = f"strat_{uuid.uuid4().hex[:12]}"
        now = utc_now_iso()

        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO strategies (
                    id, owner_id, visibility, status, name, description, 
                    market_types, timeframes, tags, entitlement_tier, 
                    recommended_risk_style, constraints_json, metrics_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    strategy_id,
                    user_id,
                    data.get("visibility", "private"),
                    "draft",
                    data.get("name", "Untitled Strategy"),
                    data.get("description", ""),
                    json.dumps(data.get("market_types", [])),
                    json.dumps(data.get("timeframes", [])),
                    json.dumps(data.get("tags", [])),
                    data.get("entitlement_tier", "free"),
                    data.get("recommended_risk_style", "moderate"),
                    json.dumps(data.get("constraints_json", {})),
                    json.dumps({}), # Empty metrics initially
                    now,
                    now
                )
            )
            
            # Create initial version 1 (Empty spec or provided)
            # If spec is provided, we use it, otherwise empty object
            spec = data.get("spec", data.get("initial_version", {}))
            schema = data.get("schema", data.get("param_schema", {}))
            
            conn.execute(
                """
                INSERT INTO strategy_versions (
                    id, strategy_id, version_number, spec_json, param_schema_json, changelog, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"{strategy_id}_v1",
                    strategy_id,
                    1,
                    json.dumps(spec), 
                    json.dumps(schema), 
                    "Initial creation",
                    now
                )
            )
        
        return strategy_id

    def update_strategy_spec(self, strategy_id: str, user_id: str, spec: Dict[str, Any]) -> str:
        """
        Save a new version of the strategy.
        """
        # Logic: Check ownership, increment version, insert new version row
        with self.db.connect() as conn:
            # Check owner
            row = conn.execute("SELECT owner_id FROM strategies WHERE id = ?", (strategy_id,)).fetchone()
            if not row or row["owner_id"] != user_id:
                raise ValueError("Unauthorized")

            # Get latest version
            last_ver = conn.execute(
                "SELECT max(version_number) as num FROM strategy_versions WHERE strategy_id = ?",
                (strategy_id,)
            ).fetchone()
            next_ver = (last_ver["num"] or 0) + 1
            now = utc_now_iso()

            conn.execute(
                """
                INSERT INTO strategy_versions (
                    id, strategy_id, version_number, spec_json, param_schema_json, changelog, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"{strategy_id}_v{next_ver}",
                    strategy_id,
                    next_ver,
                    json.dumps(spec.get("logic", {})),
                    json.dumps(spec.get("schema", {})),
                    spec.get("changelog", "Updated spec"),
                    now
                )
            )
            
            conn.execute("UPDATE strategies SET updated_at = ? WHERE id = ?", (now, strategy_id))
            
            return str(next_ver)
