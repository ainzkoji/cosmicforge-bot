"""Compact V3 artifact verification behind the canonical library loader.

Development evidence cannot grant execution authority. Runtime loading remains
closed until a separate governance checkpoint approves this artifact format.
"""
from __future__ import annotations
from dataclasses import dataclass
import json
from pathlib import Path

from app.trading_intelligence.forecast.artifact import LibraryArtifactError, row_from_dict
from app.trading_intelligence.forecast.information_conditioning import InformationConditioner, FEATURE_SCHEMA, ESTIMATOR
from app.trading_intelligence.hashing import stable_hash


@dataclass(frozen=True)
class V3Library:
    library_hash: str
    library_id: str
    conditioner: InformationConditioner
    rows: tuple
    calibration_status: str
    training_label_end: int
    horizon: int = 48
    timeframe: str = "15m"
    library_version: str = "3.0.0-research-information-conditioning"
    source_kind: str = "REAL_MARKET"

    def __len__(self):
        return len(self.rows)


def load_v3_artifact(path, *, expected_hash=None, mode="RUNTIME"):
    root=Path(path)
    try:
        d=json.loads((root/'v3_model.json').read_text(encoding='utf-8'))
        analytical={k:v for k,v in d.items() if k not in ('library_hash','candidate_id')}
        if stable_hash(analytical)!=d['library_hash']:
            raise ValueError('V3 artifact hash mismatch')
        if d['candidate_id']!='cati_v3_'+d['library_hash'][:24]:
            raise ValueError('unknown V3 candidate identity')
        if expected_hash is not None and expected_hash!=d['library_hash']:
            raise ValueError('V3 configured identity mismatch')
        if d['source_tree_dirty'] is not False:
            raise ValueError('dirty V3 code provenance')
        if len(d['code_revision'])!=40 or any(c not in '0123456789abcdef' for c in d['code_revision']):
            raise ValueError('invalid V3 code revision')
        repo=Path(__file__).resolve().parents[5]
        registry=json.loads((repo/'docs/research/cati_v3_research_registry.json').read_text())
        if d['registry_hash']!=stable_hash(registry) or d['role']!=registry['role']:
            raise ValueError('unregistered V3 candidate')
        if d['dataset_manifest_hash']!=registry['dataset_manifest_hash'] or d['parent_library_hash']!=registry['parent_library_hash']:
            raise ValueError('wrong V3 dataset identity')
        if d['feature_schema']!=FEATURE_SCHEMA or d['estimator']!=ESTIMATOR:
            raise ValueError('wrong V3 feature schema/estimator')
        if d['training_label_end']>=d['holdout_start_ms']:
            raise ValueError('V3 holdout overlap')
        m=d['metrics']; passed=(m['samples']>=300 and m['brier_skill']>=.02 and m['ece']<=.05
                               and len(d['folds'])==5 and all(f['brier_skill']>0 for f in d['folds']))
        if d['calibration_status']!=('CALIBRATED' if passed else 'RESEARCH_ONLY'):
            raise ValueError('V3 calibration status mismatch')
        if mode!='DEVELOPMENT':
            raise ValueError('V3 RESEARCH_ONLY governance: runtime format not approved; calibration cannot grant authority')
        if d['runtime_eligible'] is not False:
            raise ValueError('V3 development artifact cannot claim runtime authority')
        model=InformationConditioner.from_dict(d['model'])
        if model.regularization not in [v['C'] for v in registry['variants']]:
            raise ValueError('unregistered V3 regularization')
        rows=tuple(row_from_dict(r) for r in d['reference_rows'])
        if not rows or len(rows)>2048 or any(r.label.decision_time+48*900000>=d['holdout_start_ms'] for r in rows):
            raise ValueError('invalid V3 reference distribution')
    except (OSError,ValueError,KeyError,TypeError) as exc:
        raise LibraryArtifactError(str(exc)) from exc
    # Even passing development metrics remain RESEARCH_ONLY at the forecast boundary.
    return V3Library(d['library_hash'],d['candidate_id'],model,rows,'RESEARCH_ONLY',d['training_label_end']),d
