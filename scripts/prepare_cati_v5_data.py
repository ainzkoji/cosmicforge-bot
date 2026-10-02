"""Verify immutable development rows once; add label event times and mmap arrays."""
import argparse,hashlib,json,sys
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
import numpy as np
from app.trading_intelligence.hashing import TextHasher
from app.trading_intelligence.forecast.artifact import manifest_hash_of


def main(a):
    cache=Path(a.cache); library=Path(a.library); out=Path(a.output); out.mkdir(parents=True,exist_ok=False)
    meta=json.loads((cache/'metadata.json').read_text()); manifest=json.loads((library/'manifest.json').read_text())
    assert manifest_hash_of(manifest)==manifest['manifest_hash']
    assert meta['parent_library_hash']==manifest['library_hash']
    assert hashlib.sha256((cache/'development.npz').read_bytes()).hexdigest()==meta['cache_sha256']
    with np.load(cache/'development.npz',allow_pickle=False) as data:
        ids=data['label_ids']; index={k:i for i,k in enumerate(ids)}
        for k in data.files:
            if k!='label_ids': np.save(out/f'{k}.npy',data[k],allow_pickle=False)
    del ids
    event=np.zeros(meta['samples'],dtype=np.uint8); seen=np.zeros(meta['samples'],dtype=bool)
    h=TextHasher(); count=0
    with (library/'rows.jsonl').open(encoding='utf-8',newline='') as f:
        for i,line in enumerate(f):
            h.update(line); count+=1
            if i%12: continue
            label=json.loads(line)['label']; ix=index[label['label_id']]
            if label['decision_time']+48*900000>=meta['holdout_start_ms']: raise ValueError('holdout overlap')
            terminal=label['terminal_outcome']
            raw=(label['time_to_target_bars'] if terminal=='TARGET_BEFORE_STOP' else
                 label['time_to_stop_bars'] if terminal=='STOP_BEFORE_TARGET' else 47)
            # Frozen labels use a zero-based first-touch bar index. The model's
            # elapsed-bar support is 1..48; this is a unit conversion, not relabeling.
            if raw is None or int(raw)!=raw or not 0<=raw<48: raise ValueError('invalid event label')
            duration=int(raw)+1
            if seen[ix]: raise ValueError('duplicate sampled label')
            event[ix]=duration; seen[ix]=True
    assert count==manifest['row_count'] and h.hexdigest()==manifest['rows_sha256'] and seen.all()
    np.save(out/'event_bars.npy',event,allow_pickle=False)
    meta.update(event_time_source='Frozen zero-based first-touch index +1 elapsed bars; timeout administrative censoring at horizon 48',
                v4_cache_sha256=meta.pop('cache_sha256'),array_sha256={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in out.glob('*.npy')})
    (out/'metadata.json').write_text(json.dumps(meta,indent=2)); print(json.dumps(dict(samples=meta['samples'],verified_rows=count,holdout_query_count=0)),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--cache',required=True); p.add_argument('--library',required=True); p.add_argument('--output',required=True); main(p.parse_args())
