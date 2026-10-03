"""Verify fresh V2 row identity/timestamps and diagnostic union without price queries."""
import argparse,gzip,hashlib,json
from pathlib import Path
from evaluate_cati_alpha_v2 import HOLDOUT_START_MS,metrics
from app.trading_intelligence.hashing import short_id


def main(args):
    root=Path(args.artifact);report=json.loads((root/'report.json').read_text())
    ids=set();records=[];digest=hashlib.sha256()
    labels=Path(args.labels) if args.labels else root/'labels.jsonl'
    if not labels.exists(): labels=root/'labels.jsonl.gz'
    opener=gzip.open if labels.suffix=='.gz' else open
    with opener(labels,'rb') as stream:
        for line in stream:
            digest.update(line);d=json.loads(line)
            if d['label_id'] in ids: raise ValueError('duplicate label identity')
            ids.add(d['label_id']);records.append(d)
            if d['decision_time']+d['horizon']*900000>=HOLDOUT_START_MS:
                raise ValueError('holdout label overlap')
            for name in ('closed_1h_at','closed_4h_at'):
                if d[name] is not None and d[name]>d['decision_time']:
                    raise ValueError('future HTF context')
            identity=dict(d['candidate']);identity.pop('schema_version')
            identity.update(identity.pop('instrument_key'))
            if identity['decision_time']!=d['decision_time'] or short_id('setc',identity)!=d['setup_candidate_id']:
                raise ValueError('canonical candidate identity')
    if digest.hexdigest()!=report['labels_sha256']: raise ValueError('label stream content hash')
    if len(records)!=sum(v['pooled']['samples'] for v in report['results'].values()):
        raise ValueError('label/report sample count')
    result=dict(rows=len(records),unique_label_ids=len(ids),labels_sha256=digest.hexdigest(),holdout_overlap=False,
        HTF_future_overlap=False,canonical_candidate_ids_verified=True,diagnostic_union=metrics(records),
        diagnostic_union_folds=[metrics([d for d in records if d['fold']==i]) for i in range(1,6)])
    if labels.suffix=='.gz':
        compressed=hashlib.sha256()
        with labels.open('rb') as stream:
            for chunk in iter(lambda:stream.read(1048576),b''): compressed.update(chunk)
        result['compressed_labels_sha256']=compressed.hexdigest()
        result['compressed_labels_bytes']=labels.stat().st_size
        result['compression']='deterministic gzip mtime0, filename empty'
    Path(args.output).write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(dict(rows=len(records),identity_verified=True,holdout_queries=0)))
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--artifact',required=True);p.add_argument('--output',required=True);p.add_argument('--labels')
    main(p.parse_args())
