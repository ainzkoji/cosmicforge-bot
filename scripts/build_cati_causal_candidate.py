"""New research-only library identity; reuse immutable row bytes, no market rebuild.

The version changes because the governed evaluation contract changes. This
does not claim new predictive information. RUNTIME rejects this version.
"""
import argparse, copy, hashlib, json, os, subprocess, sys
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
from app.trading_intelligence.forecast.artifact import CAUSAL_RESEARCH_LIBRARY_VERSION,manifest_hash_of
from app.trading_intelligence.forecast.calibration_report import CausalCalibrationPolicy
from app.trading_intelligence.hashing import TextHasher,stable_hash

def build(parent, output):
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=REPO).strip(),'Requires clean code tree'
    commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
    m=json.loads((parent/'manifest.json').read_text());assert manifest_hash_of(m)==m['manifest_hash']
    ids=[];hasher=TextHasher();content=hashlib.sha256(b'[');previous=None
    for i,line in enumerate((parent/'rows.jsonl').open(encoding='utf-8',newline='')):
        hasher.update(line);raw=line.rstrip('\n');d=json.loads(raw);lid=d['label']['label_id']
        assert previous is None or lid>previous;previous=lid;ids.append(lid)
        assert d['label']['decision_time']+d['label']['terminal_horizon_bars']*900000<m['governance']['holdout_start_ms']
        if i:content.update(b',')
        content.update(raw.encode())
    content.update(b']');assert hasher.hexdigest()==m['rows_sha256'] and len(ids)==m['row_count']
    payload={'source_kind':m['source_kind'],'dataset_source_hash':m['source_data_hash'],
        'candidate_generation_versions':m['setup_family_versions'],'label_policy_version':m['label_policy_version'],
        'cost_model_version':m['cost_model_version'],'feature_bucket_schema_version':m['cohort_schema_version'],
        'schema_version':m['library_version'],'row_label_ids':ids,'rows_content_hash':content.hexdigest()}
    assert stable_hash(payload)==m['library_hash']
    payload['schema_version']=CAUSAL_RESEARCH_LIBRARY_VERSION
    new_hash=stable_hash(payload);new=copy.deepcopy(m)
    new.update(library_hash=new_hash,library_id='cati_lib_'+new_hash[:24],library_version=CAUSAL_RESEARCH_LIBRARY_VERSION)
    new['governance'].update(parent_library_hash=m['library_hash'],parent_manifest_hash=m['manifest_hash'],
        library_role='RESEARCH_CANDIDATE_V2_CAUSAL_BASELINE_CORRECTION',code_commit=commit,source_tree_dirty=False,
        calibration_policy_hash=CausalCalibrationPolicy().policy_hash,runtime_eligible=False)
    new['manifest_hash']=manifest_hash_of(new)
    target=output/new['library_id'];assert not target.exists(),'Refusing overwrite'
    target.mkdir(parents=True)
    os.link(parent/'rows.jsonl',target/'rows.jsonl')
    (target/'manifest.json').write_text(json.dumps(new,sort_keys=True,indent=2)+'\n')
    print(json.dumps({'library_id':new['library_id'],'library_hash':new_hash,'parent_library_hash':m['library_hash'],
                      'path':str(target),'row_bytes':'immutable hard link; no data rebuild','code_commit':commit},indent=2))
    return target

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--parent',type=Path,required=True);ap.add_argument('--output',type=Path,required=True)
    args=ap.parse_args();build(args.parent,args.output)
