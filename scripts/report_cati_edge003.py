"""Report only the one published run; never generate or re-evaluate candidates."""
import gzip
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'docs/research/cati_edge003'
CACHE=ROOT/'data/research/edge003_inputs'

def main():
    result=json.loads((OUT/'results.json').read_text())
    source=json.loads((OUT/'source_causality_audit.json').read_text())
    if result['evaluation_runs']!=1 or result['registry_hash']!=source['registry_hash']:raise ValueError('ONE_RUN_IDENTITY_REQUIRED')
    labels=[json.loads(x) for x in gzip.decompress((OUT/'selected_portfolio_labels.jsonl.gz').read_bytes()).splitlines()]
    if len({x['label_id'] for x in labels})!=len(labels):raise ValueError('DUPLICATE_LABEL')
    for name,h in result['outcome_population_hashes'].items():
        assert hashlib.sha256((OUT/name).read_bytes()).hexdigest()==h
    durations=[];adverses=[]
    for row in labels:
        assert row['exit_time']<1783876499999
        durations.append({'family':row['family'],'label_id':row['label_id'],'holding_hours':(row['exit_time']-row['entry_time']+1)/3600000})
        if len(row['legs'])==2:
            bound=0.
            for s,w,sgn,entry in zip(row['legs'],row['weights'],(1,-1),row['entry_prices']):
                a=np.load(CACHE/(s+'.npy'),mmap_mode='r')
                first=np.searchsorted(a[:,0],row['entry_time']);last=np.searchsorted(a[:,0],row['exit_time']+1)
                path=a[first:last]
                if len(path)==0 or not np.array_equal(path[:,0],row['entry_time']+np.arange(len(path))*900000):raise ValueError('DIAGNOSTIC_LEG_PATH_GAP')
                worst=path[:,3].min() if sgn>0 else path[:,2].max()
                bound=max(bound,-sgn*w*(worst-entry)/entry/row['risk'])
            adverses.append({'family':row['family'],'label_id':row['label_id'],'largest_one_leg_adverse_15m_bound_R':float(bound),'not_a_basket_stop_touch':True})
    pd.DataFrame(adverses,columns=['family','label_id','largest_one_leg_adverse_15m_bound_R','not_a_basket_stop_touch']).to_csv(OUT/'paired_leg_adverse_bounds.csv',index=False)
    pd.DataFrame(durations).to_csv(OUT/'holding_durations.csv',index=False)
    summaries={}
    for family,v in result['families'].items():
        selected=[x for x in labels if x['family']==family]
        if len(selected)!=v['selected_portfolio_trades']:raise ValueError('SELECTED_COUNT_MISMATCH')
        ordered=sorted(selected,key=lambda x:x['entry_time'])
        assert all(a['exit_time']<b['entry_time'] for a,b in zip(ordered,ordered[1:]))
        days=np.array([x['decision_time']//86400000 for x in selected]);counts=np.unique(days,return_counts=True)[1]
        summaries[family]={'selected_day_cluster_sizes':{'mean':float(counts.mean()) if len(counts) else None,'max':int(counts.max()) if len(counts) else None},
            'actual_nonoverlap_verified':True,'actual_nonoverlapping_trades':len(selected),'portfolio_admission_count_not_automatically_satisfied':True,
            'largest_one_leg_adverse_15m_bound_R':max([x['largest_one_leg_adverse_15m_bound_R'] for x in adverses if x['family']==family],default=None),
            'holding_hours_median':float(np.median([x['holding_hours'] for x in durations if x['family']==family])) if selected else None}
    (OUT/'portfolio_realism.json').write_text(json.dumps(summaries,indent=2))
    families=list(result['families']);x=np.arange(3);fig,ax=plt.subplots(1,2,figsize=(11,4.4),layout='constrained')
    for offset,key,label,color in [(-.24,'gross_R','Gross','#2563eb'),(0,'net_R','Net','#dc2626'),(.24,'net_2x_cost_R','Net2x costs','#f59e0b')]:
        vals=[result['families'][f]['pooled'][key] for f in families]
        ax[0].bar(x+offset,[0 if v is None else v for v in vals],width=.24,label=label,color=color)
    ax[0].axhline(0,color='#475569',lw=.7);ax[0].set_xticks(x,['Residual','Dispersion','Carry']);ax[0].set_ylabel('Selected portfolio mean R');ax[0].legend(frameon=False)
    for i,f in enumerate(families):
        values=[v['net_lower_95_R'] for v in result['families'][f]['folds']]
        ax[1].plot(range(1,6),[np.nan if v is None else v for v in values],marker='o',label=['Residual','Dispersion','Carry'][i])
    ax[1].axhline(0,color='#475569',lw=.7);ax[1].set_xticks(range(1,6));ax[1].set_xlabel('Frozen chronological fold');ax[1].set_ylabel('Selected net R: UTC-day clustered95% lower bound');ax[1].legend(frameon=False)
    fig.suptitle('CATI Mandate003 · one registered run · no holdout or execution',fontsize=12)
    fig.savefig(OUT/'registered_economics.png',dpi=180)
    foldrows=[]
    for f,v in result['families'].items():
        for k,m in enumerate(v['folds'],1):foldrows.append(dict(family=f,fold=k,population='SELECTED_PORTFOLIO',**m))
        for k,m in enumerate(v['counterfactual_folds'],1):foldrows.append(dict(family=f,fold=k,population='COUNTERFACTUAL_DIAGNOSTIC',**m))
    pd.DataFrame(foldrows).to_csv(OUT/'fold_metrics.csv',index=False)
    notebook={'nbformat':4,'nbformat_minor':5,'metadata':{'kernelspec':{'display_name':'Python3','language':'python','name':'python3'}},'cells':[
      {'cell_type':'markdown','metadata':{},'source':['# CATI frozen Mandate003 results\nOne registered run only. This notebook reads artifacts and never executes the evaluator.\nCounterfactuals are not selected portfolio trades; paired families remain runtime-capability blocked.']},
      {'cell_type':'code','execution_count':None,'metadata':{},'outputs':[],'source':['from pathlib import Path\nimport json, gzip, hashlib\nimport pandas as pd\nroot=next(p for p in [Path.cwd(),*Path.cwd().parents] if (p/"docs/research/cati_edge003/results.json").exists())\nout=root/"docs/research/cati_edge003"\nr=json.loads((out/"results.json").read_text())\nassert r["evaluation_runs"]==1 and r["holdout_query_count"]==0\nfor name,h in r["outcome_population_hashes"].items(): assert hashlib.sha256((out/name).read_bytes()).hexdigest()==h\npd.DataFrame({f:v["pooled"] for f,v in r["families"].items()}).T']},
      {'cell_type':'code','execution_count':None,'metadata':{},'outputs':[],'source':['labels=[json.loads(x) for x in gzip.decompress((out/"selected_portfolio_labels.jsonl.gz").read_bytes()).splitlines()]\nassert len({x["label_id"] for x in labels})==len(labels)\nfor family,v in r["families"].items():\n    rows=sorted([x for x in labels if x["family"]==family],key=lambda x:x["entry_time"])\n    assert len(rows)==v["selected_portfolio_trades"]\n    assert all(a["exit_time"]<b["entry_time"] for a,b in zip(rows,rows[1:]))\npd.read_csv(out/"fold_metrics.csv")']},
      {'cell_type':'markdown','metadata':{},'source':['Funding cashflows use historical settled rates and a latest causal hourly mark notional proxy; they are not broker payment receipts.\nBasis attribution is inside price return, not an extra return. PRICE_R+BASIS_R+FUNDING_R=GROSS_R for carry.\nLeg excursions use native15m bounds solely for diagnostics, never to infer paired basket stop/target touches.']} ]}
    namespace={}
    for i,cell in enumerate(notebook['cells']):
        if cell['cell_type']=='code':
            exec(''.join(cell['source']),namespace);cell['execution_count']=i;cell['outputs']=[{'output_type':'stream','name':'stdout','text':['Artifact identity/nonoverlap checks passed.\n']}]
    (OUT/'mandate003_results.ipynb').write_text(json.dumps(notebook,indent=2))
    print('published selected population inspected',len(labels),'no second evaluation')

if __name__=='__main__':main()
