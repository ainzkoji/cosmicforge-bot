import sqlite3, json
DB='bot.db'
START='2026-05-09T05:01:49.220730+00:00'
END='2026-05-10T17:03:27.676744+00:00'
ALLOW=['coindesk.com','cointelegraph.com','decrypt.co','bitcoinmagazine.com']
ALLOW_PROVIDERS=[f'rss:{sid}' for sid in ALLOW]

conn=sqlite3.connect(DB)
conn.row_factory=sqlite3.Row

out={}
out['raw_news_items']= [dict(r) for r in conn.execute(
    'select * from raw_news_items where ingested_utc>=? and ingested_utc<=? and provider in (%s) order by id asc' % (','.join('?'*len(ALLOW_PROVIDERS))),
    (START, END, *ALLOW_PROVIDERS)
)]
out['news_clusters']= [dict(r) for r in conn.execute(
    'select * from news_clusters where first_seen_utc>=? and first_seen_utc<=? and first_seen_provider in (%s) order by id asc' % (','.join('?'*len(ALLOW_PROVIDERS))),
    (START, END, *ALLOW_PROVIDERS)
)]
out['news_intelligence_signals']= [dict(r) for r in conn.execute(
    'select * from news_intelligence_signals where created_at>=? and created_at<=? order by id asc',
    (START, END)
)]

health_rows=[dict(r) for r in conn.execute(
    'select * from news_provider_health where created_at>=? and created_at<=? and source_id in (%s) order by id asc' % (','.join('?'*len(ALLOW))),
    (START, END, *ALLOW)
)]
latest={}
for r in health_rows:
    latest[r['source_id']]=r
out['news_provider_health']=list(latest.values())

narr_tables=[row['name'] for row in conn.execute("select name from sqlite_master where type='table' and name like '%narr%'")]
dup_narr=[]
for t in narr_tables:
    cols=[c[1] for c in conn.execute(f'pragma table_info({t})')]
    if 'cluster_id' in cols and 'narrative' in cols:
        q=f"""
        select cluster_id, narrative, count(1) as n
        from {t}
        where created_at>=? and created_at<=?
        group by cluster_id, narrative
        having n>1
        order by n desc
        """
        try:
            for r in conn.execute(q,(START,END)):
                dup_narr.append({'table':t, **dict(r)})
        except sqlite3.OperationalError:
            pass
out['duplicate_narrative_rows']=dup_narr

unsafe=[r for r in out['news_intelligence_signals'] if (r.get('shadow_only')!=1 or r.get('should_affect_trading')!=0)]
out['unsafe_news_signals']=unsafe

print(json.dumps({'window':{'start_utc':START,'end_utc':END}, **out}, indent=2))
