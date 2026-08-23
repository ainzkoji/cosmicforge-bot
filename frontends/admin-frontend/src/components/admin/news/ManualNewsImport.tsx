import React, { useState } from 'react';
import { newsAdminApi, ManualImportResult } from '../../../api/newsIntelligenceApi';

const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'SOLUSDT', 'BNBUSDT', 'ADAUSDT',
                 'DOGEUSDT', 'MATICUSDT', 'AVAXUSDT', 'LINKUSDT'];

const _input = (style?: React.CSSProperties): React.CSSProperties => ({
  width: '100%', boxSizing: 'border-box',
  background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.1)',
  borderRadius: '8px', padding: '10px 14px', color: '#e2e8f0', fontSize: '13px',
  outline: 'none', ...style,
});

export const ManualNewsImport: React.FC = () => {
  const [title, setTitle]       = useState('');
  const [body, setBody]         = useState('');
  const [sourceName, setSource] = useState('');
  const [sourceUrl, setUrl]     = useState('');
  const [published, setPublished] = useState('');
  const [symbols, setSymbols]   = useState<string[]>([]);
  const [category, setCategory] = useState('CRYPTO');
  const [loading, setLoading]   = useState(false);
  const [result, setResult]     = useState<ManualImportResult | null>(null);
  const [error, setError]       = useState<string | null>(null);

  const toggleSymbol = (sym: string) =>
    setSymbols(prev => prev.includes(sym) ? prev.filter(s => s !== sym) : [...prev, sym]);

  const submit = async () => {
    if (!title.trim()) { setError('Title is required'); return; }
    setLoading(true); setError(null); setResult(null);
    try {
      const res = await newsAdminApi.importManual({
        title: title.trim(),
        body_snippet: body.trim(),
        source_name: sourceName.trim() || 'Manual',
        source_url: sourceUrl.trim(),
        published_utc: published || new Date().toISOString(),
        affected_symbols: symbols,
        category,
      });
      setResult(res);
      if (!res.error) {
        setTitle(''); setBody(''); setSource('');
        setUrl(''); setPublished(''); setSymbols([]);
      }
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Import failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{
      background: 'rgba(255,255,255,0.03)',
      border: '1px solid rgba(255,255,255,0.07)',
      borderRadius: '14px', padding: '22px',
    }}>
      <div style={{ fontSize: '14px', fontWeight: 700, color: '#e2e8f0', marginBottom: '18px', display: 'flex', alignItems: 'center', gap: '8px' }}>
        <span>✍</span> Manual News Import
        <span style={{ fontSize: '10px', color: '#475569', fontWeight: 400, marginLeft: '4px' }}>Shadow-only — enters pipeline, never affects trading</span>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px', marginBottom: '12px' }}>
        <div>
          <label style={{ fontSize: '11px', color: '#64748b', fontWeight: 600, display: 'block', marginBottom: '4px' }}>HEADLINE *</label>
          <input value={title} onChange={e => setTitle(e.target.value)} placeholder="e.g. Bitcoin hits $100k for the first time" style={_input()} />
        </div>
        <div>
          <label style={{ fontSize: '11px', color: '#64748b', fontWeight: 600, display: 'block', marginBottom: '4px' }}>SOURCE NAME</label>
          <input value={sourceName} onChange={e => setSource(e.target.value)} placeholder="e.g. CoinDesk" style={_input()} />
        </div>
      </div>

      <div style={{ marginBottom: '12px' }}>
        <label style={{ fontSize: '11px', color: '#64748b', fontWeight: 600, display: 'block', marginBottom: '4px' }}>BODY / SUMMARY</label>
        <textarea value={body} onChange={e => setBody(e.target.value)} placeholder="Article summary or body snippet…" rows={3}
          style={{ ..._input(), resize: 'vertical', fontFamily: 'inherit' }} />
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '12px', marginBottom: '12px' }}>
        <div>
          <label style={{ fontSize: '11px', color: '#64748b', fontWeight: 600, display: 'block', marginBottom: '4px' }}>SOURCE URL</label>
          <input value={sourceUrl} onChange={e => setUrl(e.target.value)} placeholder="https://…" style={_input()} />
        </div>
        <div>
          <label style={{ fontSize: '11px', color: '#64748b', fontWeight: 600, display: 'block', marginBottom: '4px' }}>PUBLISHED (UTC)</label>
          <input type="datetime-local" value={published.replace('Z', '').slice(0, 16)} onChange={e => setPublished(e.target.value + ':00Z')} style={_input()} />
        </div>
        <div>
          <label style={{ fontSize: '11px', color: '#64748b', fontWeight: 600, display: 'block', marginBottom: '4px' }}>CATEGORY</label>
          <select value={category} onChange={e => setCategory(e.target.value)} style={{ ..._input(), cursor: 'pointer' }}>
            {['CRYPTO', 'MACRO', 'MARKET', 'GENERAL'].map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
      </div>

      <div style={{ marginBottom: '16px' }}>
        <label style={{ fontSize: '11px', color: '#64748b', fontWeight: 600, display: 'block', marginBottom: '6px' }}>AFFECTED SYMBOLS</label>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
          {SYMBOLS.map(sym => (
            <button key={sym} onClick={() => toggleSymbol(sym)} style={{
              padding: '4px 10px', borderRadius: '6px', fontSize: '11px', fontWeight: 600, cursor: 'pointer', border: 'none', transition: 'all 0.15s',
              background: symbols.includes(sym) ? 'rgba(99,102,241,0.3)' : 'rgba(255,255,255,0.06)',
              color: symbols.includes(sym) ? '#a5b4fc' : '#64748b',
            }}>{sym.replace('USDT', '')}</button>
          ))}
        </div>
      </div>

      {error && (
        <div style={{ background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', borderRadius: '8px', padding: '10px 14px', marginBottom: '12px', color: '#f87171', fontSize: '13px' }}>
          {error}
        </div>
      )}

      {result && !result.error && (
        <div style={{ background: 'rgba(34,197,94,0.08)', border: '1px solid rgba(34,197,94,0.25)', borderRadius: '8px', padding: '12px 16px', marginBottom: '12px', fontSize: '12px', color: '#86efac' }}>
          Imported — raw_id: {result.raw_news_item_id} · cluster_id: {result.cluster_id} · symbols: {result.symbols.join(', ') || '—'} · signals: {result.signals_emitted} · narrative: {result.top_narrative || '—'}
          {result.is_manipulation_suspect && <span style={{ color: '#fbbf24', marginLeft: '8px' }}>⚠ Manipulation suspect</span>}
        </div>
      )}

      <button onClick={submit} disabled={loading || !title.trim()} style={{
        padding: '10px 24px', borderRadius: '9px', border: 'none', cursor: loading || !title.trim() ? 'not-allowed' : 'pointer',
        background: loading || !title.trim() ? 'rgba(99,102,241,0.2)' : 'linear-gradient(135deg, #6366f1, #4f46e5)',
        color: '#f1f5f9', fontSize: '13px', fontWeight: 700, opacity: loading || !title.trim() ? 0.6 : 1, transition: 'all 0.2s',
      }}>
        {loading ? '⟳ Importing…' : '↑ Import into Pipeline'}
      </button>
    </div>
  );
};
