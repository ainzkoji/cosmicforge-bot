import React from 'react';
import { NewsSignal } from '../../api/newsIntelligenceApi';

interface ShadowSignalPanelProps {
  signals: NewsSignal[];
  loading: boolean;
}

export const ShadowSignalPanel: React.FC<ShadowSignalPanelProps> = ({ signals, loading }) => {
  if (loading) return <div className="p-4 text-center text-gray-500">Loading shadow signals...</div>;

  return (
    <div className="bg-white dark:bg-gray-800 shadow rounded-lg overflow-hidden flex flex-col h-full border-2 border-orange-400 dark:border-orange-500 relative">
      {/* SHADOW MODE WARNING BANNER */}
      <div className="bg-orange-100 dark:bg-orange-900/50 text-orange-800 dark:text-orange-200 px-4 py-2 text-center text-xs font-bold uppercase tracking-widest border-b border-orange-200 dark:border-orange-800">
        ⚠️ Shadow Only — Not Used For Live Trading
      </div>
      
      <div className="px-4 py-4 sm:px-6 border-b border-gray-200 dark:border-gray-700 flex justify-between items-center">
        <h3 className="text-lg leading-6 font-medium text-gray-900 dark:text-white">Generated Intelligence Signals</h3>
      </div>
      
      <div className="overflow-y-auto flex-1 p-4 space-y-4">
        {signals.length === 0 ? (
          <div className="text-center text-gray-500 py-8">No news intelligence signals have been generated yet.</div>
        ) : (
          signals.map(signal => {
            const isBullish = (signal.sentiment_score || 0) > 0;
            return (
              <div key={signal.id} className={`p-3 rounded border ${isBullish ? 'border-green-200 bg-green-50 dark:bg-green-900/10 dark:border-green-800' : 'border-red-200 bg-red-50 dark:bg-red-900/10 dark:border-red-800'}`}>
                <div className="flex justify-between items-start mb-2">
                  <div className="flex items-center gap-2">
                    <span className="font-bold text-gray-900 dark:text-gray-100">{signal.symbol || 'GLOBAL'}</span>
                    <span className={`px-2 py-0.5 rounded text-xs font-bold ${isBullish ? 'bg-green-200 text-green-800' : 'bg-red-200 text-red-800'}`}>
                      {signal.signal_type}
                    </span>
                  </div>
                  <span className="text-xs text-gray-500">{new Date(signal.created_at).toLocaleTimeString()}</span>
                </div>
                
                <div className="flex flex-wrap gap-2 text-xs">
                  {signal.narrative_type && (
                    <span className="text-gray-600 dark:text-gray-400 font-medium">Narrative: {signal.narrative_type}</span>
                  )}
                  {signal.severity_level && (
                    <span className="text-gray-600 dark:text-gray-400 font-medium">Severity: {signal.severity_level}</span>
                  )}
                  <span className="text-gray-600 dark:text-gray-400 font-medium">
                    Sentiment: {signal.sentiment_score !== null ? signal.sentiment_score.toFixed(2) : 'N/A'}
                  </span>
                </div>
                
                <div className="mt-2 text-[10px] text-gray-500 bg-white/50 dark:bg-black/20 p-2 rounded">
                  <span className="font-semibold">Suppression Reason: </span>
                  {signal.suppression_reason || 'SHADOW_MODE_ENFORCED'}
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
};
