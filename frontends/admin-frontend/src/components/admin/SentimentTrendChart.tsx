import React from 'react';
import { IntelligenceStats } from '../../api/newsIntelligenceApi';

interface SentimentTrendChartProps {
  stats: IntelligenceStats | null;
  loading: boolean;
}

export const SentimentTrendChart: React.FC<SentimentTrendChartProps> = ({ stats, loading }) => {
  if (loading) return <div className="p-4 text-center text-gray-500">Loading sentiment trends...</div>;
  if (!stats || stats.signals.length === 0) return <div className="p-4 text-center text-gray-500">No sentiment data available.</div>;

  return (
    <div className="bg-white dark:bg-gray-800 shadow rounded-lg overflow-hidden flex flex-col h-full">
      <div className="px-4 py-5 sm:px-6 border-b border-gray-200 dark:border-gray-700">
        <h3 className="text-lg leading-6 font-medium text-gray-900 dark:text-white">Sentiment Overview</h3>
      </div>
      <div className="p-4 flex-1 overflow-y-auto">
        <div className="space-y-4">
          {stats.signals.map((sig, idx) => {
            const isBullish = (sig.signal_type || '').includes('BULLISH');
            const displayStrength = Math.min((sig.avg_confidence || 0) * 100, 100);
            return (
              <div key={idx} className="flex flex-col">
                <div className="flex justify-between text-xs font-medium mb-1 text-gray-700 dark:text-gray-300">
                  <span>{sig.symbol || 'GLOBAL'} - {sig.signal_type}</span>
                  <span className={isBullish ? 'text-green-600' : 'text-red-600'}>
                    {isBullish ? 'BULLISH' : 'BEARISH'} ({displayStrength.toFixed(0)}%)
                  </span>
                </div>
                <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2.5">
                  <div 
                    className={`h-2.5 rounded-full ${isBullish ? 'bg-green-500' : 'bg-red-500'}`} 
                    style={{ width: `${displayStrength}%` }}
                  ></div>
                </div>
                <div className="text-[10px] text-gray-500 mt-1 flex justify-between">
                  <span>Count: {sig.signal_count}</span>
                  <span>Conf: {(sig.avg_confidence * 100).toFixed(0)}%</span>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
};
