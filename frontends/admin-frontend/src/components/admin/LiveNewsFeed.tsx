import React from 'react';
import { RawNewsItem } from '../../api/newsIntelligenceApi';

interface LiveNewsFeedProps {
  news: RawNewsItem[];
  loading: boolean;
}

export const LiveNewsFeed: React.FC<LiveNewsFeedProps> = ({ news, loading }) => {
  if (loading) return <div className="p-4 text-center text-gray-500">Loading live feed...</div>;
  if (news.length === 0) {
    return (
      <div className="p-4 text-center text-gray-500">
        No live news has been ingested. News Intelligence is running, but no real news source is currently flowing.
      </div>
    );
  }

  return (
    <div className="bg-white dark:bg-gray-800 shadow rounded-lg overflow-hidden flex flex-col h-full">
      <div className="px-4 py-5 sm:px-6 border-b border-gray-200 dark:border-gray-700 flex justify-between items-center">
        <h3 className="text-lg leading-6 font-medium text-gray-900 dark:text-white">Live News Feed</h3>
        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
          Raw Ingestion
        </span>
      </div>
      <div className="overflow-y-auto flex-1 p-4 space-y-4">
        {news.map((item) => (
          <div key={item.id} className="border-l-4 border-blue-400 pl-4 py-2 flex flex-col gap-1">
            <div className="flex justify-between items-start">
              <a 
                href={item.source_url || '#'} 
                target="_blank" 
                rel="noopener noreferrer"
                className="text-sm font-semibold text-gray-900 dark:text-white hover:underline"
              >
                {item.title}
              </a>
              <span className="text-xs text-gray-500 whitespace-nowrap ml-2">
                {new Date(item.published_utc).toLocaleTimeString()}
              </span>
            </div>
            <div className="flex flex-wrap gap-2 text-xs">
              <span className="text-gray-600 dark:text-gray-400 font-medium">
                {item.provider} / {item.source_domain || item.source_name || 'Unknown Source'}
              </span>
              {item.latency_seconds !== null && (
                <span className={`px-2 py-0.5 rounded flex items-center ${
                  item.latency_seconds > 300 
                    ? 'bg-red-100 text-red-800' 
                    : 'bg-green-100 text-green-800'
                }`}>
                  {item.latency_seconds > 0 ? `${item.latency_seconds.toFixed(1)}s delay` : 'Real-time'}
                </span>
              )}
              {item.is_duplicate === 1 && (
                <span className="px-2 py-0.5 rounded bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-300">
                  Duplicate
                </span>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};
