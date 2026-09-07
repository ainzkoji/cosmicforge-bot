import React from 'react';

interface NarrativeMonitorProps {
  narratives: any[];
  loading: boolean;
}

export const NarrativeMonitor: React.FC<NarrativeMonitorProps> = ({ narratives, loading }) => {
  if (loading) return <div className="p-4 text-center text-gray-500">Loading narratives...</div>;
  if (narratives.length === 0) return <div className="p-4 text-center text-gray-500">No active narratives found.</div>;

  // Group by narrative type
  const grouped = narratives.reduce((acc, curr: any) => {
    if (!acc[curr.narrative_type]) {
      acc[curr.narrative_type] = { count: 0, items: [] };
    }
    acc[curr.narrative_type].count += 1;
    acc[curr.narrative_type].items.push(curr);
    return acc;
  }, {} as Record<string, { count: number, items: any[] }>);

  const sortedTypes = Object.keys(grouped).sort((a, b) => grouped[b].count - grouped[a].count);

  return (
    <div className="bg-white dark:bg-gray-800 shadow rounded-lg overflow-hidden flex flex-col h-full">
      <div className="px-4 py-5 sm:px-6 border-b border-gray-200 dark:border-gray-700">
        <h3 className="text-lg leading-6 font-medium text-gray-900 dark:text-white">Active Narratives</h3>
      </div>
      <div className="overflow-y-auto flex-1 p-4">
        <div className="space-y-4">
          {sortedTypes.map(type => (
            <div key={type} className="flex flex-col">
              <div className="flex justify-between items-center mb-1">
                <span className="text-sm font-bold text-gray-800 dark:text-gray-200 uppercase tracking-wide">
                  {type.replace(/_/g, ' ')}
                </span>
                <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300">
                  {grouped[type].count} events
                </span>
              </div>
              <div className="pl-2 border-l-2 border-purple-200 dark:border-purple-800 space-y-2 mt-2">
                {grouped[type].items.slice(0, 3).map((item: any) => (
                  <div key={item.id} className="text-xs text-gray-600 dark:text-gray-400">
                    <span className="font-medium text-gray-900 dark:text-gray-300">{item.canonical_title}</span>
                    <div className="text-[10px] text-gray-500 mt-0.5">
                      Conf: {(item.narrative_confidence * 100).toFixed(0)}% • Sev: {item.severity_level}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};
