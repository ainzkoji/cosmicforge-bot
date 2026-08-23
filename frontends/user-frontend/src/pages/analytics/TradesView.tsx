import React from 'react';
import { useSearchParams } from 'react-router-dom';
import { CheckCircle2, Clock, List } from 'lucide-react';
import { CompletedTrades } from '@/pages/analytics/CompletedTrades';
import { OpenPositions } from '@/pages/analytics/OpenPositions';
import { TradesTable } from '@/pages/analytics/TradesTable';

const VALID_SUBS = ['completed', 'open', 'fills'] as const;
type TradesSub = typeof VALID_SUBS[number];

const TABS: { id: TradesSub; label: string; icon: React.ElementType }[] = [
    { id: 'completed', label: 'Completed Trades', icon: CheckCircle2 },
    { id: 'open',      label: 'Open Positions',   icon: Clock },
    { id: 'fills',     label: 'Raw Fills',         icon: List },
];

export function TradesView({ timeframe }: { timeframe: string }) {
    const [searchParams, setSearchParams] = useSearchParams();
    const subParam = searchParams.get('sub') as TradesSub | null;
    // Derive active tab from URL so deep links like ?sub=completed always work
    const activeSub: TradesSub = (subParam && VALID_SUBS.includes(subParam)) ? subParam : 'completed';

    const handleTabChange = (sub: TradesSub) => {
        setSearchParams(prev => {
            const next = new URLSearchParams(prev);
            next.set('sub', sub);
            return next;
        });
    };

    return (
        <div className="space-y-6">
            <div className="flex gap-1 bg-muted/50 p-1 rounded-xl w-fit border border-border">
                {TABS.map((tab) => {
                    const Icon = tab.icon;
                    return (
                        <button
                            key={tab.id}
                            onClick={() => handleTabChange(tab.id)}
                            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                                activeSub === tab.id
                                    ? 'bg-background shadow text-foreground'
                                    : 'text-muted-foreground hover:text-foreground'
                            }`}
                        >
                            <Icon className="w-4 h-4" />
                            {tab.label}
                        </button>
                    );
                })}
            </div>

            {activeSub === 'fills' && (
                <div className="flex items-start gap-2 bg-muted/20 border border-border rounded-lg px-4 py-3 text-sm text-muted-foreground">
                    <List className="w-4 h-4 mt-0.5 flex-shrink-0" />
                    <span>
                        <span className="font-medium text-foreground">Raw fills</span> are broker execution records showing individual OPEN and CLOSE events.{' '}
                        Use{' '}
                        <button
                            onClick={() => handleTabChange('completed')}
                            className="text-primary underline underline-offset-2 hover:no-underline"
                        >
                            Completed Trades
                        </button>{' '}
                        for position-level profit/loss.
                    </span>
                </div>
            )}

            {activeSub === 'completed' && <CompletedTrades timeframe={timeframe} />}
            {activeSub === 'open'      && <OpenPositions timeframe={timeframe} />}
            {activeSub === 'fills'     && <TradesTable timeframe={timeframe} />}
        </div>
    );
}
