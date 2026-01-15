import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import { Search, Rocket, Lock, Info, TrendingUp } from 'lucide-react';
import { Link } from 'react-router-dom';

const StrategyCard = ({ strategy }: { strategy: any }) => (
    <Link to={`/dashboard/strategies/${strategy.id}`} className="block group">
        <div className="bg-card border border-border rounded-xl overflow-hidden hover:border-primary/50 transition-all hover:shadow-lg">
            {/* Header / Banner */}
            <div className="h-24 bg-gradient-to-r from-primary/10 to-purple-500/10 p-4 relative">
                <div className="flex justify-between items-start">
                    <span className={`text-[10px] font-bold uppercase tracking-wider px-2 py-1 rounded bg-background/50 backdrop-blur-sm border border-border ${strategy.visibility === 'official' ? 'text-primary border-primary/20' : 'text-muted-foreground'
                        }`}>
                        {strategy.visibility}
                    </span>
                    {strategy.entitlement_tier !== 'free' && (
                        <span className="bg-yellow-500/10 text-yellow-500 border border-yellow-500/20 p-1.5 rounded-full">
                            <Lock className="w-3 h-3" />
                        </span>
                    )}
                </div>
            </div>

            {/* Content */}
            <div className="p-5">
                <div className="flex gap-2 mb-3">
                    {strategy.market_types?.map((m: string) => (
                        <span key={m} className="text-xs font-semibold px-2 py-0.5 rounded-full bg-muted text-muted-foreground">
                            {m}
                        </span>
                    ))}
                </div>

                <h3 className="text-lg font-bold mb-1 group-hover:text-primary transition-colors">{strategy.name}</h3>
                <p className="text-sm text-muted-foreground line-clamp-2 mb-4 h-10">
                    {strategy.description || "No description provided."}
                </p>

                {/* Metrics / Footer */}
                <div className="flex items-center justify-between text-xs text-muted-foreground pt-4 border-t border-border/50">
                    <div className="flex items-center gap-1">
                        <TrendingUp className="w-3 h-3" />
                        <span>v{strategy.latest_version || 1}</span>
                    </div>
                    <span>Updated {new Date(strategy.updated_at).toLocaleDateString()}</span>
                </div>
            </div>
        </div>
    </Link>
);

export default function StrategyGallery() {
    const [filter, setFilter] = useState('all'); // all, official, community, mine
    const [search, setSearch] = useState('');
    const [riskStyle, setRiskStyle] = useState<string>(''); // conservative, aggressive

    const { data: strategies = [], isLoading } = useQuery({
        queryKey: ['strategies', filter, search, riskStyle],
        queryFn: async () => {
            if (filter === 'mine') {
                return await api.getMyStrategies();
            } else {
                // Map frontend filter to API params
                const params: any = {};
                // The API doesn't support 'q' search for name yet (only tags/market), 
                // but we can pass 'style' or 'market_type' if we had those inputs.
                // We'll filter by name on client side for MVP until full search API.

                if (filter !== 'all') params.visibility = filter; // This might need backend tweak if we want strict visibility filter in public list
                if (riskStyle) params.risk_style = riskStyle;

                return await api.getMarketplaceStrategies(params);
            }
        }
    });

    // Client-side search filtering (temporary until API supports full text search)
    const filteredStrategies = strategies.filter((s: any) =>
        s.name.toLowerCase().includes(search.toLowerCase()) ||
        s.description?.toLowerCase().includes(search.toLowerCase())
    );

    return (
        <div className="container mx-auto max-w-7xl animate-in fade-in duration-500 py-8">
            {/* Header */}
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 mb-8">
                <div>
                    <h1 className="text-3xl font-bold flex items-center gap-2">
                        <Rocket className="w-8 h-8 text-primary" /> Strategy Marketplace
                    </h1>
                    <p className="text-muted-foreground">Discover, fork, or build automated trading strategies.</p>
                </div>
                <Link to="/dashboard/strategies/builder" className="px-4 py-2 bg-primary text-primary-foreground rounded-lg font-medium hover:bg-primary/90 transition-colors shadow-lg shadow-primary/20">
                    Build New Strategy
                </Link>
            </div>

            {/* Filters */}
            <div className="flex flex-col lg:flex-row gap-4 mb-8">
                <div className="relative flex-1">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                    <input
                        type="text"
                        placeholder="Search strategies by name..."
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                        className="w-full pl-9 pr-4 py-2 bg-card border border-border rounded-lg outline-none focus:ring-2 focus:ring-primary/20 transition-all"
                    />
                </div>

                <div className="flex gap-2 overflow-x-auto pb-2 lg:pb-0">
                    <div className="flex bg-muted p-1 rounded-lg shrink-0">
                        {['all', 'official', 'mine'].map((f) => (
                            <button
                                key={f}
                                onClick={() => setFilter(f)}
                                className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all capitalize ${filter === f ? 'bg-background shadow-sm text-foreground' : 'text-muted-foreground hover:text-foreground'
                                    }`}
                            >
                                {f}
                            </button>
                        ))}
                    </div>

                    <select
                        className="px-3 py-2 bg-card border border-border rounded-lg text-sm outline-none focus:ring-2 focus:ring-primary/20"
                        value={riskStyle}
                        onChange={(e) => setRiskStyle(e.target.value)}
                    >
                        <option value="">All Risks</option>
                        <option value="conservative">Conservative</option>
                        <option value="moderate">Moderate</option>
                        <option value="aggressive">Aggressive</option>
                    </select>
                </div>
            </div>

            {/* Grid */}
            {isLoading ? (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
                    {[1, 2, 3, 4, 5, 6].map(i => (
                        <div key={i} className="h-72 bg-card/50 animate-pulse rounded-xl border border-white/5" />
                    ))}
                </div>
            ) : (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
                    {filteredStrategies.map((strategy: any) => (
                        <StrategyCard key={strategy.id} strategy={strategy} />
                    ))}

                    {filteredStrategies.length === 0 && (
                        <div className="col-span-full py-20 text-center text-muted-foreground border-2 border-dashed border-white/5 rounded-xl">
                            <Info className="w-12 h-12 mx-auto mb-4 opacity-50" />
                            <p className="text-lg font-medium">No strategies found</p>
                            <p>Try adjusting your search or filters.</p>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
