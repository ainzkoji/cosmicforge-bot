import { useState } from 'react';
import {
    Users, Star, Search, Copy,
    Shield, Award, Filter, AlertTriangle
} from 'lucide-react';
import { motion } from 'framer-motion';

// Mock Data
const traders = [
    { id: 1, name: 'Alex Thompson', handle: '@alex_t', profit: 145.2, winRate: 72, copiers: 1240, risk: 'Low', avatar: 'AT' },
    { id: 2, name: 'Sarah Chen', handle: '@sarah_c', profit: 89.5, winRate: 68, copiers: 850, risk: 'Medium', avatar: 'SC' },
    { id: 3, name: 'Michael Bond', handle: '@mbond_007', profit: 210.8, winRate: 55, copiers: 420, risk: 'High', avatar: 'MB' },
    { id: 4, name: 'Emma Wilson', handle: '@emma_w', profit: 45.2, winRate: 82, copiers: 2100, risk: 'Low', avatar: 'EW' },
];

export default function SocialTrading() {
    const [filter, setFilter] = useState('all');

    return (
        <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            className="max-w-[1600px] mx-auto space-y-8"
        >
            {/* Risk Disclaimer */}
            <div className="bg-amber-500/10 border border-amber-500/20 rounded-xl p-4 flex gap-4 items-start">
                <AlertTriangle className="w-5 h-5 text-amber-500 shrink-0 mt-0.5" />
                <div className="text-sm text-amber-200/80">
                    <p className="font-bold text-amber-500 mb-1">Risk Warning</p>
                    <p>Copy trading involves significant risk and may result in the loss of your invested capital. Past performance is not indicative of future results. Please ensure you understand the risks involved before trading.</p>
                </div>
            </div>

            {/* Header */}
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Social Trading</h1>
                    <p className="text-muted-foreground">Follow and copy the success of top performing traders.</p>
                </div>
                <div className="flex gap-2">
                    <button className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg shadow hover:shadow-lg transition-all">
                        <Users className="w-4 h-4" />
                        <span>My Portfolio</span>
                    </button>
                </div>
            </div>

            {/* Featured / Stats Row */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="bg-gradient-to-br from-primary/10 to-primary/5 border border-primary/20 rounded-xl p-6 relative overflow-hidden">
                    <div className="relative z-10">
                        <h3 className="text-lg font-bold mb-1">Top Performer</h3>
                        <p className="text-sm text-muted-foreground mb-4">Highest ROI this month</p>
                        <div className="flex items-center gap-3">
                            <div className="w-10 h-10 rounded-full bg-primary/20 flex items-center justify-center font-bold text-primary">
                                MB
                            </div>
                            <div>
                                <div className="font-bold">Michael Bond</div>
                                <div className="text-green-500 font-mono font-bold">+210.8%</div>
                            </div>
                        </div>
                    </div>
                    <Award className="absolute right-[-10px] bottom-[-10px] w-24 h-24 text-primary/10 -rotate-12" />
                </div>

                <div className="bg-card border border-border rounded-xl p-6">
                    <h3 className="text-lg font-bold mb-1">Most Copied</h3>
                    <p className="text-sm text-muted-foreground mb-4">Community favorite</p>
                    <div className="flex items-center gap-3">
                        <div className="w-10 h-10 rounded-full bg-purple-500/20 flex items-center justify-center font-bold text-purple-500">
                            EW
                        </div>
                        <div>
                            <div className="font-bold">Emma Wilson</div>
                            <div className="text-muted-foreground text-sm">2,100 Copiers</div>
                        </div>
                    </div>
                </div>

                <div className="bg-card border border-border rounded-xl p-6">
                    <h3 className="text-lg font-bold mb-1">Risk Score</h3>
                    <p className="text-sm text-muted-foreground mb-4">Safest consistent returns</p>
                    <div className="flex items-center gap-3">
                        <div className="w-10 h-10 rounded-full bg-green-500/20 flex items-center justify-center font-bold text-green-500">
                            AT
                        </div>
                        <div>
                            <div className="font-bold">Alex Thompson</div>
                            <div className="text-green-500 text-sm flex items-center gap-1">
                                <Shield className="w-3 h-3" /> Low Risk
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Search & Filters */}
            <div className="flex flex-col md:flex-row gap-4 items-center justify-between">
                <div className="relative w-full md:w-96">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                    <input
                        type="text"
                        placeholder="Search traders..."
                        className="w-full pl-10 pr-4 py-2 bg-background border border-border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
                    />
                </div>
                <div className="flex gap-2">
                    {['all', 'profit', 'copiers'].map(f => (
                        <button
                            key={f}
                            onClick={() => setFilter(f)}
                            className={`px-3 py-2 rounded-lg text-sm font-medium border capitalize ${filter === f ? 'bg-primary text-primary-foreground border-primary' : 'bg-background border-border hover:bg-muted'
                                }`}
                        >
                            {f === 'all' ? 'All Traders' : `Top ${f}`}
                        </button>
                    ))}
                </div>
                <button className="flex items-center gap-2 px-3 py-2 border border-border rounded-lg hover:bg-muted text-sm font-medium">
                    <Filter className="w-4 h-4" /> Filters
                </button>
            </div>

            {/* Traders Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                {traders.map((trader) => {
                    const [isCopying, setIsCopying] = useState(false);

                    return (
                        <div key={trader.id} className="bg-card border border-border rounded-xl overflow-hidden hover:border-primary/50 transition-all group">
                            <div className="p-6">
                                <div className="flex items-center justify-between mb-4">
                                    <div className="flex items-center gap-3">
                                        <div className="w-12 h-12 rounded-full bg-muted flex items-center justify-center font-bold text-lg">
                                            {trader.avatar}
                                        </div>
                                        <div>
                                            <div className="font-bold">{trader.name}</div>
                                            <div className="text-xs text-muted-foreground">{trader.handle}</div>
                                        </div>
                                    </div>
                                    <div className={`px-2 py-1 rounded text-xs font-bold border ${trader.risk === 'Low' ? 'bg-green-500/10 text-green-500 border-green-500/20' :
                                        trader.risk === 'Medium' ? 'bg-amber-500/10 text-amber-500 border-amber-500/20' :
                                            'bg-red-500/10 text-red-500 border-red-500/20'
                                        }`}>
                                        {trader.risk} Risk
                                    </div>
                                </div>

                                <div className="grid grid-cols-2 gap-4 mb-6">
                                    <div>
                                        <div className="text-xs text-muted-foreground mb-1">Return (12M)</div>
                                        <div className="text-xl font-mono font-bold text-green-500">+{trader.profit}%</div>
                                    </div>
                                    <div>
                                        <div className="text-xs text-muted-foreground mb-1">Win Rate</div>
                                        <div className="text-xl font-mono font-bold">{trader.winRate}%</div>
                                    </div>
                                </div>

                                <div className="flex items-center justify-between text-sm text-muted-foreground mb-4">
                                    <div className="flex items-center gap-1">
                                        <Users className="w-4 h-4" />
                                        <span>{trader.copiers.toLocaleString()} copiers</span>
                                    </div>
                                    <div className="flex items-center gap-1">
                                        <Star className="w-4 h-4 text-amber-500 fill-amber-500" />
                                        <span>4.9</span>
                                    </div>
                                </div>

                                <button
                                    onClick={() => setIsCopying(!isCopying)}
                                    className={`w-full py-2 rounded-lg font-medium shadow transition-all flex items-center justify-center gap-2 ${isCopying
                                        ? 'bg-green-500/10 text-green-500 border border-green-500/20'
                                        : 'bg-primary text-primary-foreground hover:shadow-lg group-hover:scale-[1.02]'
                                        }`}
                                >
                                    {isCopying ? (
                                        <>
                                            <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
                                            Copying
                                        </>
                                    ) : (
                                        <>
                                            <Copy className="w-4 h-4" />
                                            Copy Trader
                                        </>
                                    )}
                                </button>
                            </div>
                            {/* Mini Chart Area (Fake) */}
                            <div className="h-16 bg-muted/20 border-t border-border relative">
                                <svg className="w-full h-full" viewBox="0 0 100 20" preserveAspectRatio="none">
                                    <path
                                        d={`M0,15 Q25,${20 - (trader.profit % 10)} 50,10 T100,5`}
                                        fill="none"
                                        stroke="currentColor"
                                        strokeWidth="2"
                                        className="text-primary/50"
                                    />
                                </svg>
                            </div>
                        </div>
                    );
                })}
            </div>
        </motion.div>
    );
}
