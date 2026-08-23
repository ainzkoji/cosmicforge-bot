import { useState } from 'react';
import {
    BookOpen, PlayCircle, FileText, BadgeCheck,
    Search, GraduationCap, ChevronRight
} from 'lucide-react';
import { motion } from 'framer-motion';

// Mock Data
const categories = [
    { id: 'basics', name: 'Trading Basics', count: 12, color: 'text-blue-500', bg: 'bg-blue-500/10' },
    { id: 'strategies', name: 'Strategies', count: 8, color: 'text-purple-500', bg: 'bg-purple-500/10' },
    { id: 'technical', name: 'Technical Analysis', count: 15, color: 'text-amber-500', bg: 'bg-amber-500/10' },
    { id: 'psychology', name: 'Trading Psychology', count: 5, color: 'text-green-500', bg: 'bg-green-500/10' },
];

const features = [
    {
        id: 1, type: 'video', title: 'Introduction to Algorithmic Trading', duration: '15 min',
        category: 'Basics', author: 'Dr. Quant', thumbnail: 'bg-slate-800'
    },
    {
        id: 2, type: 'article', title: 'Understanding Moving Averages', duration: '5 min read',
        category: 'Technical', author: 'Market Wizard', thumbnail: 'bg-slate-700'
    },
    {
        id: 3, type: 'course', title: 'Mastering Risk Management', duration: '2 hours',
        category: 'Strategies', author: 'Risk Pro', thumbnail: 'bg-slate-900'
    },
];

export default function Academy() {
    const [searchQuery, setSearchQuery] = useState('');

    return (
        <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            className="max-w-[1600px] mx-auto space-y-8"
        >
            {/* Header */}
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">CosmicForge Academy</h1>
                    <p className="text-muted-foreground">Master the markets with our comprehensive learning resources.</p>
                </div>
                <div className="flex gap-2">
                    <button className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg shadow hover:shadow-lg transition-all">
                        <BadgeCheck className="w-4 h-4" />
                        <span>My Progress</span>
                    </button>
                </div>
            </div>

            {/* Search Banner */}
            <div className="relative bg-gradient-to-r from-slate-900 to-slate-800 rounded-2xl p-8 md:p-12 overflow-hidden border border-border">
                <div className="relative z-10 max-w-2xl">
                    <h2 className="text-2xl md:text-3xl font-bold mb-4">What do you want to learn today?</h2>
                    <div className="relative">
                        <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-muted-foreground w-5 h-5" />
                        <input
                            type="text"
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                            placeholder="Search tutorials, guides, and courses..."
                            className="w-full pl-12 pr-4 py-3 bg-background/90 backdrop-blur border border-border rounded-xl focus:outline-none focus:ring-2 focus:ring-primary/50 text-lg shadow-xl"
                        />
                    </div>
                </div>
                {/* Decorative Elements */}
                <GraduationCap className="absolute right-8 bottom-8 w-48 h-48 text-primary/5 -rotate-12" />
                <BookOpen className="absolute right-1/4 top-8 w-24 h-24 text-primary/5 rotate-12" />
            </div>

            {/* Categories */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {categories.map((cat) => (
                    <button key={cat.id} className="flex flex-col items-center justify-center p-6 bg-card border border-border rounded-xl hover:border-primary/50 hover:bg-muted/50 transition-all group">
                        <div className={`w-12 h-12 rounded-full ${cat.bg} flex items-center justify-center mb-3 group-hover:scale-110 transition-transform`}>
                            <BookOpen className={`w-6 h-6 ${cat.color}`} />
                        </div>
                        <h3 className="font-bold">{cat.name}</h3>
                        <p className="text-xs text-muted-foreground">{cat.count} resources</p>
                    </button>
                ))}
            </div>

            {/* Featured Content */}
            <div>
                <div className="flex items-center justify-between mb-4">
                    <h2 className="text-xl font-bold">Featured Content</h2>
                    <button className="text-sm text-primary hover:underline flex items-center gap-1">
                        View All <ChevronRight className="w-4 h-4" />
                    </button>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                    {features.map((item) => (
                        <div
                            key={item.id}
                            onClick={() => alert(`Opening ${item.type}: ${item.title}`)}
                            className="bg-card border border-border rounded-xl overflow-hidden hover:shadow-lg transition-all group cursor-pointer"
                        >
                            <div className={`h-40 ${item.thumbnail} relative`}>
                                <div className="absolute inset-0 flex items-center justify-center bg-black/20 group-hover:bg-black/10 transition-colors">
                                    {item.type === 'video' ? (
                                        <PlayCircle className="w-12 h-12 text-white opacity-80 group-hover:scale-110 transition-transform" />
                                    ) : (
                                        <FileText className="w-12 h-12 text-white opacity-40 group-hover:scale-110 transition-transform" />
                                    )}
                                </div>
                                <div className="absolute top-3 right-3 px-2 py-1 bg-black/60 backdrop-blur rounded text-xs font-bold text-white uppercase">
                                    {item.type}
                                </div>
                            </div>
                            <div className="p-5">
                                <div className="text-xs font-bold text-primary mb-2 uppercase tracking-wide">{item.category}</div>
                                <h3 className="font-bold text-lg mb-2 group-hover:text-primary transition-colors line-clamp-2">{item.title}</h3>
                                <div className="flex items-center justify-between text-sm text-muted-foreground mt-4">
                                    <span>{item.author}</span>
                                    <span>{item.duration}</span>
                                </div>
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            {/* Banner CTA */}
            <div className="bg-primary text-primary-foreground rounded-2xl p-8 flex flex-col md:flex-row items-center justify-between gap-6">
                <div>
                    <h3 className="text-xl font-bold mb-2">Ready to test your knowledge?</h3>
                    <p className="opacity-90">Take our certification exam and earn the "Pro Trader" badge.</p>
                </div>
                <button className="px-6 py-3 bg-background text-foreground rounded-xl font-bold hover:bg-background/90 transition-colors">
                    Start Certification
                </button>
            </div>
        </motion.div>
    );
}
