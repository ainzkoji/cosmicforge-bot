import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMutation } from '@tanstack/react-query';
import { BacktestAPI, BacktestConfig } from '@/api/backtest';
import {
    Button, Input, Label, Card, CardHeader, CardTitle, CardContent, CardFooter
} from '@/components/UI/SimpleUI';
import { ArrowLeft, Play, Loader2, Info } from 'lucide-react';

const TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"];
const DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"];

// System-managed modes mapping to internal strategies and params
const SIMULATION_MODES = [
    {
        id: "conservative",
        name: "Conservative",
        description: "Lower risk, longer trend confirmation. Prioritizes capital preservation.",
        internal_strategy: "sma_cross",
        params: { fast_period: 20, slow_period: 60 }
    },
    {
        id: "balanced",
        name: "Balanced",
        description: "Standard risk profile. Balances win rate and trade frequency.",
        internal_strategy: "sma_cross",
        params: { fast_period: 10, slow_period: 30 }
    },
    {
        id: "aggressive",
        name: "Aggressive",
        description: "Higher risk, faster signals. Captures shorter trends but may have more false signals.",
        internal_strategy: "sma_cross",
        params: { fast_period: 5, slow_period: 15 }
    },
];

export default function CreateBacktest() {
    const navigate = useNavigate();

    // Default dates
    const today = new Date();
    const lastMonth = new Date();
    lastMonth.setMonth(today.getMonth() - 1);

    const [name, setName] = useState(`Simulation ${today.toLocaleDateString()} ${today.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`);
    const [marketType, setMarketType] = useState<"crypto" | "forex">("crypto");
    const [selectedModeId, setSelectedModeId] = useState("balanced");
    const [symbols, setSymbols] = useState<string[]>(["BTCUSDT"]);
    const [timeframe, setTimeframe] = useState("1h");
    const [startDate, setStartDate] = useState(lastMonth.toISOString().split('T')[0]);
    const [endDate, setEndDate] = useState(today.toISOString().split('T')[0]);
    const [capital, setCapital] = useState(10000);
    const [slippage, setSlippage] = useState(10);
    const [fee, setFee] = useState(6); // 0.06%

    // Custom symbol input
    const [customSymbol, setCustomSymbol] = useState("");

    const createMutation = useMutation({
        mutationFn: (config: BacktestConfig) => BacktestAPI.create(config),
        onSuccess: (data) => {
            navigate(`/dashboard/backtests/${data.run_id}`);
        },
        onError: (error: any) => {
            console.error("Failed to create simulation", error);
            alert("Failed to create simulation: " + (error.message || "Unknown error"));
        }
    });

    const handleSymbolToggle = (symbol: string) => {
        if (symbols.includes(symbol)) {
            setSymbols(symbols.filter(s => s !== symbol));
        } else {
            setSymbols([...symbols, symbol]);
        }
    };

    const addCustomSymbol = () => {
        if (customSymbol && !symbols.includes(customSymbol.toUpperCase())) {
            setSymbols([...symbols, customSymbol.toUpperCase()]);
            setCustomSymbol("");
        }
    };

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();

        if (symbols.length === 0) {
            alert("Please select at least one symbol");
            return;
        }

        const mode = SIMULATION_MODES.find(m => m.id === selectedModeId);
        if (!mode) return;

        const config: BacktestConfig = {
            name,
            strategy_id: mode.internal_strategy,
            symbols,
            interval: timeframe,
            start_date: new Date(startDate).toISOString(),
            end_date: new Date(endDate).toISOString(),
            initial_capital: Number(capital),
            slippage_bps: Number(slippage),
            fee_bps: Number(fee),
            market_type: marketType,
            data_source: "binance", // TODO: Update based on market type (e.g. OANDA for forex)
            strategy_params: mode.params,
            risk_params: {}
        };

        createMutation.mutate(config);
    };

    return (
        <div className="container mx-auto px-4 py-8 max-w-4xl">
            <Button variant="ghost" className="mb-6 pl-0 hover:pl-0 hover:bg-transparent" onClick={() => navigate('/dashboard/backtests')}>
                <ArrowLeft className="mr-2 h-4 w-4" /> Back to History
            </Button>

            <h1 className="text-3xl font-bold tracking-tight mb-2">Run Simulation</h1>
            <p className="text-muted-foreground mb-8 text-sm">
                This simulation shows how the bot would have performed historically using your selected configuration and system-managed strategy.
            </p>

            <form onSubmit={handleSubmit}>
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                    {/* Main Config */}
                    <div className="lg:col-span-2 space-y-6">
                        <Card>
                            <CardHeader>
                                <CardTitle>Configuration</CardTitle>
                            </CardHeader>
                            <CardContent className="space-y-6">
                                {/* Name */}
                                <div className="space-y-2">
                                    <Label htmlFor="name">Simulation Name</Label>
                                    <Input id="name" value={name} onChange={e => setName(e.target.value)} required />
                                </div>

                                {/* Market Type (Future Proofing) */}
                                <div className="space-y-3">
                                    <Label>Market Type</Label>
                                    <div className="flex bg-muted p-1 rounded-lg w-fit">
                                        <button
                                            type="button"
                                            onClick={() => setMarketType('crypto')}
                                            className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${marketType === 'crypto'
                                                ? 'bg-white text-foreground shadow-sm'
                                                : 'text-muted-foreground hover:text-foreground'}`}
                                        >
                                            Crypto
                                        </button>
                                        <button
                                            type="button"
                                            disabled
                                            className="px-4 py-1.5 rounded-md text-sm font-medium text-muted-foreground/50 cursor-not-allowed flex items-center gap-2"
                                        >
                                            Forex <span className="text-[10px] bg-primary/10 text-primary px-1.5 py-0.5 rounded">Soon</span>
                                        </button>
                                        <button
                                            type="button"
                                            disabled
                                            className="px-4 py-1.5 rounded-md text-sm font-medium text-muted-foreground/50 cursor-not-allowed flex items-center gap-2"
                                        >
                                            Stocks <span className="text-[10px] bg-primary/10 text-primary px-1.5 py-0.5 rounded">Soon</span>
                                        </button>
                                    </div>
                                </div>

                                {/* Mode Selection */}
                                <div className="space-y-3">
                                    <Label>System Mode</Label>
                                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                                        {SIMULATION_MODES.map(mode => (
                                            <div
                                                key={mode.id}
                                                onClick={() => setSelectedModeId(mode.id)}
                                                className={`cursor-pointer rounded-lg border p-4 transition-all hover:bg-muted/50 ${selectedModeId === mode.id
                                                    ? "border-blue-500 bg-blue-500/5 ring-1 ring-blue-500"
                                                    : "border-input bg-transparent"
                                                    }`}
                                            >
                                                <div className="font-semibold mb-1">{mode.name}</div>
                                                <div className="text-xs text-muted-foreground leading-snug">
                                                    {mode.description}
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>

                                {/* Symbols */}
                                <div className="space-y-3">
                                    <Label>Assets to Simulate</Label>
                                    <div className="flex flex-wrap gap-3">
                                        {DEFAULT_SYMBOLS.map(sym => (
                                            <div key={sym} className="flex items-center space-x-2">
                                                <input
                                                    type="checkbox"
                                                    id={`sym-${sym}`}
                                                    checked={symbols.includes(sym)}
                                                    onChange={() => handleSymbolToggle(sym)}
                                                    className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                                                />
                                                <Label htmlFor={`sym-${sym}`} className="cursor-pointer font-normal">{sym}</Label>
                                            </div>
                                        ))}
                                        {symbols.filter(s => !DEFAULT_SYMBOLS.includes(s)).map(sym => (
                                            <div key={sym} className="flex items-center space-x-2 bg-muted px-2 py-1 rounded">
                                                <input
                                                    type="checkbox"
                                                    id={`sym-${sym}`}
                                                    checked={symbols.includes(sym)}
                                                    onChange={() => handleSymbolToggle(sym)}
                                                    className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                                                />
                                                <Label htmlFor={`sym-${sym}`} className="cursor-pointer font-normal">{sym}</Label>
                                            </div>
                                        ))}
                                    </div>
                                    <div className="flex gap-2 max-w-xs mt-2">
                                        <Input
                                            placeholder="Add custom (e.g. DOGEUSDT)"
                                            value={customSymbol}
                                            onChange={e => setCustomSymbol(e.target.value)}
                                            className="h-8 text-sm"
                                        />
                                        <Button type="button" size="sm" variant="secondary" onClick={addCustomSymbol}>Add</Button>
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    </div>

                    {/* Side Settings */}
                    <div className="space-y-6">
                        <Card>
                            <CardHeader>
                                <CardTitle className="text-base">Parameters</CardTitle>
                            </CardHeader>
                            <CardContent className="space-y-4">
                                <div className="space-y-2">
                                    <Label htmlFor="startDate">Start Date</Label>
                                    <Input type="date" id="startDate" value={startDate} onChange={e => setStartDate(e.target.value)} required />
                                </div>
                                <div className="space-y-2">
                                    <Label htmlFor="endDate">End Date</Label>
                                    <Input type="date" id="endDate" value={endDate} onChange={e => setEndDate(e.target.value)} required />
                                </div>
                                <div className="space-y-2">
                                    <Label htmlFor="timeframe">Timeframe</Label>
                                    <select
                                        id="timeframe"
                                        className="flex h-10 w-full rounded-md border border-gray-300 bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                                        value={timeframe}
                                        onChange={(e) => setTimeframe(e.target.value)}
                                    >
                                        {TIMEFRAMES.map(tf => (
                                            <option key={tf} value={tf}>{tf}</option>
                                        ))}
                                    </select>
                                </div>
                                <div className="pt-4 border-t">
                                    <div className="space-y-2">
                                        <Label htmlFor="capital">Initial Capital (USDT)</Label>
                                        <Input type="number" id="capital" value={capital} onChange={e => setCapital(Number(e.target.value))} min={100} />
                                    </div>
                                </div>
                                <div className="grid grid-cols-2 gap-4">
                                    <div className="space-y-2">
                                        <Label htmlFor="fee">Fee (bps)</Label>
                                        <Input type="number" id="fee" value={fee} onChange={e => setFee(Number(e.target.value))} min={0} />
                                    </div>
                                    <div className="space-y-2">
                                        <Label htmlFor="slippage">Slip (bps)</Label>
                                        <Input type="number" id="slippage" value={slippage} onChange={e => setSlippage(Number(e.target.value))} min={0} />
                                    </div>
                                </div>
                            </CardContent>
                            <CardFooter>
                                <Button type="submit" className="w-full" disabled={createMutation.isPending}>
                                    {createMutation.isPending ? (
                                        <>
                                            <Loader2 className="mr-2 h-4 w-4 animate-spin" /> Starting...
                                        </>
                                    ) : (
                                        <>
                                            <Play className="mr-2 h-4 w-4" /> Run Simulation
                                        </>
                                    )}
                                </Button>
                            </CardFooter>
                        </Card>

                        <div className="bg-blue-500/10 border border-blue-500/20 rounded-lg p-4 text-xs text-blue-200 flex gap-2">
                            <Info className="h-4 w-4 flex-shrink-0 mt-0.5" />
                            <div>
                                <p className="font-semibold mb-1">System Managed Strategy</p>
                                <p>You select the mode (Conservative/Balanced/Aggressive), and the system automatically optimizes indicators and risk parameters for that profile. No manual logic editing required.</p>
                            </div>
                        </div>
                    </div>
                </div>
            </form>
        </div>
    );
}
