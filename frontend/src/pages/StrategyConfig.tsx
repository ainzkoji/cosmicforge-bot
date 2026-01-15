import React, { useState, useEffect } from 'react';
import { api } from '../api/client';
import { ConfigWizard } from '../components/StrategyConfig/ConfigWizard';
import {
    ShieldCheck,
    Plus,
    Play,
    Pause,
    TriangleAlert,
    BarChart3
} from 'lucide-react';

const StrategyConfig = () => {
    const [accounts, setAccounts] = useState<any[]>([]);
    const [selectedAccount, setSelectedAccount] = useState<string | null>(null);
    const [configs, setConfigs] = useState<any[]>([]);
    const [activeConfig, setActiveConfig] = useState<any>(null);
    const [loading, setLoading] = useState(true);
    const [showWizard, setShowWizard] = useState(false);

    useEffect(() => {
        fetchAccounts();
    }, []);

    useEffect(() => {
        if (selectedAccount) {
            fetchConfigs(selectedAccount);
            fetchActiveConfig(selectedAccount);
        }
    }, [selectedAccount]);

    const fetchAccounts = async () => {
        try {
            const res = await api.getBrokerAccounts();
            setAccounts(res.accounts);
            if (res.accounts.length > 0) {
                setSelectedAccount(res.accounts[0].account_id);
            }
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    const fetchConfigs = async (accountId: string) => {
        try {
            const res = await api.getStrategyConfigs(accountId);
            setConfigs(res);
        } catch (err) {
            console.error(err);
        }
    };

    const fetchActiveConfig = async (accountId: string) => {
        try {
            const config = await api.getActiveConfig(accountId);
            setActiveConfig(config);
        } catch (err) {
            // 404 is expected if no config is active
            setActiveConfig(null);
        }
    };

    const handleActivate = async (configId: string) => {
        try {
            await api.activateStrategyConfig(configId);
            if (selectedAccount) {
                fetchConfigs(selectedAccount);
                fetchActiveConfig(selectedAccount);
            }
        } catch (err) {
            alert("Failed to activate config: " + err);
        }
    }

    return (
        <div className="p-6 bg-gray-900 min-h-screen text-white">
            <div className="flex justify-between items-center mb-8">
                <div>
                    <h1 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-purple-500">
                        Strategy Governance
                    </h1>
                    <p className="text-gray-400 mt-2">
                        Configure automated trading strategies with safety-first risk management.
                    </p>
                </div>
                <button
                    onClick={() => setShowWizard(true)}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg font-medium transition-colors"
                >
                    <Plus className="w-5 h-5" />
                    New Configuration
                </button>
            </div>

            {/* Account Selector */}
            <div className="mb-8">
                <label className="block text-sm font-medium text-gray-400 mb-2">Select Broker Account</label>
                <div className="flex gap-4">
                    {accounts.map(acc => (
                        <button
                            key={acc.account_id}
                            onClick={() => setSelectedAccount(acc.account_id)}
                            className={`px-4 py-3 rounded-xl border flex items-center gap-3 transition-all ${selectedAccount === acc.account_id
                                    ? 'bg-blue-900/30 border-blue-500 text-blue-100'
                                    : 'bg-gray-800 border-gray-700 text-gray-400 hover:border-gray-600'
                                }`}
                        >
                            <div className={`w-3 h-3 rounded-full ${acc.is_connected ? 'bg-green-500' : 'bg-red-500'}`} />
                            <span className="font-semibold">{acc.label || acc.account_id}</span>
                            <span className="text-xs px-2 py-0.5 bg-gray-700 rounded uppercase">{acc.broker_id}</span>
                        </button>
                    ))}
                </div>
            </div>

            {/* Active Configuration Banner */}
            {activeConfig && (
                <div className="mb-8 p-6 rounded-2xl bg-gradient-to-r from-emerald-900/30 to-teal-900/30 border border-emerald-500/30">
                    <div className="flex justify-between items-start">
                        <div>
                            <div className="flex items-center gap-2 mb-2">
                                <span className="flex items-center gap-1 px-2 py-0.5 text-xs font-bold bg-emerald-500 text-black rounded uppercase">
                                    Active
                                </span>
                                <h2 className="text-xl font-bold">{activeConfig.name}</h2>
                            </div>
                            <div className="flex gap-6 mt-4 text-sm text-gray-300">
                                <div>
                                    <span className="block text-gray-500 text-xs uppercase tracking-wider">Strategy</span>
                                    <span className="font-mono text-emerald-300">{activeConfig.strategy_id}</span>
                                </div>
                                <div>
                                    <span className="block text-gray-500 text-xs uppercase tracking-wider">Risk Profile</span>
                                    <span className="font-mono text-emerald-300">{activeConfig.risk_parameters.risk_profile}</span>
                                </div>
                                <div>
                                    <span className="block text-gray-500 text-xs uppercase tracking-wider">Daily Loss Limit</span>
                                    <span className="font-mono text-red-300">
                                        {(activeConfig.risk_parameters.daily_loss_limit_pct * 100).toFixed(1)}%
                                    </span>
                                </div>
                            </div>
                        </div>
                        <div className="text-right">
                            <button
                                onClick={() => handleActivate(activeConfig.id)}
                                className="px-4 py-2 bg-emerald-600/20 text-emerald-400 border border-emerald-500/50 rounded-lg hover:bg-emerald-600/30 flex items-center gap-2"
                            >
                                <ShieldCheck className="w-5 h-5" />
                                System Protected
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* Configuration List */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {configs.map(config => (
                    <div
                        key={config.id}
                        className={`p-6 rounded-2xl bg-gray-800/50 border ${activeConfig?.id === config.id ? 'border-emerald-500/50' : 'border-gray-700'
                            } hover:border-gray-600 transition-all`}
                    >
                        <div className="flex justify-between items-start mb-4">
                            <div>
                                <h3 className="text-lg font-bold flex items-center gap-2">
                                    {config.name}
                                    {activeConfig?.id === config.id && (
                                        <ShieldCheck className="w-5 h-5 text-emerald-500" />
                                    )}
                                </h3>
                                <p className="text-gray-400 text-sm mt-1">
                                    Last updated: {new Date(config.updated_at).toLocaleDateString()}
                                </p>
                            </div>
                            <div>
                                {activeConfig?.id !== config.id ? (
                                    <button
                                        onClick={() => handleActivate(config.id)}
                                        className="p-2 bg-gray-700 hover:bg-gray-600 rounded-lg text-gray-300 transition-colors"
                                        title="Activate this configuration"
                                    >
                                        <Play className="w-5 h-5" />
                                    </button>
                                ) : (
                                    <span className="px-3 py-1 bg-emerald-500/10 text-emerald-400 text-xs font-bold rounded-full border border-emerald-500/20">
                                        ACTIVE
                                    </span>
                                )}
                            </div>
                        </div>

                        <div className="grid grid-cols-2 gap-4 text-sm mb-6">
                            <div className="p-3 bg-gray-900/50 rounded-lg">
                                <span className="block text-gray-500 text-xs mb-1">Risk Profile</span>
                                <span className={`font-bold ${config.risk_parameters.risk_profile === 'aggressive' ? 'text-red-400' :
                                        config.risk_parameters.risk_profile === 'conservative' ? 'text-blue-400' :
                                            'text-yellow-400'
                                    }`}>
                                    {config.risk_parameters.risk_profile.toUpperCase()}
                                </span>
                            </div>
                            <div className="p-3 bg-gray-900/50 rounded-lg">
                                <span className="block text-gray-500 text-xs mb-1">Max Drawdown</span>
                                <span className="font-mono">
                                    {(config.risk_parameters.max_drawdown_pct * 100).toFixed(1)}%
                                </span>
                            </div>
                        </div>

                        <div className="flex justify-end gap-2">
                            <button className="px-3 py-1.5 text-sm text-gray-400 hover:text-white transition-colors">
                                Edit Config
                            </button>
                            <button className="px-3 py-1.5 text-sm text-gray-400 hover:text-white transition-colors">
                                View History
                            </button>
                        </div>
                    </div>
                ))}

                {configs.length === 0 && !loading && (
                    <div className="col-span-2 text-center py-12 text-gray-500">
                        <TriangleAlert className="w-12 h-12 mx-auto mb-3 opacity-20" />
                        <p>No strategy configurations found for this account.</p>
                        <button
                            onClick={() => setShowWizard(true)}
                            className="mt-4 text-blue-400 hover:text-blue-300 font-medium"
                        >
                            Create your first configuration
                        </button>
                    </div>
                )}
            </div>

            {/* Wizard Modal */}
            {showWizard && (
                <div className="fixed inset-0 bg-black/80 backdrop-blur-sm flex items-center justify-center z-50">
                    <div className="bg-gray-800 rounded-2xl w-full max-w-4xl max-h-[90vh] overflow-y-auto border border-gray-700 shadow-2xl">
                        <ConfigWizard
                            accountId={selectedAccount || ''}
                            onClose={() => setShowWizard(false)}
                            onComplete={() => {
                                setShowWizard(false);
                                if (selectedAccount) {
                                    fetchConfigs(selectedAccount);
                                    fetchActiveConfig(selectedAccount);
                                }
                            }}
                        />
                    </div>
                </div>
            )}
        </div>
    );
};

export default StrategyConfig;
