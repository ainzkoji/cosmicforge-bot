import React, { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
    Activity,
    AlertTriangle,
    Building2,
    CheckCircle2,
    ChevronDown,
    Loader2,
    Link2,
} from "lucide-react";
import {
    getAffiliateSettings,
    updateAffiliateSettings,
    type AffiliateSettings,
} from "@/api/admin";
import { AdminLayout } from "@/components/admin/layout/AdminLayout";

// ─── Helpers ──────────────────────────────────────────────────────────────────

const ATTRIBUTION_OPTIONS: { value: string; label: string; description: string }[] = [
    { value: "last_click",     label: "Last Click",      description: "Revenue attributed to the most recent referral link click" },
    { value: "first_click",    label: "First Click",     description: "Revenue attributed to the first referral link click" },
    { value: "fixed_partner",  label: "Fixed Partner",   description: "Revenue attributed to a fixed assigned broker partner" },
    { value: "manual_review",  label: "Manual Review",   description: "Attribution reviewed and assigned manually by an admin" },
];

const COOKIE_OPTIONS = [7, 14, 30, 60, 90];

const DEFAULT_FORM: AffiliateSettings = {
    referral_commission_rate: 15,
    cookie_duration_days: 30,
    minimum_payout_amount: 100,
    minimum_payout_currency: "USD",
    tiered_referrals_enabled: false,
    attribution_model: "last_click",
    is_enabled: false,
};

// ─── Shared styles ────────────────────────────────────────────────────────────

const labelStyle: React.CSSProperties = {
    fontSize: 11,
    fontWeight: 700,
    textTransform: "uppercase",
    letterSpacing: "0.06em",
    color: "var(--admin-text-muted)",
    marginBottom: 6,
    display: "block",
};

const helperStyle: React.CSSProperties = {
    fontSize: 11,
    color: "var(--admin-text-muted)",
    marginTop: 5,
    lineHeight: 1.4,
};

const inputStyle: React.CSSProperties = {
    width: "100%",
    background: "var(--admin-bg-secondary)",
    border: "1px solid var(--admin-border-color)",
    color: "var(--admin-text-primary)",
    borderRadius: 6,
    padding: "8px 12px",
    fontSize: 13,
    outline: "none",
    boxSizing: "border-box",
};

const selectWrapStyle: React.CSSProperties = {
    position: "relative",
};

const selectStyle: React.CSSProperties = {
    ...inputStyle,
    appearance: "none",
    WebkitAppearance: "none",
    paddingRight: 32,
    cursor: "pointer",
};

// ─── Toggle component ─────────────────────────────────────────────────────────

function Toggle({ checked, onChange, id }: { checked: boolean; onChange: (v: boolean) => void; id: string }) {
    return (
        <button
            id={id}
            role="switch"
            aria-checked={checked}
            onClick={() => onChange(!checked)}
            style={{
                width: 40, height: 22, borderRadius: 999, border: "none", cursor: "pointer",
                background: checked ? "var(--admin-green)" : "var(--admin-bg-hover)",
                position: "relative", transition: "background 0.2s", flexShrink: 0,
                outline: "none",
            }}
        >
            <span style={{
                position: "absolute", top: 3, left: checked ? 21 : 3,
                width: 16, height: 16, borderRadius: "50%", background: "#fff",
                transition: "left 0.2s", boxShadow: "0 1px 3px rgba(0,0,0,0.3)",
            }} />
        </button>
    );
}

// ─── Section header ───────────────────────────────────────────────────────────

function SectionHeader({ title, subtitle, badge }: { title: string; subtitle?: string; badge?: string }) {
    return (
        <div style={{ marginBottom: 20 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <h2 style={{ margin: 0, fontSize: "0.95rem", fontWeight: 700, color: "var(--admin-text-primary)" }}>
                    {title}
                </h2>
                {badge && (
                    <span style={{
                        fontSize: 9, fontWeight: 700, padding: "2px 7px", borderRadius: 999,
                        background: "rgba(245,158,11,0.12)", color: "var(--admin-yellow)",
                        border: "1px solid rgba(245,158,11,0.25)",
                        textTransform: "uppercase", letterSpacing: "0.06em",
                    }}>
                        {badge}
                    </span>
                )}
            </div>
            {subtitle && (
                <p style={{ margin: "3px 0 0", fontSize: 11, color: "var(--admin-text-muted)" }}>{subtitle}</p>
            )}
        </div>
    );
}

// ─── Field row ────────────────────────────────────────────────────────────────

function FieldRow({ label, helper, children }: { label: string; helper?: string; children: React.ReactNode }) {
    return (
        <div>
            <label style={labelStyle}>{label}</label>
            {children}
            {helper && <p style={helperStyle}>{helper}</p>}
        </div>
    );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function AffiliateRevenue() {
    const queryClient = useQueryClient();
    const [form, setForm] = useState<AffiliateSettings>(DEFAULT_FORM);
    const [saveStatus, setSaveStatus] = useState<"idle" | "saved" | "error">("idle");

    const { data, isLoading, isError } = useQuery({
        queryKey: ["affiliateSettings"],
        queryFn: getAffiliateSettings,
    });

    useEffect(() => {
        if (data) setForm(data);
    }, [data]);

    const mutation = useMutation({
        mutationFn: updateAffiliateSettings,
        onSuccess: (updated) => {
            queryClient.setQueryData(["affiliateSettings"], updated);
            setForm(updated);
            setSaveStatus("saved");
            setTimeout(() => setSaveStatus("idle"), 3500);
        },
        onError: () => {
            setSaveStatus("error");
            setTimeout(() => setSaveStatus("idle"), 4000);
        },
    });

    function set<K extends keyof AffiliateSettings>(key: K, value: AffiliateSettings[K]) {
        setForm((prev) => ({ ...prev, [key]: value }));
    }

    function handleSave() {
        setSaveStatus("idle");
        const { id: _id, updated_at: _ua, ...payload } = form as any;
        mutation.mutate(payload);
    }

    const isSaving = mutation.isPending;
    const hasUnsaved = data ? JSON.stringify({ ...data, id: undefined, updated_at: undefined }) !== JSON.stringify({ ...form, id: undefined, updated_at: undefined }) : false;

    return (
        <AdminLayout>
            <div style={{ display: "flex", flexDirection: "column", gap: 20, maxWidth: 1200, margin: "0 auto" }}>

                {/* ── Page Header ── */}
                <div style={{ display: "flex", flexWrap: "wrap", alignItems: "flex-start", justifyContent: "space-between", gap: 10 }}>
                    <div>
                        <div style={{ display: "flex", alignItems: "center", flexWrap: "wrap", gap: 8, marginBottom: 5 }}>
                            <h1 style={{ margin: 0, fontSize: "1.55rem", fontWeight: 800, letterSpacing: "-0.025em", color: "var(--admin-text-primary)", lineHeight: 1.2 }}>
                                Affiliate & Broker Revenue
                            </h1>
                            {/* Live status badge */}
                            {!isLoading && data && (
                                <span style={{
                                    fontSize: 9, fontWeight: 700, padding: "2px 8px", borderRadius: 999,
                                    background: form.is_enabled ? "rgba(16,185,129,0.10)" : "rgba(255,255,255,0.04)",
                                    color: form.is_enabled ? "var(--admin-green)" : "var(--admin-text-muted)",
                                    border: form.is_enabled ? "1px solid rgba(16,185,129,0.28)" : "1px solid var(--admin-border-color)",
                                    textTransform: "uppercase", letterSpacing: "0.07em",
                                }}>
                                    {form.is_enabled ? "Tracking Enabled" : "Tracking Disabled"}
                                </span>
                            )}
                        </div>
                        <p style={{ margin: 0, fontSize: 13, color: "var(--admin-text-secondary)", lineHeight: 1.45 }}>
                            Configure broker referral settings, affiliate attribution, payout rules, and partner revenue controls.
                        </p>
                        {data?.updated_at && (
                            <p style={{ margin: "4px 0 0", fontSize: 11, color: "var(--admin-text-muted)" }}>
                                Last saved: {new Date(data.updated_at).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short" })}
                            </p>
                        )}
                    </div>

                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        {/* Save status feedback */}
                        {saveStatus === "saved" && (
                            <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 12, color: "var(--admin-green)" }}>
                                <CheckCircle2 className="w-4 h-4" />
                                Saved
                            </div>
                        )}
                        {saveStatus === "error" && (
                            <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 12, color: "var(--admin-red)" }}>
                                <AlertTriangle className="w-4 h-4" />
                                Failed to save
                            </div>
                        )}
                        <button
                            className="admin-btn admin-btn-primary"
                            onClick={handleSave}
                            disabled={isSaving || isLoading}
                            style={{ display: "flex", alignItems: "center", gap: 6, opacity: (!hasUnsaved && saveStatus === "idle") ? 0.6 : 1 }}
                        >
                            {isSaving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Activity className="w-4 h-4" />}
                            {isSaving ? "Saving…" : "Save Changes"}
                        </button>
                    </div>
                </div>

                {/* ── Loading ── */}
                {isLoading ? (
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", padding: "64px 0" }}>
                        <Loader2 className="w-8 h-8 animate-spin" style={{ color: "var(--admin-blue)" }} />
                    </div>
                ) : isError ? (
                    <div className="admin-card" style={{ display: "flex", alignItems: "center", gap: 10 }}>
                        <AlertTriangle className="w-5 h-5 flex-shrink-0" style={{ color: "var(--admin-red)" }} />
                        <div>
                            <div style={{ fontSize: 13, fontWeight: 700, color: "var(--admin-red)" }}>Unable to load affiliate settings</div>
                            <div style={{ fontSize: 11, color: "var(--admin-text-muted)", marginTop: 2 }}>Check backend connectivity and refresh the page.</div>
                        </div>
                    </div>
                ) : (
                    <>
                        {/* ── Main Settings Card ── */}
                        <div className="admin-card" style={{ padding: "22px 24px" }}>
                            <SectionHeader
                                title="Affiliate Program Settings"
                                subtitle="Control how broker referral and affiliate revenue is tracked and attributed."
                            />

                            <div style={{ display: "flex", flexDirection: "column", gap: 22 }}>

                                {/* Enable toggle — full-width row */}
                                <div style={{
                                    display: "flex", alignItems: "center", justifyContent: "space-between", gap: 16,
                                    padding: "14px 16px", borderRadius: 8,
                                    background: form.is_enabled ? "rgba(16,185,129,0.06)" : "var(--admin-bg-primary)",
                                    border: form.is_enabled ? "1px solid rgba(16,185,129,0.20)" : "1px solid var(--admin-border-color)",
                                    transition: "all 0.2s",
                                }}>
                                    <div>
                                        <div style={{ fontSize: 13, fontWeight: 700, color: "var(--admin-text-primary)" }}>
                                            Enable Affiliate & Broker Revenue Tracking
                                        </div>
                                        <div style={{ fontSize: 11, color: "var(--admin-text-muted)", marginTop: 3 }}>
                                            When enabled, referral attribution and broker partner revenue will be tracked.
                                        </div>
                                    </div>
                                    <Toggle
                                        id="is_enabled"
                                        checked={form.is_enabled}
                                        onChange={(v) => set("is_enabled", v)}
                                    />
                                </div>

                                {/* 2-column grid */}
                                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 20 }}>

                                    <FieldRow
                                        label="Referral Commission Rate (%)"
                                        helper="Percentage of qualifying revenue paid to the referring partner (0–100)."
                                    >
                                        <input
                                            type="number"
                                            min={0}
                                            max={100}
                                            step={0.5}
                                            value={form.referral_commission_rate}
                                            onChange={(e) => set("referral_commission_rate", parseFloat(e.target.value) || 0)}
                                            style={inputStyle}
                                        />
                                    </FieldRow>

                                    <FieldRow
                                        label="Cookie Duration"
                                        helper="How long a referral attribution cookie stays active after a click."
                                    >
                                        <div style={selectWrapStyle}>
                                            <select
                                                value={form.cookie_duration_days}
                                                onChange={(e) => set("cookie_duration_days", parseInt(e.target.value))}
                                                style={selectStyle}
                                            >
                                                {COOKIE_OPTIONS.map((d) => (
                                                    <option key={d} value={d}>{d} days</option>
                                                ))}
                                            </select>
                                            <ChevronDown style={{ position: "absolute", right: 10, top: "50%", transform: "translateY(-50%)", width: 13, height: 13, color: "var(--admin-text-muted)", pointerEvents: "none" }} />
                                        </div>
                                    </FieldRow>

                                    <FieldRow
                                        label="Minimum Payout Amount"
                                        helper="Minimum accumulated earnings required before a payout is processed."
                                    >
                                        <div style={{ display: "flex", gap: 8 }}>
                                            <input
                                                type="number"
                                                min={0}
                                                step={10}
                                                value={form.minimum_payout_amount}
                                                onChange={(e) => set("minimum_payout_amount", parseFloat(e.target.value) || 0)}
                                                style={{ ...inputStyle, flex: 1 }}
                                            />
                                            <div style={{
                                                ...inputStyle, width: 60, textAlign: "center",
                                                color: "var(--admin-text-muted)", background: "var(--admin-bg-primary)",
                                                cursor: "default", flexShrink: 0,
                                            }}>
                                                {form.minimum_payout_currency}
                                            </div>
                                        </div>
                                    </FieldRow>

                                    <FieldRow
                                        label="Attribution Model"
                                        helper={ATTRIBUTION_OPTIONS.find((o) => o.value === form.attribution_model)?.description ?? ""}
                                    >
                                        <div style={selectWrapStyle}>
                                            <select
                                                value={form.attribution_model}
                                                onChange={(e) => set("attribution_model", e.target.value)}
                                                style={selectStyle}
                                            >
                                                {ATTRIBUTION_OPTIONS.map((opt) => (
                                                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                                                ))}
                                            </select>
                                            <ChevronDown style={{ position: "absolute", right: 10, top: "50%", transform: "translateY(-50%)", width: 13, height: 13, color: "var(--admin-text-muted)", pointerEvents: "none" }} />
                                        </div>
                                    </FieldRow>
                                </div>

                                {/* Tiered referrals toggle */}
                                <div style={{
                                    display: "flex", alignItems: "center", justifyContent: "space-between", gap: 16,
                                    padding: "14px 16px", borderRadius: 8,
                                    background: "var(--admin-bg-primary)",
                                    border: "1px solid var(--admin-border-color)",
                                }}>
                                    <div>
                                        <div style={{ fontSize: 13, fontWeight: 700, color: "var(--admin-text-primary)" }}>
                                            Enable Tiered Referrals
                                        </div>
                                        <div style={{ fontSize: 11, color: "var(--admin-text-muted)", marginTop: 3 }}>
                                            Allow multi-level referral chains with separate commission rates per tier.
                                        </div>
                                    </div>
                                    <Toggle
                                        id="tiered_referrals"
                                        checked={form.tiered_referrals_enabled}
                                        onChange={(v) => set("tiered_referrals_enabled", v)}
                                    />
                                </div>
                            </div>
                        </div>

                        {/* ── Broker Partner Rules ── */}
                        <div className="admin-card" style={{ padding: "22px 24px" }}>
                            <SectionHeader
                                title="Broker Partner Rules"
                                subtitle="Connected broker partner revenue rules and referral tracking."
                                badge="Coming Soon"
                            />

                            <div style={{
                                display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
                                padding: "36px 20px", borderRadius: 8,
                                background: "var(--admin-bg-primary)", border: "1px solid var(--admin-border-color)",
                                textAlign: "center", gap: 12,
                            }}>
                                <div style={{
                                    width: 52, height: 52, borderRadius: "50%",
                                    background: "rgba(255,255,255,0.03)", border: "1px solid var(--admin-border-color)",
                                    display: "flex", alignItems: "center", justifyContent: "center",
                                }}>
                                    <Building2 className="w-5 h-5" style={{ color: "var(--admin-text-muted)" }} />
                                </div>
                                <div>
                                    <div style={{ fontSize: 13, fontWeight: 700, color: "var(--admin-text-secondary)", marginBottom: 4 }}>
                                        No Broker Partners Configured
                                    </div>
                                    <div style={{ fontSize: 12, color: "var(--admin-text-muted)", maxWidth: 380, lineHeight: 1.5 }}>
                                        Connected broker partner rules will appear here once broker integrations and
                                        referral tracking are activated. Partner attribution, revenue splits, and
                                        custom payout rules are configured per partner.
                                    </div>
                                </div>
                            </div>
                        </div>

                        {/* ── Unsaved changes reminder ── */}
                        {hasUnsaved && saveStatus === "idle" && (
                            <div style={{
                                display: "flex", alignItems: "center", gap: 10,
                                padding: "10px 16px", borderRadius: 8,
                                background: "rgba(245,158,11,0.08)", border: "1px solid rgba(245,158,11,0.25)",
                            }}>
                                <AlertTriangle className="w-4 h-4 flex-shrink-0" style={{ color: "var(--admin-yellow)" }} />
                                <span style={{ fontSize: 12, color: "var(--admin-yellow)" }}>
                                    You have unsaved changes. Click <strong>Save Changes</strong> to persist them.
                                </span>
                            </div>
                        )}
                    </>
                )}
            </div>
        </AdminLayout>
    );
}
