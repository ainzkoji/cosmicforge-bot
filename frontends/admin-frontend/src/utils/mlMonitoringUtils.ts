/**
 * Pure utility functions for ML Monitoring views.
 *
 * These functions have NO React / DOM / browser dependencies so they can be
 * imported and tested in a Node.js environment without a DOM shim.
 *
 * FIX-F adds: buildSafeStateStatus, buildDocument1ChecklistSummary.
 */
import type { MLPhaseIChecklistItem, MLPhaseIReadiness } from "@/api/admin";

// ── Formatters ────────────────────────────────────────────────────────────────

export function formatPercent(value: number | null | undefined, digits: number = 1): string {
    if (value === null || value === undefined || Number.isNaN(value)) return "--";
    return `${value.toFixed(digits)}%`;
}

export function formatNumber(value: number | null | undefined, digits: number = 0): string {
    if (value === null || value === undefined || Number.isNaN(value)) return "--";
    return new Intl.NumberFormat("en-US", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
    }).format(value);
}

export function formatDecimal(value: number | null | undefined, digits: number = 3): string {
    if (value === null || value === undefined || Number.isNaN(value)) return "--";
    return value.toFixed(digits);
}

export function formatDate(value: string | null | undefined): string {
    if (!value) return "--";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return value;
    return parsed.toLocaleString();
}

export function formatCompactPath(value: string | null | undefined): string {
    if (!value) return "--";
    if (value.length <= 64) return value;
    return `${value.slice(0, 30)}...${value.slice(-26)}`;
}

export function formatModeLabel(mode: string): string {
    if (mode === "shadow") return "Shadow Only";
    return mode.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

// ── Readiness issues builder (used by MLMonitoring readiness tab) ─────────────

export function buildReadinessIssues({
    totalLinkedCompletedTrades,
    requiredTrades,
    wins,
    requiredWins,
    featureCoveragePct,
    linkageHealthy,
    labelDistributionSingleClass,
    brokenFeatureCount,
    tradesMissingCriticalFeatures,
}: {
    totalLinkedCompletedTrades: number;
    requiredTrades: number;
    wins: number;
    requiredWins: number;
    featureCoveragePct: number;
    linkageHealthy: boolean;
    labelDistributionSingleClass: boolean;
    brokenFeatureCount: number;
    tradesMissingCriticalFeatures: number;
}): string[] {
    const issues: string[] = [];
    if (totalLinkedCompletedTrades < requiredTrades) {
        issues.push(`Linked completed trades are below the gate (${formatNumber(totalLinkedCompletedTrades)} / ${formatNumber(requiredTrades)}).`);
    }
    if (wins < requiredWins) {
        issues.push(`Wins are below the gate (${formatNumber(wins)} / ${formatNumber(requiredWins)}).`);
    }
    if (featureCoveragePct < 90) {
        issues.push(`Feature coverage is below the 90% readiness floor (${formatPercent(featureCoveragePct)}).`);
    }
    if (!linkageHealthy) {
        issues.push("Linkage health is failing the readiness gate.");
    }
    if (brokenFeatureCount > 0) {
        issues.push(`${formatNumber(brokenFeatureCount)} tracked features are currently broken.`);
    }
    if (tradesMissingCriticalFeatures > 0) {
        issues.push(`${formatNumber(tradesMissingCriticalFeatures)} linked trades are still missing critical features.`);
    }
    if (labelDistributionSingleClass) {
        issues.push("Label distribution is single-class, so training must remain blocked.");
    }
    return issues;
}

// ── FIX-F: Safe-state status ─────────────────────────────────────────────────

export type SafeStateLevel = "safe" | "warning" | "danger";

export interface SafeStateStatus {
    confirmed: boolean;
    level: SafeStateLevel;
    title: string;
    message: string;
}

/**
 * Derive a human-readable safe-state status from the runtime-status response.
 *
 * safe_state_confirmed=True  → level="safe"   — ML is disabled, rule-based only.
 * ml_enabled=False but no snapshot → level="warning" — state unknown.
 * ml_enabled=True            → level="danger" — ML is active, alert admin.
 */
export function buildSafeStateStatus(
    mlEnabled: boolean,
    safeStateConfirmed: boolean,
    snapshotMissing: boolean,
): SafeStateStatus {
    if (snapshotMissing) {
        return {
            confirmed: false,
            level: "warning",
            title: "ML State Unknown",
            message: "No ML snapshot available. Bot has not yet synced its ML config to the admin DB.",
        };
    }
    if (!mlEnabled && safeStateConfirmed) {
        return {
            confirmed: true,
            level: "safe",
            title: "ML Disabled — Safe State",
            message: "ML_ENABLED=False. Bot is running rule-based strategy only. No live AI scoring is active.",
        };
    }
    // ml_enabled=True or safe_state_confirmed=False
    return {
        confirmed: false,
        level: "danger",
        title: "ML Active — Review Required",
        message: "ML_ENABLED=True detected in the bot config snapshot. Live AI scoring is active. Verify this is intentional before trading with user capital.",
    };
}

// ── FIX-F: Document 1 checklist summary ──────────────────────────────────────

export interface ChecklistCategorySummary {
    category: string;
    items: MLPhaseIChecklistItem[];
    metCount: number;
    totalCount: number;
    allMet: boolean;
}

/**
 * Group the flat Document 1 checklist by category and compute per-category
 * met counts.  Used to render the checklist section in the Readiness tab.
 */
export function buildDocument1ChecklistSummary(
    phaseI: MLPhaseIReadiness,
): ChecklistCategorySummary[] {
    const byCategory = new Map<string, MLPhaseIChecklistItem[]>();
    for (const item of phaseI.checklist) {
        const cat = item.category ?? "other";
        if (!byCategory.has(cat)) byCategory.set(cat, []);
        byCategory.get(cat)!.push(item);
    }
    return Array.from(byCategory.entries()).map(([category, items]) => ({
        category,
        items,
        metCount: items.filter((i) => i.met).length,
        totalCount: items.length,
        allMet: items.every((i) => i.met),
    }));
}
