/**
 * FIX-F: Frontend logic tests for ML runtime status utilities.
 *
 * 8 tests covering the pure utility functions in mlMonitoringUtils.ts.
 *
 * These tests have NO React/DOM/browser dependencies.  They are run via the
 * Node.js built-in test runner using the --experimental-strip-types flag
 * (available since Node 22.6, production-ready in Node 25):
 *
 *   node --experimental-strip-types src/tests/fix_f.test.ts
 *
 * Or via: npm test
 *
 * CRITICAL: None of these tests enable ML, call any API, or modify any config.
 * They only test pure TypeScript logic functions.
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";

// ---------------------------------------------------------------------------
// Inline the pure utility functions we want to test.
// We cannot use path aliases (@/...) in node:test without a bundler, so we
// reproduce the functions directly.  The canonical source is
// src/utils/mlMonitoringUtils.ts — these must stay in sync.
// ---------------------------------------------------------------------------

function formatPercent(value: number | null | undefined, digits = 1): string {
    if (value === null || value === undefined || Number.isNaN(value)) return "--";
    return `${value.toFixed(digits)}%`;
}

function formatNumber(value: number | null | undefined, digits = 0): string {
    if (value === null || value === undefined || Number.isNaN(value)) return "--";
    return new Intl.NumberFormat("en-US", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
    }).format(value);
}

function formatModeLabel(mode: string): string {
    if (mode === "shadow") return "Shadow Only";
    return mode.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

type SafeStateLevel = "safe" | "warning" | "danger";

interface SafeStateStatus {
    confirmed: boolean;
    level: SafeStateLevel;
    title: string;
    message: string;
}

function buildSafeStateStatus(
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
    return {
        confirmed: false,
        level: "danger",
        title: "ML Active — Review Required",
        message: "ML_ENABLED=True detected in the bot config snapshot. Live AI scoring is active. Verify this is intentional before trading with user capital.",
    };
}

interface ChecklistItem {
    id: string;
    label: string;
    met: boolean;
    detail: string;
    category: string;
}

interface PhaseIReadiness {
    checklist: ChecklistItem[];
    met_count: number;
    total_count: number;
    overall_ready: boolean;
    overall_note: string;
}

interface ChecklistCategorySummary {
    category: string;
    items: ChecklistItem[];
    metCount: number;
    totalCount: number;
    allMet: boolean;
}

function buildDocument1ChecklistSummary(phaseI: PhaseIReadiness): ChecklistCategorySummary[] {
    const byCategory = new Map<string, ChecklistItem[]>();
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

function buildReadinessIssues({
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("FIX-F: formatPercent", () => {
    it("returns '--' for null/undefined/NaN values", () => {
        assert.equal(formatPercent(null), "--");
        assert.equal(formatPercent(undefined), "--");
        assert.equal(formatPercent(NaN), "--");
    });

    it("formats positive percentage correctly", () => {
        assert.equal(formatPercent(27.9), "27.9%");
        assert.equal(formatPercent(100), "100.0%");
        assert.equal(formatPercent(0), "0.0%");
    });
});

describe("FIX-F: formatModeLabel", () => {
    it("converts 'shadow' to 'Shadow Only'", () => {
        assert.equal(formatModeLabel("shadow"), "Shadow Only");
    });

    it("converts underscore_mode to Title Case", () => {
        assert.equal(formatModeLabel("active"), "Active");
        assert.equal(formatModeLabel("not_ready"), "Not Ready");
    });
});

describe("FIX-F: buildSafeStateStatus", () => {
    it("returns safe level when ml_enabled=false and safe_state_confirmed=true", () => {
        const status = buildSafeStateStatus(false, true, false);
        assert.equal(status.confirmed, true);
        assert.equal(status.level, "safe");
        assert.ok(status.title.includes("Safe"), `Expected title to mention Safe, got: ${status.title}`);
        assert.ok(
            status.message.includes("ML_ENABLED=False"),
            `Expected message to confirm ML is disabled, got: ${status.message}`,
        );
    });

    it("returns danger level when ml_enabled=true in snapshot", () => {
        // This tests that the DASHBOARD correctly surfaces the dangerous state.
        // It does NOT enable ML — it only tests the display logic.
        const status = buildSafeStateStatus(true, false, false);
        assert.equal(status.confirmed, false);
        assert.equal(status.level, "danger");
        assert.ok(
            status.message.includes("ML_ENABLED=True"),
            `Expected danger message about ML being enabled, got: ${status.message}`,
        );
    });

    it("returns warning level when snapshot is missing", () => {
        const status = buildSafeStateStatus(false, false, true);
        assert.equal(status.confirmed, false);
        assert.equal(status.level, "warning");
        assert.ok(
            status.message.includes("No ML snapshot"),
            `Expected warning about missing snapshot, got: ${status.message}`,
        );
    });
});

describe("FIX-F: buildDocument1ChecklistSummary", () => {
    const sampleChecklist: PhaseIReadiness = {
        checklist: [
            { id: "fix_d_tp_break_even", label: "FIX-D: TP1/TP2 break-even", met: true, detail: "Deployed", category: "execution" },
            { id: "fix_e_ensemble_filter", label: "FIX-E: Ensemble filter", met: true, detail: "Deployed", category: "execution" },
            { id: "paper_trades_count", label: "≥100 paper trades", met: false, detail: "2/100", category: "accumulation" },
            { id: "fix_f_ml_visibility", label: "FIX-F: ML visibility", met: true, detail: "Deployed", category: "visibility" },
            { id: "ml_safety_state", label: "ML safety state", met: true, detail: "ML disabled", category: "safety" },
        ],
        met_count: 4,
        total_count: 5,
        overall_ready: false,
        overall_note: "4/5 gates satisfied",
    };

    it("groups checklist items by category", () => {
        const summary = buildDocument1ChecklistSummary(sampleChecklist);
        const categories = summary.map((s) => s.category);
        assert.ok(categories.includes("execution"), "Should have execution category");
        assert.ok(categories.includes("accumulation"), "Should have accumulation category");
        assert.ok(categories.includes("visibility"), "Should have visibility category");
    });

    it("counts met items per category correctly", () => {
        const summary = buildDocument1ChecklistSummary(sampleChecklist);
        const execution = summary.find((s) => s.category === "execution")!;
        assert.equal(execution.metCount, 2, "Both execution items are met");
        assert.equal(execution.totalCount, 2);
        assert.equal(execution.allMet, true);

        const accumulation = summary.find((s) => s.category === "accumulation")!;
        assert.equal(accumulation.metCount, 0, "Accumulation item is not met");
        assert.equal(accumulation.allMet, false);
    });
});

describe("FIX-F: buildReadinessIssues", () => {
    it("returns empty array when all gates are satisfied", () => {
        const issues = buildReadinessIssues({
            totalLinkedCompletedTrades: 200,
            requiredTrades: 200,
            wins: 50,
            requiredWins: 50,
            featureCoveragePct: 95,
            linkageHealthy: true,
            labelDistributionSingleClass: false,
            brokenFeatureCount: 0,
            tradesMissingCriticalFeatures: 0,
        });
        assert.equal(issues.length, 0, "No issues when all gates pass");
    });
});
