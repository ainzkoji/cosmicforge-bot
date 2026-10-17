/**
 * Step 7.3 -- market scope of a CATI deployment, as the portal shows it.
 *
 * Pure functions and types only (no DOM, no network). The backend's market
 * availability answer (/api/v1/markets/availability) is the authority for why
 * a market is or is not available; this module only words it. Traditional
 * markets (futures, FX, equities) are never selectable on the CATI path in
 * this build, whatever an answer says: the deployment backend refuses them
 * with TRADFI_NOT_AVAILABLE and no execution path for them exists.
 */

export type MarketScope = "CRYPTO_ONLY" | "CRYPTO_AND_TRADFI";

export const DEFAULT_MARKET_SCOPE: MarketScope = "CRYPTO_ONLY";

/** No traditional-market execution exists in this build; the option is shown, never enabled. */
export const TRADFI_SELECTABLE_IN_THIS_BUILD = false;

export interface MarketReason {
    code: string;
    message: string;
}

export interface MarketEntry {
    market_id: string;
    asset_class: string;
    market_type: string;
    venue: string;
    product: string;
    is_tradfi: boolean;
    calendar_id: string;
    calendar_verification_status: string;
    calendar_verified: boolean;
    families: string[];
    certified_families: string[];
    activation_stage: string | null;
    connected_accounts: number;
    available: boolean;
    reasons: MarketReason[];
}

export interface MarketAvailability {
    version: string;
    markets: MarketEntry[];
    scopes: Record<MarketScope, { available: boolean; reasons: MarketReason[] }>;
    default_scope: MarketScope;
    live_trading: string;
}

export interface MarketScopeOption {
    scope: MarketScope;
    title: string;
    description: string;
    selectable: boolean;
    reasons: string[];
}

const UNAVAILABLE_FALLBACK = "Traditional markets (futures, FX, equities) are not available yet.";

function reasonTexts(reasons: MarketReason[] | undefined): string[] {
    const out: string[] = [];
    for (const r of reasons ?? []) {
        const text = (r?.message || r?.code || "").trim();
        if (text && !out.includes(text)) out.push(text);
    }
    return out;
}

/**
 * The two scope options. Crypto only is always selectable (the existing
 * behaviour). Crypto and traditional markets is shown as unavailable with the
 * backend's reasons, or a plain statement when the answer could not be loaded.
 */
export function marketScopeOptions(availability: MarketAvailability | null | undefined): MarketScopeOption[] {
    const tradfi = availability?.scopes?.CRYPTO_AND_TRADFI;
    const reasons = reasonTexts(tradfi?.reasons);
    return [
        {
            scope: "CRYPTO_ONLY",
            title: "Crypto only",
            description: "USDT perpetual futures on your connected crypto exchange account.",
            selectable: true,
            reasons: [],
        },
        {
            scope: "CRYPTO_AND_TRADFI",
            title: "Crypto and traditional markets",
            description: "Futures, FX and equities in addition to crypto.",
            selectable: TRADFI_SELECTABLE_IN_THIS_BUILD && tradfi?.available === true,
            reasons: reasons.length ? reasons : [availability ? UNAVAILABLE_FALLBACK : `${UNAVAILABLE_FALLBACK} Market availability could not be loaded.`],
        },
    ];
}

/** The scope a form may actually carry: anything not selectable falls back to crypto only. */
export function effectiveMarketScope(requested: MarketScope | null | undefined, availability?: MarketAvailability | null): MarketScope {
    if (!requested || requested === "CRYPTO_ONLY") return "CRYPTO_ONLY";
    const option = marketScopeOptions(availability).find((o) => o.scope === requested);
    return option?.selectable ? requested : "CRYPTO_ONLY";
}

/** How a bot's market scope is shown. A bot without one predates Step 7.3 and is crypto only. */
export function marketScopeLabel(scope: string | null | undefined): string {
    const value = String(scope ?? "").trim().toUpperCase();
    if (!value || value === "CRYPTO_ONLY") return "Crypto only";
    if (value === "CRYPTO_AND_TRADFI") return "Crypto and traditional markets";
    if (value.startsWith("LEGACY_")) return `Legacy ${value.slice("LEGACY_".length).toLowerCase()} bot`;
    return value;
}
