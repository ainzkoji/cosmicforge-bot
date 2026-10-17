/**
 * Section H Step 8.5 -- public status page (/status): parsing and presentation of GET /api/public/status.
 *
 * The payload is an allow-list built by the backend (component name + state, admin-authored notices). This
 * module only reads the fields it knows and drops anything else, so an unexpected field can never be shown.
 */

export type ComponentState = "OPERATIONAL" | "DEGRADED" | "OUTAGE" | "MAINTENANCE";
export const COMPONENT_STATES: readonly ComponentState[] = ["OPERATIONAL", "DEGRADED", "OUTAGE", "MAINTENANCE"];

export interface StatusComponent {
    key: string;
    name: string;
    status: ComponentState;
}

export interface IncidentUpdate {
    status: string;
    message: string;
    at: string | null;
}

export interface StatusIncident {
    id: string;
    title: string;
    status: string;
    impact: ComponentState;
    components: string[];
    started_at: string | null;
    updated_at: string | null;
    resolved_at: string | null;
    updates: IncidentUpdate[];
}

export interface PublicStatus {
    generated_at: string;
    overall: ComponentState;
    data_available: boolean;
    notice: string | null;
    components: StatusComponent[];
    incidents: StatusIncident[];
}

export const STATE_LABELS: Record<ComponentState, string> = {
    OPERATIONAL: "Operational",
    DEGRADED: "Degraded performance",
    OUTAGE: "Outage",
    MAINTENANCE: "Maintenance",
};

export const STATE_TONE: Record<ComponentState, string> = {
    OPERATIONAL: "bg-green-500/10 text-green-500 border-green-500/20",
    DEGRADED: "bg-yellow-500/10 text-yellow-500 border-yellow-500/20",
    OUTAGE: "bg-red-500/10 text-red-500 border-red-500/20",
    MAINTENANCE: "bg-blue-500/10 text-blue-400 border-blue-500/20",
};

export const INCIDENT_STATUS_LABELS: Record<string, string> = {
    INVESTIGATING: "Investigating",
    IDENTIFIED: "Identified",
    MONITORING: "Monitoring",
    RESOLVED: "Resolved",
};

function state(v: unknown): ComponentState {
    return COMPONENT_STATES.includes(v as ComponentState) ? (v as ComponentState) : "DEGRADED";
}

const text = (v: unknown): string => (typeof v === "string" ? v : "");
const maybe = (v: unknown): string | null => (typeof v === "string" ? v : null);

/** Keep only known fields; an unknown state is shown as DEGRADED, never as OPERATIONAL. */
export function parsePublicStatus(raw: unknown): PublicStatus {
    const r = (raw ?? {}) as Record<string, unknown>;
    const components = Array.isArray(r.components) ? r.components : [];
    const incidents = Array.isArray(r.incidents) ? r.incidents : [];
    return {
        generated_at: text(r.generated_at),
        overall: state(r.overall),
        data_available: r.data_available === true,
        notice: maybe(r.notice),
        components: components.map((c) => {
            const cc = (c ?? {}) as Record<string, unknown>;
            return { key: text(cc.key), name: text(cc.name), status: state(cc.status) };
        }),
        incidents: incidents.map((i) => {
            const ii = (i ?? {}) as Record<string, unknown>;
            return {
                id: text(ii.id), title: text(ii.title), status: text(ii.status), impact: state(ii.impact),
                components: Array.isArray(ii.components) ? ii.components.filter((x): x is string => typeof x === "string") : [],
                started_at: maybe(ii.started_at), updated_at: maybe(ii.updated_at), resolved_at: maybe(ii.resolved_at),
                updates: (Array.isArray(ii.updates) ? ii.updates : []).map((u) => {
                    const uu = (u ?? {}) as Record<string, unknown>;
                    return { status: text(uu.status), message: text(uu.message), at: maybe(uu.at) };
                }),
            };
        }),
    };
}

/** A status that could not be loaded at all: never "all operational". */
export function unreachableStatus(): PublicStatus {
    return { generated_at: "", overall: "DEGRADED", data_available: false,
             notice: "The status service could not be reached.", components: [], incidents: [] };
}

export function headline(s: PublicStatus): string {
    if (!s.data_available) return s.notice || "Live status information is temporarily unavailable.";
    switch (s.overall) {
        case "OPERATIONAL": return "All systems operational";
        case "MAINTENANCE": return "Scheduled maintenance in progress";
        case "OUTAGE": return "Some systems are experiencing an outage";
        default: return "Some systems are experiencing degraded performance";
    }
}

export function activeIncidents(s: PublicStatus): StatusIncident[] {
    return s.incidents.filter((i) => i.status !== "RESOLVED");
}

export function resolvedIncidents(s: PublicStatus): StatusIncident[] {
    return s.incidents.filter((i) => i.status === "RESOLVED");
}
