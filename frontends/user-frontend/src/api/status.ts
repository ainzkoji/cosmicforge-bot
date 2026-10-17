/** Public status page (GET /api/public/status), Section H Step 8.5. No authentication, no credentials sent. */
import { API_BASE } from "./http";
import { parsePublicStatus, unreachableStatus } from "../lib/statusPage";
import type { PublicStatus } from "../lib/statusPage";

export async function getPublicStatus(): Promise<PublicStatus> {
    try {
        const res = await fetch(`${API_BASE}/api/public/status`, { credentials: "omit" });
        if (!res.ok) return unreachableStatus();
        return parsePublicStatus(await res.json());
    } catch {
        return unreachableStatus();
    }
}
