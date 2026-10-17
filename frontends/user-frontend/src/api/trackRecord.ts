/** Steps 6.4 / 8.6 -- public track-record reads (no authentication, no credentials sent). */
import type { SeriesDocument, TrackRecordIndex } from "../lib/trackRecord";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";
const BASE = `${API_BASE}/api/v1/public/track-record`;

async function getJson<T>(url: string): Promise<T> {
    const res = await fetch(url, { method: "GET", credentials: "omit", headers: { Accept: "application/json" } });
    if (!res.ok) throw new Error(`TRACK_RECORD_HTTP_${res.status}`);
    return (await res.json()) as T;
}

export const getTrackRecordIndex = (): Promise<TrackRecordIndex> => getJson<TrackRecordIndex>(BASE);

export const getTrackRecordSeries = (seriesId: string): Promise<SeriesDocument> =>
    getJson<SeriesDocument>(`${BASE}?series_id=${encodeURIComponent(seriesId)}`);
