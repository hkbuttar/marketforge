export type Dataset = {
  dataset: string; status: string; row_count: number; null_rate?: number | null;
  duplicate_count?: number; quarantine_count?: number; latest_event_time?: string | null;
  last_successful_run?: string | null; last_successful_run_at?: string | null;
};
export type Security = { symbol: string; source: string; latest_price_date: string; latest_price: number };
export type PricePoint = { trade_date: string; close: number; daily_return: number | null; rolling_20d_volatility: number | null; volume: number };
export type Readiness = { status: string; checks: { component: string; status: string; detail: string }[]; cache: Record<string, number> };
export type Lineage = { dataset: string; nodes: { id: string; name: string; type: string }[]; edges: { from: string; to: string }[] };

const API = import.meta.env.VITE_API_URL ?? "";
async function get<T>(path: string): Promise<T> {
  const response = await fetch(`${API}${path}`);
  if (!response.ok) {
    const body = await response.json().catch(() => ({ detail: response.statusText }));
    throw new Error(body.detail ?? `Request failed (${response.status})`);
  }
  return response.json() as Promise<T>;
}
export const api = {
  readiness: async () => {
    const response = await fetch(`${API}/health/ready`);
    if (response.status !== 200 && response.status !== 503) throw new Error(`Readiness failed (${response.status})`);
    return response.json() as Promise<Readiness>;
  },
  health: () => get<{ data: Dataset[] }>("/api/pipeline/health"),
  datasets: () => get<{ data: Dataset[] }>("/api/datasets"),
  securities: () => get<{ data: Security[] }>("/api/securities?limit=100"),
  history: (symbol: string) => get<{ data: PricePoint[] }>(`/api/securities/${encodeURIComponent(symbol)}/history?limit=252`),
  lineage: (dataset: string) => get<Lineage>(`/api/datasets/${encodeURIComponent(dataset)}/lineage`),
};
