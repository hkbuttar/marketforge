export type Dataset = {
  dataset: string; status: string; row_count: number; null_rate?: number | null;
  duplicate_count?: number; quarantine_count?: number; latest_event_time?: string | null;
  last_successful_run?: string | null; last_successful_run_at?: string | null;
};
export type Security = { symbol: string; source: string; latest_price_date: string; latest_price: number };
export type PricePoint = { trade_date: string; close: number; daily_return: number | null; rolling_20d_volatility: number | null; volume: number };
export type Readiness = { status: string; checks: { component: string; status: string; detail: string }[]; cache: Record<string, number> };
export type Lineage = { dataset: string; nodes: { id: string; name: string; type: string }[]; edges: { from: string; to: string }[] };
export type PipelineRun = { run_id: string; job_name: string; dataset: string; run_type: string; started_at: string; finished_at: string; status: string; records_fetched: number; records_written: number; records_rejected: number; error: string | null };
export type QualityResult = { result_id: string; dataset: string; check_name: string; status: string; observed_value: number | null; message: string; evaluated_at: string };
export type QuarantineSummary = { total_records: number; artifact_files: number; groups: { source: string; error_type: string; records: number }[] };
export type Sector = { sector: string; latest_date: string; latest_average_return: number | null; securities_with_returns: number };
export type Breadth = { trade_date: string; market_breadth: number | null; advancers: number; decliners: number; unchanged: number; securities_with_returns: number };
export type Storage = { project_bytes: number; raw_bytes: number; transformed_bytes: number; metadata_bytes: number; quarantine_bytes: number; project_budget_bytes: number; raw_budget_bytes: number };
export type Benchmarks = { historical_rows: number; disk_footprint_bytes: number; parquet_compression_ratio: number; full_refresh: { wall_clock_seconds: number; peak_ram_bytes: number; bytes_read: number; bytes_written: number }; incremental: { wall_clock_seconds: number; peak_ram_bytes: number; bytes_read: number; bytes_written: number }; incremental_comparison: { runtime_speedup: number; write_reduction_percent: number }; compaction: { file_count_before: number; file_count_after: number; latency_before_ms: number; latency_after_ms: number } };

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
  runs: () => get<PipelineRun[]>("/api/pipeline/runs?limit=100"),
  quality: () => get<QualityResult[]>("/api/quality?limit=100"),
  quarantine: () => get<QuarantineSummary>("/api/quarantine/summary"),
  sectors: () => get<Sector[]>("/api/sectors?limit=100"),
  breadth: () => get<Breadth[]>("/api/market/breadth?limit=30"),
  storage: () => get<Storage>("/api/system/storage"),
  benchmarks: () => get<Benchmarks>("/api/system/benchmarks"),
};
