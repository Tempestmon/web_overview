export type FaultKind = 'stop' | 'pause' | 'latency' | 'loss' | 'app-error';
export type NodeKind = 'actor' | 'lb' | 'service' | 'db' | 'cache' | 'broker';
export type NodeState = 'healthy' | 'broken' | 'degraded' | 'recovering';

export interface TopoNode {
  id: string;
  label: string;
  kind: NodeKind;
  container?: string | null;
  pos: { x: number; y: number };
  grafana?: string | null;
  faults: FaultKind[];
  ops_port?: number | null;
}

export interface TopoEdge {
  from: string;
  to: string;
  proto: 'http' | 'grpc' | 'sql' | 'redis' | 'kafka';
  dep: 'hard' | 'soft';
}

export interface Topology {
  nodes: TopoNode[];
  edges: TopoEdge[];
}

export interface Snapshot {
  ts_ms: number;
  nodes: { id: string; state: NodeState; fault: FaultKind | null }[];
  edges: { from: string; to: string; rps: number; error_rps: number; p95_ms: number; breaker: string | null }[];
  faults: { id: number; node: string; kind: FaultKind; remaining_s: number }[];
  cooldown_s: number;
  people_online: number;
  orders_per_min: number;
  kafka_lag: number;
  lead_time_s: number;
}

export interface ApiFailure {
  error: string;
  retry_after_s?: number | null;
}

export async function fetchTopology(): Promise<Topology> {
  const r = await fetch('/api/topology');
  if (!r.ok) throw new Error(`topology: ${r.status}`);
  return r.json();
}

/** EventSource reconnects by itself; we only surface connectivity. */
export function connectStream(onSnapshot: (s: Snapshot) => void, onStatus: (online: boolean) => void): () => void {
  const es = new EventSource('/api/stream');
  es.addEventListener('snapshot', (e) => {
    onStatus(true);
    onSnapshot(JSON.parse((e as MessageEvent).data));
  });
  es.onerror = () => onStatus(false);
  return () => es.close();
}

export async function startFault(node: string, kind: FaultKind): Promise<{ ok: true; id: number } | { ok: false; failure: ApiFailure }> {
  const r = await fetch('/api/faults', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ node, kind }),
  });
  const body = await r.json();
  return r.ok ? { ok: true, id: body.id } : { ok: false, failure: body };
}

export async function stopFault(id: number): Promise<boolean> {
  const r = await fetch(`/api/faults/${id}`, { method: 'DELETE' });
  return r.ok;
}
