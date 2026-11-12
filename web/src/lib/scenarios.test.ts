import { describe, expect, it } from 'vitest';
import type { NodeState, Snapshot, Topology } from './api';
import { advance, availableScenarios, SCENARIOS } from './scenarios';
import topologyJson from '../../../topology.json';

const topology = topologyJson as unknown as Topology;

function snapshot(overrides: {
  states?: Record<string, NodeState>;
  edges?: Record<string, Partial<Snapshot['edges'][number]>>;
  faults?: string[];
  kafka_lag?: number;
}): Snapshot {
  return {
    ts_ms: 0,
    nodes: topology.nodes.map((n) => ({ id: n.id, state: overrides.states?.[n.id] ?? 'healthy', fault: null })),
    edges: topology.edges.map((e) => ({
      from: e.from, to: e.to, rps: 5, error_rps: 0, p95_ms: 20,
      breaker: e.to === 'payment-svc' ? 'closed' : null,
      ...overrides.edges?.[`${e.from}->${e.to}`],
    })),
    faults: (overrides.faults ?? []).map((node, id) => ({ id, node, kind: 'stop', remaining_s: 30 })),
    cooldown_s: 0,
    people_online: 10,
    orders_per_min: 10,
    kafka_lag: overrides.kafka_lag ?? 0,
    lead_time_s: 15,
    load: null,
  };
}

const scenario = (id: string) => SCENARIOS.find((s) => s.id === id)!;
const calm = snapshot({});

describe('scenario catalogue', () => {
  it('references only faults the real topology declares', () => {
    expect(availableScenarios(topology).map((s) => s.id)).toEqual(SCENARIOS.map((s) => s.id));
  });

  it('hides a scenario whose fault the topology no longer offers', () => {
    const withoutKitchenFaults: Topology = {
      ...topology,
      nodes: topology.nodes.map((n) => (n.id === 'kitchen-svc' ? { ...n, faults: [] } : n)),
    };
    expect(availableScenarios(withoutKitchenFaults).map((s) => s.id)).not.toContain('kitchen-stop');
  });

  it('never completes a step on a calm stand', () => {
    for (const s of SCENARIOS) expect(advance(0, s, calm, calm), s.id).toBe(0);
  });
});

describe('advance', () => {
  const kitchen = scenario('kitchen-stop');
  const kitchenDown = { states: { 'kitchen-svc': 'broken' as NodeState }, faults: ['kitchen-svc'] };

  it('ticks every step already visible in one snapshot', () => {
    expect(advance(0, kitchen, snapshot({ ...kitchenDown, kafka_lag: 40 }), calm)).toBe(3);
  });

  it('waits for the backlog to grow past the baseline', () => {
    expect(advance(0, kitchen, snapshot({ ...kitchenDown, kafka_lag: 2 }), calm)).toBe(2);
  });

  it('keeps the order: recovery does not count before the fault was seen', () => {
    expect(advance(0, kitchen, calm, calm)).toBe(0);
    expect(advance(3, kitchen, calm, calm)).toBe(5);
  });

  it('holds the recovery step while the backlog is still draining', () => {
    expect(advance(3, kitchen, snapshot({ kafka_lag: 30 }), calm)).toBe(4);
  });
});

describe('scenario conditions', () => {
  it('gateway failover needs the dead replica to carry no traffic', () => {
    const down = { states: { 'gateway-1': 'broken' as NodeState }, faults: ['gateway-1'] };
    expect(advance(0, scenario('gateway-stop'), snapshot(down), calm)).toBe(1);
    const drained = snapshot({ ...down, edges: { 'traefik->gateway-1': { rps: 0 } } });
    expect(advance(0, scenario('gateway-stop'), drained, calm)).toBe(3);
  });

  it('postgres outage is complete only when customers see errors', () => {
    const cascade = {
      states: { postgres: 'broken', 'order-svc': 'degraded', 'gateway-1': 'degraded', 'gateway-2': 'degraded', traefik: 'degraded' } as Record<string, NodeState>,
      faults: ['postgres'],
    };
    expect(advance(0, scenario('postgres-stop'), snapshot(cascade), calm)).toBe(2);
    const failing = snapshot({ ...cascade, edges: { 'traefik->gateway-1': { rps: 3, error_rps: 2 }, 'traefik->gateway-2': { rps: 3, error_rps: 2 } } });
    expect(advance(0, scenario('postgres-stop'), failing, calm)).toBe(3);
  });

  it('payment errors are followed through the breaker states', () => {
    const payment = scenario('payment-errors');
    const open = snapshot({
      states: { 'payment-svc': 'broken', 'order-svc': 'degraded' },
      faults: ['payment-svc'],
      edges: { 'order-svc->payment-svc': { breaker: 'open' } },
    });
    expect(advance(0, payment, open, calm)).toBe(3);
    const stillOpen = snapshot({ edges: { 'order-svc->payment-svc': { breaker: 'open' } } });
    expect(advance(3, payment, stillOpen, calm)).toBe(3);
    expect(advance(3, payment, calm, calm)).toBe(4);
  });

  it('latency degrades gateways through p95, not through container state', () => {
    const slow = snapshot({
      states: { 'order-svc': 'broken', 'gateway-2': 'degraded' },
      faults: ['order-svc'],
      edges: { 'gateway-2->order-svc': { p95_ms: 1400 } },
    });
    expect(advance(0, scenario('order-latency'), slow, calm)).toBe(3);
  });
});
