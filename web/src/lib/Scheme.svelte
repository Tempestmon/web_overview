<script lang="ts">
  import { onMount } from 'svelte';
  import type { FaultKind, Snapshot, Topology } from './api';
  import Node from './Node.svelte';
  import { ParticleField } from './Particles';

  interface Props {
    topology: Topology;
    snapshot: Snapshot | null;
    myFaults: Set<number>;
    onBreak: (node: string, kind: FaultKind) => void;
    onHeal: (id: number) => void;
  }
  let { topology, snapshot, myFaults, onBreak, onHeal }: Props = $props();

  const VIEW_W = 1400;
  const VIEW_H = 800;

  let canvas: HTMLCanvasElement;
  let field: ParticleField | null = null;

  onMount(() => {
    field = new ParticleField(canvas, VIEW_W, VIEW_H);
    field.setTopology(topology);
    field.start();
    const ro = new ResizeObserver(() => resizeCanvas());
    ro.observe(canvas);
    resizeCanvas();
    return () => {
      field?.stop();
      ro.disconnect();
    };
  });

  function resizeCanvas() {
    const rect = canvas.getBoundingClientRect();
    canvas.width = Math.round(rect.width * devicePixelRatio);
    canvas.height = Math.round(rect.height * devicePixelRatio);
  }

  $effect(() => {
    if (snapshot && field) field.update(snapshot);
  });

  const pos = $derived(new Map(topology.nodes.map((n) => [n.id, n.pos])));
  const nodeState = $derived(new Map((snapshot?.nodes ?? []).map((n) => [n.id, n])));
  const edgeRate = $derived(new Map((snapshot?.edges ?? []).map((e) => [`${e.from}->${e.to}`, e])));
  const faultByNode = $derived(new Map((snapshot?.faults ?? []).map((f) => [f.node, f])));

  const busyReason = $derived.by(() => {
    if (!snapshot) return 'нет связи с контроллером';
    if (snapshot.faults.length > 0) {
      const longest = Math.max(...snapshot.faults.map((f) => f.remaining_s));
      return `уже что-то сломано, подождите ${Math.ceil(longest)} с`;
    }
    if (snapshot.cooldown_s > 0) return `система восстанавливается, ${Math.ceil(snapshot.cooldown_s)} с`;
    return null;
  });

  function edgeClass(from: string, to: string) {
    const r = edgeRate.get(`${from}->${to}`);
    if (!r) return 'idle';
    // A stray 5xx per minute is noise; paint red only when errors are a real share.
    if (r.rps > 0 && r.error_rps / r.rps > 0.05) return 'erroring';
    return r.rps > 0 ? 'live' : 'idle';
  }
</script>

<div class="scheme" style="aspect-ratio:{VIEW_W}/{VIEW_H}">
  <svg viewBox="0 0 {VIEW_W} {VIEW_H}" preserveAspectRatio="none">
    <defs>
      <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 0 L 10 5 L 0 10 z" fill="currentColor" />
      </marker>
    </defs>
    {#each topology.edges as e}
      {@const a = pos.get(e.from)}
      {@const b = pos.get(e.to)}
      {#if a && b}
        {@const r = edgeRate.get(`${e.from}->${e.to}`)}
        <line class="edge {edgeClass(e.from, e.to)} {e.dep}" x1={a.x} y1={a.y} x2={b.x} y2={b.y}>
          <title>{e.from} → {e.to}: {r ? `${r.rps.toFixed(1)} rps, p95 ${Math.round(r.p95_ms)} мс, ошибок ${r.error_rps.toFixed(2)}/с` : 'нет данных'}</title>
        </line>
        <text class="proto" x={(a.x + b.x) / 2} y={(a.y + b.y) / 2 - 6} text-anchor="middle">{e.proto}</text>
        {#if r?.breaker && r.breaker !== 'closed'}
          <text class="breaker {r.breaker}" x={(a.x + b.x) / 2} y={(a.y + b.y) / 2 + 14} text-anchor="middle">CB {r.breaker}</text>
        {/if}
      {/if}
    {/each}
  </svg>
  <canvas bind:this={canvas}></canvas>
  {#each topology.nodes as node (node.id)}
    {@const s = nodeState.get(node.id)}
    {@const f = faultByNode.get(node.id)}
    <Node
      {node}
      nodeState={s?.state ?? 'healthy'}
      fault={s?.fault ?? null}
      activeFaultId={f?.id ?? null}
      remainingS={f?.remaining_s ?? 0}
      mine={f ? myFaults.has(f.id) : false}
      {busyReason}
      viewW={VIEW_W}
      viewH={VIEW_H}
      onBreak={(kind) => onBreak(node.id, kind)}
      onHeal={() => f && onHeal(f.id)}
    />
  {/each}
</div>

<style>
  .scheme { position: relative; width: 100%; max-width: 1400px; margin: 0 auto; }
  svg, canvas { position: absolute; inset: 0; width: 100%; height: 100%; }
  canvas { pointer-events: none; }
  .edge { stroke: var(--line); stroke-width: 1.5; }
  .edge.live { stroke: var(--ink); }
  .edge.erroring { stroke: var(--red); }
  .edge.soft { stroke-dasharray: 6 5; }
  .proto { font-family: var(--mono); font-size: 11px; fill: var(--muted); }
  .breaker { font-family: var(--mono); font-size: 11px; font-weight: 700; fill: var(--red); }
  .breaker.half-open { fill: #9a7b00; }
</style>
