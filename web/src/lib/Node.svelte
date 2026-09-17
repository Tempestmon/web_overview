<script lang="ts">
  import type { FaultKind, NodeState, TopoNode } from './api';

  interface Props {
    node: TopoNode;
    nodeState: NodeState;
    fault: FaultKind | null;
    activeFaultId: number | null;
    remainingS: number;
    mine: boolean;
    busyReason: string | null;
    viewW: number;
    viewH: number;
    onBreak: (kind: FaultKind) => void;
    onHeal: () => void;
  }
  let { node, nodeState, fault, activeFaultId, remainingS, mine, busyReason, viewW, viewH, onBreak, onHeal }: Props = $props();

  let menuOpen = $state(false);

  const kindLabel: Record<string, string> = {
    actor: 'люди', lb: 'балансер', service: 'сервис', db: 'БД', cache: 'кеш', broker: 'брокер',
  };
  const stateLabel: Record<NodeState, string> = {
    healthy: 'работает', broken: 'сломан', degraded: 'деградация', recovering: 'поднимается',
  };
  const faultLabel: Record<FaultKind, string> = {
    stop: 'Остановить', pause: 'Заморозить', latency: 'Замедлить сеть', loss: 'Терять пакеты', 'app-error': 'Инжектить ошибки',
  };
  const faultVerb: Record<FaultKind, string> = {
    stop: 'остановлен', pause: 'заморожен', latency: 'тормозит', loss: 'теряет пакеты', 'app-error': 'ошибки 50%',
  };

  function pick(kind: FaultKind) {
    menuOpen = false;
    onBreak(kind);
  }
</script>

<div
  class="node {nodeState} {node.kind}"
  style="left:{(node.pos.x / viewW) * 100}%; top:{(node.pos.y / viewH) * 100}%"
>
  <div class="head">
    <span class="kind">{kindLabel[node.kind]}</span>
    <span class="status">{stateLabel[nodeState]}</span>
  </div>
  <div class="label">{node.label}</div>
  {#if node.kind !== 'actor'}
    <div class="actions">
      {#if activeFaultId !== null && fault}
        <span class="num countdown">{faultVerb[fault]} · {Math.ceil(remainingS)} с</span>
        {#if mine}<button onclick={onHeal}>Починить</button>{/if}
      {:else if node.faults.length > 0}
        <div class="menu-wrap">
          <button disabled={busyReason !== null} title={busyReason ?? ''} onclick={() => (menuOpen = !menuOpen)}>
            Сломать ▾
          </button>
          {#if menuOpen}
            <div class="menu" role="menu">
              {#each node.faults as kind}
                <button onclick={() => pick(kind)}>{faultLabel[kind]}</button>
              {/each}
            </div>
          {/if}
        </div>
      {/if}
      {#if node.grafana}
        <a class="grafana" href={`/grafana/${node.grafana}`} target="_blank" rel="noopener" title="Открыть в Grafana">📈</a>
      {/if}
    </div>
  {/if}
</div>

<style>
  .node {
    position: absolute; width: 156px; min-height: 72px; transform: translate(-50%, -50%);
    background: var(--paper); border: 1.5px solid var(--ink); border-radius: 4px; padding: 6px 8px;
    font-size: 12px; transition: background 0.3s, color 0.3s, border-color 0.3s;
  }
  .node.actor { border-style: dotted; }
  .node.degraded { background: var(--yellow); }
  .node.broken { background: var(--red); color: var(--paper); border-color: var(--red); }
  .node.broken button { color: var(--paper); border-color: var(--paper); background: transparent; }
  .node.broken button:hover { background: var(--paper); color: var(--red); }
  .node.recovering { border-style: dashed; color: var(--muted); }
  .head { display: flex; justify-content: space-between; font-size: 10px; text-transform: uppercase; letter-spacing: 0.06em; opacity: 0.75; }
  .label { font-weight: 600; font-size: 14px; margin: 3px 0 6px; }
  .actions { display: flex; align-items: center; gap: 6px; }
  .actions button { padding: 2px 8px; font-size: 12px; }
  .menu-wrap { position: relative; }
  .menu { position: absolute; top: 100%; left: 0; z-index: 10; display: flex; flex-direction: column; background: var(--paper); border: 1px solid var(--ink); margin-top: 2px; }
  .menu button { border: 0; border-bottom: 1px solid var(--line); text-align: left; white-space: nowrap; color: var(--ink); background: var(--paper); }
  .menu button:last-child { border-bottom: 0; }
  .countdown { font-size: 11px; }
  .grafana { margin-left: auto; text-decoration: none; font-size: 14px; }
</style>
