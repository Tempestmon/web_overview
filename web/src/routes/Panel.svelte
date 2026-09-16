<script lang="ts">
  import { onMount } from 'svelte';
  import { connectStream, fetchTopology, startFault, stopFault, type FaultKind, type Snapshot, type Topology } from '../lib/api';
  import Scheme from '../lib/Scheme.svelte';

  let { navigate }: { navigate: (to: string) => void } = $props();

  let topology = $state<Topology | null>(null);
  let snapshot = $state<Snapshot | null>(null);
  let online = $state(false);
  let notice = $state<string | null>(null);
  let myFaults = $state(new Set<number>());

  onMount(() => {
    fetchTopology().then((t) => (topology = t)).catch((e) => (notice = String(e)));
    return connectStream((s) => (snapshot = s), (o) => (online = o));
  });

  async function onBreak(node: string, kind: FaultKind) {
    const r = await startFault(node, kind);
    if (r.ok) {
      myFaults = new Set([...myFaults, r.id]);
      notice = null;
    } else {
      const wait = r.failure.retry_after_s ? `, повторите через ${r.failure.retry_after_s} с` : '';
      notice = `${r.failure.error}${wait}`;
    }
  }

  async function onHeal(id: number) {
    if (await stopFault(id)) myFaults = new Set([...myFaults].filter((x) => x !== id));
  }

  const labelById = $derived(new Map((topology?.nodes ?? []).map((n) => [n.id, n.label])));
  const faultVerb: Record<FaultKind, string> = { stop: 'остановлен', pause: 'заморожен' };
</script>

<header class="topbar">
  <h1>Chaos Panel</h1>
  <div class="stat"><span>людей онлайн</span><b class="num">{Math.round(snapshot?.people_online ?? 0)}</b></div>
  <div class="stat"><span>заказов / мин</span><b class="num">{Math.round(snapshot?.orders_per_min ?? 0)}</b></div>
  <div class="stat"><span>очередь кухни</span><b class="num">{Math.round(snapshot?.kafka_lag ?? 0)}</b></div>
  <div class="stat"><span>контроллер</span><b>{online ? 'online' : 'offline'}</b></div>
  <nav style="margin-left:auto">
    <a href="/order" onclick={(e) => { e.preventDefault(); navigate('/order'); }}>Заказать еду →</a>
    <a href="/grafana/d/overview" target="_blank" rel="noopener">Grafana ↗</a>
  </nav>
</header>

{#if snapshot && snapshot.faults.length > 0}
  <div class="banner red">
    {#each snapshot.faults as f}
      <span>
        <b>{labelById.get(f.node) ?? f.node}</b> {faultVerb[f.kind]}, вернётся через
        <span class="num">{Math.ceil(f.remaining_s)} с</span>
      </span>
      {#if myFaults.has(f.id)}<button onclick={() => onHeal(f.id)}>Починить сейчас</button>{/if}
    {/each}
    <span style="margin-left:auto">жёлтым помечены зависимые компоненты</span>
  </div>
{:else if snapshot && snapshot.cooldown_s > 0}
  <div class="banner gray">Система восстанавливается, следующий сбой через <span class="num">{Math.ceil(snapshot.cooldown_s)} с</span></div>
{:else}
  <div class="banner gray">Нажмите «Сломать» на любом компоненте. Сбой длится минуту и откатывается сам. Попробуйте в это время <a href="/order" onclick={(e) => { e.preventDefault(); navigate('/order'); }}>сделать заказ</a>.</div>
{/if}

{#if notice}
  <div class="banner gray">{notice}</div>
{/if}

<main>
  {#if topology}
    <Scheme {topology} {snapshot} {myFaults} {onBreak} {onHeal} />
  {:else}
    <p style="padding:20px">Загружаю схему…</p>
  {/if}
</main>

<style>
  main { padding: 16px; }
</style>
