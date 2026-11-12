<script lang="ts">
  import { onMount } from 'svelte';
  import { connectStream, fetchTopology, startFault, stopFault, type ApiFailure, type FaultKind, type Snapshot, type Topology } from '../lib/api';
  import Scheme from '../lib/Scheme.svelte';
  import Tour from '../lib/Tour.svelte';
  import { advance, availableScenarios, type Scenario } from '../lib/scenarios';

  let { navigate }: { navigate: (to: string) => void } = $props();

  let topology = $state<Topology | null>(null);
  let snapshot = $state<Snapshot | null>(null);
  let online = $state(false);
  let notice = $state<string | null>(null);
  let myFaults = $state(new Set<number>());
  let tour = $state<{ scenario: Scenario; baseline: Snapshot; done: number } | null>(null);

  onMount(() => {
    fetchTopology().then((t) => (topology = t)).catch((e) => (notice = String(e)));
    return connectStream((s) => (snapshot = s), (o) => (online = o));
  });

  function refusal(failure: ApiFailure): string {
    const wait = failure.retry_after_s ? `, повторите через ${failure.retry_after_s} с` : '';
    return `${failure.error}${wait}`;
  }

  async function onBreak(node: string, kind: FaultKind, ttlS?: number): Promise<boolean> {
    const r = await startFault(node, kind, ttlS);
    if (r.ok) myFaults = new Set([...myFaults, r.id]);
    notice = r.ok ? null : refusal(r.failure);
    return r.ok;
  }

  async function onStartTour(scenario: Scenario) {
    if (!snapshot) return;
    const baseline = snapshot;
    if (await onBreak(scenario.node, scenario.kind, scenario.ttlS)) tour = { scenario, baseline, done: 0 };
  }

  $effect(() => {
    if (!tour || !snapshot) return;
    const done = advance(tour.done, tour.scenario, snapshot, tour.baseline);
    if (done !== tour.done) tour = { ...tour, done };
  });

  const scenarios = $derived(topology ? availableScenarios(topology) : []);
  const busy = $derived(!snapshot || snapshot.faults.length > 0 || snapshot.cooldown_s > 0);

  async function onHeal(id: number) {
    if (await stopFault(id)) myFaults = new Set([...myFaults].filter((x) => x !== id));
  }

  const labelById = $derived(new Map((topology?.nodes ?? []).map((n) => [n.id, n.label])));
  const faultVerb: Record<FaultKind, string> = {
    stop: 'остановлен', pause: 'заморожен', latency: 'тормозит по сети', loss: 'теряет 30% пакетов', 'app-error': 'отвечает ошибками',
  };
</script>

<header class="topbar">
  <h1>Chaos Panel</h1>
  <div class="stat"><span>людей онлайн</span><b class="num">{Math.round(snapshot?.people_online ?? 0)}</b></div>
  <div class="stat"><span>заказов / мин</span><b class="num">{Math.round(snapshot?.orders_per_min ?? 0)}</b></div>
  <div class="stat"><span>очередь кухни</span><b class="num">{Math.round(snapshot?.kafka_lag ?? 0)}</b></div>
  <div class="stat"><span>время заказа</span><b class="num">{snapshot?.lead_time_s ? Math.round(snapshot.lead_time_s) + ' с' : '—'}</b></div>
  <div class="stat"><span>контроллер</span><b>{online ? 'online' : 'offline'}</b></div>
  <nav>
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
    <span class="aside">жёлтым помечены зависимые компоненты, в том числе по живым метрикам</span>
  </div>
{:else if snapshot && snapshot.cooldown_s > 0}
  <div class="banner gray">Система восстанавливается, следующий сбой через <span class="num">{Math.ceil(snapshot.cooldown_s)} с</span></div>
{:else}
  <div class="banner gray">Нажмите «Сломать» на любом компоненте. Сбой длится минуту и откатывается сам. Попробуйте в это время <a href="/order" onclick={(e) => { e.preventDefault(); navigate('/order'); }}>сделать заказ</a>.</div>
{/if}

{#if notice}
  <div class="banner gray">{notice}</div>
{/if}

<Tour {scenarios} active={tour?.scenario ?? null} done={tour?.done ?? 0} {busy} onStart={onStartTour} onClose={() => (tour = null)} />

<main>
  {#if topology}
    <p class="scroll-hint">Схема шире экрана, её можно листать вбок →</p>
    <Scheme {topology} {snapshot} {myFaults} {onBreak} {onHeal} />
  {:else}
    <p style="padding:20px">Загружаю схему…</p>
  {/if}
</main>

<style>
  main { padding: 16px; }
  .scroll-hint { display: none; margin: 0 0 8px; font-size: 12px; color: var(--muted); }
  @media (max-width: 1200px) { .scroll-hint { display: block; } }
  @media (max-width: 720px) { main { padding: 8px; } }
</style>
