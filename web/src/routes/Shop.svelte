<script lang="ts">
  import { onMount } from 'svelte';

  let { navigate }: { navigate: (to: string) => void } = $props();

  interface MenuItem { id: number; name: string; price_cents: number }
  interface Order { id: string; status: string; total_cents: number }

  let menu = $state<MenuItem[]>([]);
  let fromCache = $state<boolean | null>(null);
  let menuMs = $state(0);
  let menuError = $state<string | null>(null);
  let cart = $state<Record<number, number>>({});
  let order = $state<Order | null>(null);
  let history = $state<{ status: string; at: string }[]>([]);
  let orderError = $state<string | null>(null);
  let traceId = $state<string | null>(null);
  let placing = $state(false);
  let es: EventSource | null = null;

  const STEPS = ['accepted', 'paid', 'cooking', 'cooked', 'delivering', 'delivered'];
  const stepLabel: Record<string, string> = {
    accepted: 'Принят', paid: 'Оплачен', cooking: 'Готовится', cooked: 'Готов', delivering: 'Курьер в пути', delivered: 'Доставлен',
  };
  const stuckHint: Record<string, string> = {
    accepted: 'Заказ принят, ждём подтверждения оплаты.',
    paid: 'Оплачен и отправлен в очередь на кухню. Если кухня или брокер сломаны, он будет ждать здесь, пока их не починят.',
    cooking: 'Кухня готовит. Если её остановить сейчас, заказ доготовится после восстановления.',
    cooked: 'Готов, ждёт курьера. Сломанный курьер или брокер задержат его на этом шаге.',
    delivering: 'Курьер везёт. Остановка курьера заморозит доставку до починки.',
  };

  onMount(loadMenu);

  async function loadMenu() {
    const started = performance.now();
    menuError = null;
    try {
      const r = await fetch('/api/shop/menu');
      if (!r.ok) throw new Error((await r.json()).error ?? r.statusText);
      const body = await r.json();
      menu = body.items;
      fromCache = body.from_cache;
    } catch (e) {
      menuError = `Меню недоступно: ${e instanceof Error ? e.message : e}`;
    } finally {
      menuMs = Math.round(performance.now() - started);
    }
  }

  const total = $derived(menu.reduce((sum, m) => sum + (cart[m.id] ?? 0) * m.price_cents, 0));
  const lines = $derived(Object.entries(cart).filter(([, q]) => q > 0).map(([id, qty]) => ({ item_id: Number(id), qty })));

  function add(id: number, delta: number) {
    cart = { ...cart, [id]: Math.max(0, (cart[id] ?? 0) + delta) };
  }

  async function placeOrder() {
    placing = true;
    orderError = null;
    history = [];
    try {
      const r = await fetch('/api/shop/orders', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ lines }),
      });
      const body = await r.json();
      if (!r.ok) throw new Error(body.error ?? r.statusText);
      order = body;
      traceId = r.headers.get('x-trace-id');
      cart = {};
      follow(body.id);
    } catch (e) {
      orderError = `Заказ не принят: ${e instanceof Error ? e.message : e}`;
    } finally {
      placing = false;
    }
  }

  function follow(id: string) {
    es?.close();
    es = new EventSource(`/api/shop/orders/${id}/events`);
    es.addEventListener('status', (e) => {
      const o: Order = JSON.parse((e as MessageEvent).data);
      order = o;
      orderError = null;
      history = [...history, { status: o.status, at: new Date().toLocaleTimeString() }];
      if (o.status === 'delivered') es?.close();
    });
    es.addEventListener('error', (e) => {
      const msg = (e as MessageEvent).data;
      if (msg) orderError = `Статус недоступен: ${msg}. Продолжаю опрашивать…`;
    });
  }

  const rub = (cents: number) => (cents / 100).toLocaleString('ru-RU') + ' ₽';

  // Grafana Explore deep link into Tempo for the order's trace.
  function traceUrl(id: string) {
    const panes = {
      a: {
        datasource: 'tempo',
        queries: [{ refId: 'A', datasource: { type: 'tempo', uid: 'tempo' }, queryType: 'traceql', query: id }],
        range: { from: 'now-1h', to: 'now' },
      },
    };
    return `/grafana/explore?schemaVersion=1&panes=${encodeURIComponent(JSON.stringify(panes))}`;
  }
</script>

<header class="topbar">
  <h1>Доставка еды</h1>
  <span style="color:var(--muted);font-size:13px">та самая система со схемы, глазами пользователя</span>
  <nav style="margin-left:auto">
    <a href="/" onclick={(e) => { e.preventDefault(); navigate('/'); }}>← К схеме</a>
  </nav>
</header>

<main class="shop">
  <section>
    <h2>Меню
      {#if fromCache !== null}
        <small class="num">{menuMs} мс · {fromCache ? 'из кеша' : 'из базы'}</small>
      {/if}
    </h2>
    {#if menuMs > 1000 && !menuError}
      <div class="hint">Меню грузилось долго. Похоже, кеш недоступен и запрос ушёл напрямую в базу.</div>
    {/if}
    {#if menuError}
      <div class="hint error">{menuError} <button onclick={loadMenu}>Повторить</button></div>
    {/if}
    <ul class="menu">
      {#each menu as item (item.id)}
        <li>
          <span>{item.name}</span>
          <span class="num">{rub(item.price_cents)}</span>
          <span class="qty">
            <button onclick={() => add(item.id, -1)} disabled={!cart[item.id]}>−</button>
            <b class="num">{cart[item.id] ?? 0}</b>
            <button onclick={() => add(item.id, 1)}>+</button>
          </span>
        </li>
      {/each}
    </ul>
    <div class="checkout">
      <span>Итого: <b class="num">{rub(total)}</b></span>
      <button disabled={lines.length === 0 || placing} onclick={placeOrder}>{placing ? 'Отправляю…' : 'Заказать'}</button>
    </div>
    {#if orderError && !order}
      <div class="hint error">{orderError}</div>
    {/if}
  </section>

  <section>
    <h2>Ваш заказ</h2>
    {#if order}
      <p>
        № <span class="num">{order.id.slice(0, 8)}</span> на <span class="num">{rub(order.total_cents)}</span>
        {#if traceId}
          · <a href={traceUrl(traceId)} target="_blank" rel="noopener">трейс заказа ↗</a>
        {/if}
      </p>
      <ol class="timeline">
        {#each STEPS as step, i}
          {@const reached = STEPS.indexOf(order.status) >= i}
          {@const current = order.status === step}
          <li class:reached class:current>
            <span class="dot"></span>
            <span>{stepLabel[step]}</span>
            <span class="num at">{history.find((h) => h.status === step)?.at ?? ''}</span>
          </li>
        {/each}
      </ol>
      {#if stuckHint[order.status]}
        <div class="hint">{stuckHint[order.status]}</div>
      {/if}
      {#if orderError}
        <div class="hint error">{orderError}</div>
      {/if}
    {:else}
      <p style="color:var(--muted)">Соберите заказ слева. Затем сходите на схему и что-нибудь сломайте.</p>
    {/if}
  </section>
</main>

<style>
  .shop { display: grid; grid-template-columns: 1fr 1fr; gap: 32px; padding: 20px; max-width: 1000px; margin: 0 auto; }
  @media (max-width: 720px) { .shop { grid-template-columns: 1fr; } }
  h2 { font-size: 14px; text-transform: uppercase; letter-spacing: 0.08em; border-bottom: 1px solid var(--ink); padding-bottom: 6px; }
  h2 small { font-weight: normal; text-transform: none; letter-spacing: 0; color: var(--muted); margin-left: 8px; }
  .menu { list-style: none; padding: 0; margin: 0; }
  .menu li { display: grid; grid-template-columns: 1fr auto auto; gap: 12px; align-items: center; padding: 8px 0; border-bottom: 1px solid var(--line); }
  .qty { display: inline-flex; align-items: center; gap: 8px; }
  .qty button { width: 28px; padding: 2px 0; }
  .checkout { display: flex; justify-content: space-between; align-items: center; margin-top: 16px; }
  .hint { font-size: 13px; padding: 8px 10px; background: #f4f4f4; margin: 10px 0; }
  .hint.error { background: var(--red); color: var(--paper); }
  .hint.error button { color: var(--paper); border-color: var(--paper); background: transparent; margin-left: 8px; }
  .timeline { list-style: none; padding: 0; margin: 0; }
  .timeline li { display: grid; grid-template-columns: 16px 1fr auto; gap: 10px; align-items: center; padding: 10px 0; color: var(--muted); }
  .timeline li.reached { color: var(--ink); }
  .dot { width: 10px; height: 10px; border-radius: 50%; border: 1.5px solid currentColor; }
  .reached .dot { background: var(--ink); }
  .current .dot { box-shadow: 0 0 0 4px var(--yellow); }
  .at { font-size: 12px; }
</style>
