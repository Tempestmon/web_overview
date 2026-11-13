<script lang="ts">
  import type { Scenario } from './scenarios';

  interface Props {
    scenarios: Scenario[];
    active: Scenario | null;
    done: number;
    busy: boolean;
    onStart: (scenario: Scenario) => void;
    onClose: () => void;
  }
  let { scenarios, active, done, busy, onStart, onClose }: Props = $props();

  const finished = $derived(active !== null && done >= active.steps.length);
</script>

{#if active}
  <section class="tour running">
    <header>
      <h2>{active.question}</h2>
      <button onclick={onClose}>{finished ? 'Закрыть' : 'Прервать тур'}</button>
    </header>
    <ol>
      {#each active.steps as step, i}
        <li class:done={i < done} class:current={i === done}>
          <span class="mark">{i < done ? '✓' : i === done ? '…' : ''}</span>
          <div>
            <b>{step.text}</b>
            {#if i <= done}<p>{step.why}</p>{/if}
          </div>
        </li>
      {/each}
    </ol>
    {#if finished}
      <p class="lesson"><b>Вывод.</b> {active.lesson}</p>
    {:else}
      <p class="muted">Шаги отмечаются сами, когда эффект виден на схеме. Сбой откатится автоматически.</p>
    {/if}
  </section>
{:else if scenarios.length > 0}
  <section class="tour">
    <h2>Что будет, если…</h2>
    <div class="chips">
      {#each scenarios as scenario (scenario.id)}
        <button disabled={busy} onclick={() => onStart(scenario)}>{scenario.question.replace('Что будет, если ', '…')}</button>
      {/each}
    </div>
  </section>
{/if}

<style>
  .tour { padding: 12px 20px; border-bottom: 1px solid var(--line); }
  .tour.running { border-bottom-color: var(--ink); }
  header { display: flex; justify-content: space-between; align-items: center; gap: 12px; }
  h2 { font-size: 14px; margin: 0 0 8px; text-transform: uppercase; letter-spacing: 0.08em; }
  .chips { display: flex; flex-wrap: wrap; gap: 8px; }
  .chips button { font-size: 13px; text-align: left; }
  ol { list-style: none; padding: 0; margin: 4px 0 8px; display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 8px 20px; }
  li { display: grid; grid-template-columns: 18px 1fr; gap: 6px; color: var(--muted); font-size: 13px; }
  li.done, li.current { color: var(--ink); }
  li.current .mark { background: var(--yellow); }
  .mark { width: 18px; height: 18px; line-height: 18px; text-align: center; border: 1.5px solid currentColor; border-radius: 50%; font-size: 11px; }
  li.done .mark { background: var(--ink); color: var(--paper); border-color: var(--ink); }
  li p { margin: 2px 0 0; font-size: 12px; color: var(--muted); }
  .lesson { margin: 0; padding: 8px 10px; background: #f4f4f4; font-size: 14px; }
  .muted { margin: 0; font-size: 12px; color: var(--muted); }
</style>
