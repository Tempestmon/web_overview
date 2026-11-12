<script lang="ts">
  import type { Snapshot } from './api';

  interface Props {
    load: Snapshot['load'];
    onSet: (factor: number) => void;
  }
  let { load, onSet }: Props = $props();

  const PRESETS = [1, 3, 10];
  const factor = $derived(load ? Math.round(load.users / load.base_users) : 1);
  const presets = $derived(PRESETS.filter((p) => p <= (load?.max_factor ?? 1)));
</script>

<div class="stat">
  <span>нагрузка{#if load && load.remaining_s > 0}, ещё <span class="num">{Math.ceil(load.remaining_s)} с</span>{/if}</span>
  <div class="presets" title={load ? `${load.users} виртуальных покупателей` : 'генератор нагрузки недоступен'}>
    {#each presets as preset}
      <button class:active={preset === factor} disabled={!load} onclick={() => onSet(preset)}>×{preset}</button>
    {/each}
  </div>
</div>

<style>
  .presets { display: flex; gap: 4px; margin-top: 2px; }
  button { padding: 1px 8px; font-size: 13px; }
  button.active { background: var(--ink); color: var(--paper); }
</style>
