<script lang="ts">
  import Panel from './routes/Panel.svelte';
  import Shop from './routes/Shop.svelte';

  // Two pages, no router library: the controller serves index.html for any
  // path, so a plain pathname check is all the routing we need.
  let path = $state(window.location.pathname);
  function navigate(to: string) {
    history.pushState({}, '', to);
    path = to;
  }
  window.addEventListener('popstate', () => (path = window.location.pathname));
</script>

{#if path.startsWith('/order')}
  <Shop {navigate} />
{:else}
  <Panel {navigate} />
{/if}
