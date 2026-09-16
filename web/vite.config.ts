import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

// In dev the whole stack sits behind Traefik on :8080; the SPA is the only
// thing served by Vite, so every API and Grafana call is proxied there.
export default defineConfig({
  plugins: [svelte()],
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
      '/grafana': 'http://localhost:8080',
    },
  },
  build: { outDir: 'dist', emptyOutDir: true },
});
