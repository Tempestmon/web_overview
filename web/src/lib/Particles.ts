import type { Snapshot, Topology } from './api';

interface Particle {
  t: number;
  speed: number;
  error: boolean;
}

interface EdgeRun {
  from: { x: number; y: number };
  to: { x: number; y: number };
  particles: Particle[];
  spawnDebt: number;
  rps: number;
  errorShare: number;
}

const MAX_SPAWN_RPS = 40;
const BASE_SPEED = 0.35; // fraction of edge per second
const NODE_HALF_W = 78;
const NODE_HALF_H = 36;

/**
 * Draws traffic as dots running along edges on a canvas that shares the
 * scheme's coordinate system. Spawn rate follows real RPS, so a stopped
 * component visibly starves its downstream edges within seconds.
 */
export class ParticleField {
  private runs = new Map<string, EdgeRun>();
  private raf = 0;
  private last = 0;

  constructor(private canvas: HTMLCanvasElement, private viewW: number, private viewH: number) {}

  setTopology(topology: Topology) {
    const pos = new Map(topology.nodes.map((n) => [n.id, n.pos]));
    this.runs.clear();
    for (const e of topology.edges) {
      const a = pos.get(e.from);
      const b = pos.get(e.to);
      if (!a || !b) continue;
      this.runs.set(`${e.from}->${e.to}`, {
        from: clipToNode(a, b),
        to: clipToNode(b, a),
        particles: [],
        spawnDebt: 0,
        rps: 0,
        errorShare: 0,
      });
    }
  }

  update(snapshot: Snapshot) {
    for (const e of snapshot.edges) {
      const run = this.runs.get(`${e.from}->${e.to}`);
      if (!run) continue;
      run.rps = e.rps;
      run.errorShare = e.rps > 0 ? e.error_rps / e.rps : 0;
    }
  }

  start() {
    this.last = performance.now();
    const frame = (now: number) => {
      const dt = Math.min((now - this.last) / 1000, 0.1);
      this.last = now;
      this.step(dt);
      this.draw();
      this.raf = requestAnimationFrame(frame);
    };
    this.raf = requestAnimationFrame(frame);
  }

  stop() {
    cancelAnimationFrame(this.raf);
  }

  private step(dt: number) {
    for (const run of this.runs.values()) {
      run.spawnDebt += Math.min(run.rps, MAX_SPAWN_RPS) * dt;
      while (run.spawnDebt >= 1) {
        run.spawnDebt -= 1;
        run.particles.push({ t: 0, speed: BASE_SPEED * (0.8 + Math.random() * 0.4), error: Math.random() < run.errorShare });
      }
      for (const p of run.particles) p.t += p.speed * dt;
      run.particles = run.particles.filter((p) => p.t < 1);
    }
  }

  private draw() {
    const ctx = this.canvas.getContext('2d');
    if (!ctx) return;
    const { width, height } = this.canvas;
    ctx.clearRect(0, 0, width, height);
    const sx = width / this.viewW;
    const sy = height / this.viewH;
    for (const run of this.runs.values()) {
      for (const p of run.particles) {
        const x = (run.from.x + (run.to.x - run.from.x) * p.t) * sx;
        const y = (run.from.y + (run.to.y - run.from.y) * p.t) * sy;
        ctx.beginPath();
        ctx.arc(x, y, p.error ? 3.5 * sx : 2.5 * sx, 0, Math.PI * 2);
        ctx.fillStyle = p.error ? '#e0261c' : '#111';
        ctx.fill();
      }
    }
  }
}

/** Moves the endpoint from the node centre to its card border. */
function clipToNode(center: { x: number; y: number }, toward: { x: number; y: number }) {
  const dx = toward.x - center.x;
  const dy = toward.y - center.y;
  if (dx === 0 && dy === 0) return { ...center };
  const k = Math.min(NODE_HALF_W / Math.abs(dx || 1e-9), NODE_HALF_H / Math.abs(dy || 1e-9));
  return { x: center.x + dx * k, y: center.y + dy * k };
}
