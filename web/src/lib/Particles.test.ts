import { describe, expect, it } from 'vitest';
import { spawnPerSecond, speedFor } from './Particles';

describe('particle flow', () => {
  it('spawns in proportion to RPS up to the cap', () => {
    expect(spawnPerSecond(0)).toBe(0);
    expect(spawnPerSecond(12)).toBe(12);
    expect(spawnPerSecond(500)).toBe(40);
    expect(spawnPerSecond(-0)).toBe(0);
  });

  it('moves faster on a busier edge, within the ceiling', () => {
    expect(speedFor(20)).toBeGreaterThan(speedFor(1));
    expect(speedFor(500)).toBe(speedFor(40));
    expect(speedFor(40) / speedFor(0)).toBeCloseTo(1.5);
  });
});
