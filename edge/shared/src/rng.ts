/**
 * Seeded random-number generator (mulberry32). Each machine and each concern gets its own stream, derived from the
 * global seed and a label, so adding a machine or a fault type doesn't shift the draws of the others.
 */
export class Rng {
  private state: number;

  constructor(seed: number) {
    this.state = seed >>> 0;
  }

  /** Independent stream for a label such as "LS03/faults". */
  static derive(seed: number, label: string): Rng {
    return new Rng(hash32(`${seed}:${label}`));
  }

  /** Uniform in [0, 1). */
  next(): number {
    this.state = (this.state + 0x6d2b79f5) >>> 0;
    let t = this.state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  uniform([min, max]: readonly [number, number]): number {
    return min + (max - min) * this.next();
  }

  int(min: number, maxInclusive: number): number {
    return min + Math.floor(this.next() * (maxInclusive - min + 1));
  }

  chance(probability: number): boolean {
    return this.next() < probability;
  }

  pick<T>(items: readonly T[]): T {
    if (items.length === 0) throw new Error('pick from empty array');
    return items[Math.floor(this.next() * items.length)] as T;
  }

  /** Exponential inter-arrival time for a Poisson process with the given rate (events per unit). */
  exponential(rate: number): number {
    return -Math.log(1 - this.next()) / rate;
  }
}

/** FNV-1a, 32 bit. Only used to turn labels into seeds. */
function hash32(text: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}
