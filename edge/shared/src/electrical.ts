import type { Rng } from './rng.ts';

export interface PhaseReading {
  l1_a: number;
  l2_a: number;
  l3_a: number;
  voltage_v: number;
  power_factor: number;
}

/**
 * Derives three-phase readings for a balanced-ish load: I = P / (sqrt(3) * U * pf), then spreads the mean current
 * over the phases with a small imbalance that keeps the mean unchanged.
 */
export function phaseReading(
  powerKw: number,
  opts: { voltageNominal: number; voltageJitterPct: number; powerFactor: number; imbalancePct: number },
  rng: Rng,
): PhaseReading {
  const voltage = opts.voltageNominal * (1 + ((rng.next() * 2 - 1) * opts.voltageJitterPct) / 100);
  const meanCurrent = (powerKw * 1000) / (Math.sqrt(3) * voltage * opts.powerFactor);
  const d1 = ((rng.next() * 2 - 1) * opts.imbalancePct) / 100;
  const d2 = ((rng.next() * 2 - 1) * opts.imbalancePct) / 100;
  return {
    l1_a: round(meanCurrent * (1 + d1), 3),
    l2_a: round(meanCurrent * (1 + d2), 3),
    l3_a: round(meanCurrent * (1 - d1 - d2), 3),
    voltage_v: round(voltage, 1),
    power_factor: round(opts.powerFactor, 3),
  };
}

export function round(value: number, digits: number): number {
  const f = 10 ** digits;
  return Math.round(value * f) / f;
}
