import type { Assumptions } from './assumptions.ts';
import { phaseReading, round } from './electrical.ts';
import { Rng } from './rng.ts';
import type { OpcStatus } from './schema.ts';

const MINUTE_MS = 60_000;
const MONTH_MS = 30 * 24 * 60 * MINUTE_MS;

export interface MeterReading {
  tsMs: number;
  l1_a: number;
  l2_a: number;
  l3_a: number;
  voltage_v: number;
  power_kw: number;
  power_factor: number;
  energy_kwh_total: number;
  opc_status: OpcStatus;
}

/**
 * IP energy meter on one 3D printer. Its kWh counter integrates the true energy, including during dropouts, so a
 * reading after a gap jumps by the energy drawn while it was silent. Faults: dropouts, spikes, Uncertain/Bad status,
 * and a counter reset when the meter is replaced.
 */
export class MeterModel {
  private energyKwh: number;
  private lastMs: number;
  private readonly rng: Rng;
  private readonly powerFactor: number;
  private nextDropoutStart: number;
  private dropoutEnd = -Infinity;

  constructor(
    readonly meterId: string,
    private readonly a: Assumptions,
    startMs: number,
  ) {
    this.rng = Rng.derive(a.seed, `${meterId}/meter`);
    this.energyKwh = this.rng.uniform([5_000, 40_000]);
    this.powerFactor = this.rng.uniform(a.electrical.power_factor);
    this.lastMs = startMs;
    this.nextDropoutStart = startMs + this.dropoutGap();
  }

  /** Adds the energy drawn since the last call. t must not go backwards. */
  advance(tMs: number, energyKwh: number): void {
    if (tMs < this.lastMs) throw new Error(`${this.meterId}: time went backwards`);
    this.energyKwh += energyKwh;
    this.lastMs = tMs;
  }

  /** The reading at t, or null while the meter is unreachable (a dropout). */
  read(tMs: number, truePowerKw: number): MeterReading | null {
    if (this.inDropout(tMs)) return null;
    const f = this.a.faults;
    const status: OpcStatus = this.rng.chance(f.opc_status.bad_probability)
      ? 'Bad'
      : this.rng.chance(f.opc_status.uncertain_probability)
        ? 'Uncertain'
        : 'Good';
    const spike = this.rng.chance(f.spikes.probability_per_event) ? this.rng.uniform(f.spikes.factor) : 1;
    const powerKw = status === 'Bad' ? 0 : truePowerKw * spike;
    const pf = round(this.powerFactor + (this.rng.next() * 2 - 1) * 0.01, 3);
    const phases = phaseReading(
      powerKw,
      {
        voltageNominal: this.a.electrical.voltage_v_nominal,
        voltageJitterPct: this.a.electrical.voltage_jitter_pct,
        powerFactor: pf,
        imbalancePct: this.a.electrical.phase_imbalance_pct,
      },
      this.rng,
    );
    return {
      tsMs: tMs,
      ...phases,
      power_kw: round(powerKw, 3),
      energy_kwh_total: round(this.energyKwh, 3),
      opc_status: status,
    };
  }

  /** Meter replaced: the new meter's counter starts near zero. */
  replace(): void {
    this.energyKwh = this.rng.uniform([0, 0.5]);
  }

  private inDropout(tMs: number): boolean {
    while (tMs >= this.nextDropoutStart) {
      this.dropoutEnd = this.nextDropoutStart + this.rng.uniform(this.a.faults.meter_dropout.duration_min) * MINUTE_MS;
      this.nextDropoutStart = this.dropoutEnd + this.dropoutGap();
    }
    return tMs < this.dropoutEnd;
  }

  private dropoutGap(): number {
    const perMonth = this.a.faults.meter_dropout.per_meter_per_month;
    return perMonth > 0 ? this.rng.exponential(perMonth / MONTH_MS) : Infinity;
  }
}
