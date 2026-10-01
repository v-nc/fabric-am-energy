import type { Assumptions } from './assumptions.ts';
import { Rng } from './rng.ts';
import type { MachineState } from './schema.ts';

const HOUR_MS = 3_600_000;

export interface Segment {
  state: MachineState;
  startMs: number;
  endMs: number;
  /** Set only for building segments. */
  jobId: string | null;
  basePowerKw: number;
  /** Building only: extra load while a layer is exposed, and the layer rhythm. */
  exposureExtraKw: number;
  layerCycleS: number;
  exposureShare: number;
  /** Heat-up only: ground truth for the over-long heat-up fault. */
  heatupOverrun: boolean;
}

export interface MachineModelOptions {
  /** Multiplies every state duration. 1 is real time; 1/30 turns a 2 h heat-up into 4 min for a live demo. */
  durationScale?: number;
}

/**
 * Deterministic schedule of one 3D printer: gap (idle or off) → heat-up → building → cool-down → unpacking → gap …
 * Segments are generated lazily as time advances, from a random-number stream that belongs to this 3D printer only.
 */
export class MachineModel {
  readonly segments: Segment[] = [];
  private readonly schedule: Rng;
  private readonly noise: Rng;
  private readonly scale: number;
  private cursor = 0;

  constructor(
    readonly machineId: string,
    private readonly a: Assumptions,
    startMs: number,
    opts: MachineModelOptions = {},
  ) {
    this.scale = opts.durationScale ?? 1;
    this.schedule = Rng.derive(a.seed, `${machineId}/schedule`);
    this.noise = Rng.derive(a.seed, `${machineId}/noise`);
    // Start the first cycle up to 60 h before startMs so the 3D printers are not in step at t0.
    this.appendGap(startMs - this.schedule.uniform([0, 60]) * HOUR_MS * this.scale);
    this.extendTo(startMs);
  }

  /** The segment that contains t. Calls are fastest when t only moves forward. */
  segmentAt(t: number): Segment {
    const first = this.segments[0];
    if (!first || t < first.startMs) throw new Error(`${this.machineId}: ${new Date(t).toISOString()} is before the model start`);
    this.extendTo(t);
    let i = Math.min(this.cursor, this.segments.length - 1);
    while (this.segments[i]!.startMs > t) i--;
    while (this.segments[i]!.endMs <= t) i++;
    this.cursor = i;
    return this.segments[i]!;
  }

  /** Instantaneous active power at t, with layer exposure peaks while building and ±3 % noise. */
  powerAt(t: number): number {
    const s = this.segmentAt(t);
    let p = s.basePowerKw;
    if (s.state === 'building') {
      const phase = (((t - s.startMs) / 1000) % s.layerCycleS) / s.layerCycleS;
      if (phase < s.exposureShare) p += s.exposureExtraKw;
    }
    return p * (1 + (this.noise.next() * 2 - 1) * 0.03);
  }

  /** Exact energy drawn between t0 and t1 (kWh), using each segment's mean power. This is what the meter counts. */
  energyKwhBetween(t0: number, t1: number): number {
    if (t1 <= t0) return 0;
    this.extendTo(t1);
    let kwh = 0;
    for (let i = this.indexAt(t0); i < this.segments.length; i++) {
      const s = this.segments[i]!;
      if (s.startMs >= t1) break;
      const overlapH = (Math.min(s.endMs, t1) - Math.max(s.startMs, t0)) / HOUR_MS;
      kwh += meanPowerKw(s) * overlapH;
    }
    return kwh;
  }

  private indexAt(t: number): number {
    let lo = 0;
    let hi = this.segments.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (this.segments[mid]!.startMs <= t) lo = mid;
      else hi = mid - 1;
    }
    return lo;
  }

  private extendTo(t: number): void {
    while (this.segments[this.segments.length - 1]!.endMs <= t) this.appendJobCycle();
  }

  private appendJobCycle(): void {
    const st = this.a.states;
    const r = this.schedule;
    const overrun = r.chance(this.a.faults.heatup_overrun.probability_per_build);
    const heatupH = r.uniform(st.heat_up.duration_h) * (overrun ? r.uniform(this.a.faults.heatup_overrun.duration_factor) : 1);
    const heatup = this.push('heat_up', heatupH, r.uniform(st.heat_up.power_kw));
    heatup.heatupOverrun = overrun;

    const buildStart = this.end();
    const build = this.push('building', r.uniform(st.building.duration_h), r.uniform(st.building.power_kw));
    build.jobId = `${this.machineId}-${compactUtc(buildStart)}`;
    build.exposureExtraKw = r.uniform(st.building.exposure_extra_kw);
    build.layerCycleS = r.uniform(st.building.layer_cycle_s);
    build.exposureShare = r.uniform(st.building.exposure_share);

    this.push('cool_down', r.uniform(st.cool_down.duration_h), r.uniform(st.cool_down.power_kw));
    this.push('unpacking', r.uniform(st.unpacking.duration_h), r.uniform(st.unpacking.power_kw));
    this.appendGap(this.end());
  }

  private appendGap(startMs: number): void {
    const r = this.schedule;
    const state: MachineState = r.chance(this.a.schedule.off_share_of_gaps) ? 'off' : 'idle';
    const hours = r.uniform(this.a.schedule.idle_between_jobs_h);
    this.segments.push(segment(state, startMs, startMs + hours * HOUR_MS * this.scale, r.uniform(this.a.states[state].power_kw)));
  }

  private push(state: MachineState, hours: number, powerKw: number): Segment {
    const start = this.end();
    const s = segment(state, start, start + hours * HOUR_MS * this.scale, powerKw);
    this.segments.push(s);
    return s;
  }

  private end(): number {
    return this.segments[this.segments.length - 1]!.endMs;
  }
}

function segment(state: MachineState, startMs: number, endMs: number, basePowerKw: number): Segment {
  return { state, startMs, endMs, jobId: null, basePowerKw, exposureExtraKw: 0, layerCycleS: 1, exposureShare: 0, heatupOverrun: false };
}

function meanPowerKw(s: Segment): number {
  return s.basePowerKw + (s.state === 'building' ? s.exposureExtraKw * s.exposureShare : 0);
}

/** 2026-10-01T14:30:00Z → 20261001T1430 */
function compactUtc(ms: number): string {
  const iso = new Date(ms).toISOString();
  return `${iso.slice(0, 4)}${iso.slice(5, 7)}${iso.slice(8, 10)}T${iso.slice(11, 13)}${iso.slice(14, 16)}`;
}
