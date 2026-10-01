import { describe, expect, it } from 'vitest';
import { MachineModel, MeterModel, loadAssumptions } from '../src/index.ts';

const a = loadAssumptions();
const T0 = Date.UTC(2025, 9, 1);
const HOUR = 3_600_000;
const YEAR = 365 * 24 * HOUR;

describe('MachineModel', () => {
  const model = new MachineModel('LS01', a, T0);
  model.segmentAt(T0 + YEAR);
  const segs = model.segments;

  it('follows the job cycle and leaves no holes between segments', () => {
    const next: Record<string, string[]> = {
      off: ['heat_up'],
      idle: ['heat_up'],
      heat_up: ['building'],
      building: ['cool_down'],
      cool_down: ['unpacking'],
      unpacking: ['idle', 'off'],
    };
    for (let i = 1; i < segs.length; i++) {
      expect(segs[i]!.startMs).toBe(segs[i - 1]!.endMs);
      expect(next[segs[i - 1]!.state]).toContain(segs[i]!.state);
    }
  });

  it('sets job_id only while building, unique per job', () => {
    const jobIds = segs.filter((s) => s.jobId !== null).map((s) => s.jobId);
    expect(segs.filter((s) => s.state !== 'building').every((s) => s.jobId === null)).toBe(true);
    expect(segs.filter((s) => s.state === 'building').every((s) => s.jobId?.startsWith('LS01-'))).toBe(true);
    expect(new Set(jobIds).size).toBe(jobIds.length);
  });

  it('gives about 3 builds per week and some heat-up overruns over a year', () => {
    const builds = segs.filter((s) => s.state === 'building').length;
    expect(builds / 52).toBeGreaterThan(2);
    expect(builds / 52).toBeLessThan(4.5);
    const overruns = segs.filter((s) => s.heatupOverrun);
    expect(overruns.length).toBeGreaterThan(0);
    for (const s of overruns) expect(s.endMs - s.startMs).toBeGreaterThan(a.states.heat_up.duration_h[0] * 1.4 * HOUR - 1);
  });

  it('is reproducible and independent per 3D printer', () => {
    const again = new MachineModel('LS01', a, T0);
    again.segmentAt(T0 + 30 * 24 * HOUR);
    expect(again.segments.slice(0, 20)).toEqual(segs.slice(0, 20));
    const other = new MachineModel('LS02', a, T0);
    expect(other.segments[0]!.startMs).not.toBe(segs[0]!.startMs);
  });

  it('integrates energy consistently with mean power', () => {
    const b = segs.find((s) => s.state === 'building')!;
    const kwh = model.energyKwhBetween(b.startMs, b.endMs);
    const meanKw = b.basePowerKw + b.exposureExtraKw * b.exposureShare;
    expect(kwh).toBeCloseTo((meanKw * (b.endMs - b.startMs)) / HOUR, 6);
    // additive across a split point
    const mid = (b.startMs + b.endMs) / 2;
    expect(model.energyKwhBetween(b.startMs, mid) + model.energyKwhBetween(mid, b.endMs)).toBeCloseTo(kwh, 6);
  });

  it('compresses durations for live demos', () => {
    const fast = new MachineModel('LS01', a, T0, { durationScale: 1 / 30 });
    fast.segmentAt(T0 + 24 * HOUR);
    const h = fast.segments.find((s) => s.state === 'heat_up')!;
    expect(h.endMs - h.startMs).toBeLessThan((a.states.heat_up.duration_h[1] * 2.5 * HOUR) / 30 + 1);
  });
});

describe('MeterModel', () => {
  it('keeps the counter monotonic, keeps counting through dropouts, and leaves gaps', () => {
    const machine = new MachineModel('LS03', a, T0);
    const meter = new MeterModel('EM03', a, T0);
    let last = -1;
    let gaps = 0;
    let prevT = T0;
    for (let t = T0; t < T0 + 60 * 24 * HOUR; t += 60_000) {
      meter.advance(t, machine.energyKwhBetween(prevT, t));
      prevT = t;
      const r = meter.read(t, machine.powerAt(t));
      if (!r) {
        gaps++;
        continue;
      }
      expect(r.energy_kwh_total).toBeGreaterThanOrEqual(last);
      last = r.energy_kwh_total;
    }
    expect(gaps).toBeGreaterThan(0);
  });

  it('restarts the counter near zero when the meter is replaced', () => {
    const meter = new MeterModel('EM07', a, T0);
    meter.advance(T0 + HOUR, 3);
    const before = meter.read(T0 + HOUR, 2)?.energy_kwh_total ?? meter.read(T0 + HOUR + 1, 2)!.energy_kwh_total;
    meter.replace();
    const after = meter.read(T0 + HOUR + 2, 2)!.energy_kwh_total;
    expect(before).toBeGreaterThan(1000);
    expect(after).toBeLessThan(1);
  });
});
