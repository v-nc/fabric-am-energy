import { describe, expect, it } from 'vitest';
import { Assumptions, MeterEvent, Rng, loadAssumptions, phaseReading } from '../src/index.ts';

const v1 = {
  schema_version: 1,
  machine_id: 'LS01',
  meter_id: 'EM01',
  seq: 42,
  ts_source: '2026-10-01T12:00:00.000Z',
  ts_edge: '2026-10-01T12:00:01.250Z',
  state: 'building',
  job_id: 'LS01-000017',
  l1_a: 7.1,
  l2_a: 7.3,
  l3_a: 6.9,
  voltage_v: 401.2,
  power_kw: 4.7,
  energy_kwh_total: 18234.512,
  opc_status: 'Good',
};

describe('MeterEvent schema', () => {
  it('accepts version 1 and version 2 events', () => {
    expect(MeterEvent.parse(v1).schema_version).toBe(1);
    expect(MeterEvent.parse({ ...v1, schema_version: 2, power_factor: 0.94 }).schema_version).toBe(2);
  });

  it('rejects power_factor on version 1 and requires it on version 2', () => {
    expect(MeterEvent.safeParse({ ...v1, power_factor: 0.94 }).success).toBe(false);
    expect(MeterEvent.safeParse({ ...v1, schema_version: 2 }).success).toBe(false);
  });

  it('rejects missing fields, unknown states and non-UTC timestamps', () => {
    const { seq: _, ...noSeq } = v1;
    expect(MeterEvent.safeParse(noSeq).success).toBe(false);
    expect(MeterEvent.safeParse({ ...v1, state: 'printing' }).success).toBe(false);
    expect(MeterEvent.safeParse({ ...v1, ts_source: '2026-10-01T14:00:00+02:00' }).success).toBe(false);
  });
});

describe('assumptions file', () => {
  const a = loadAssumptions();

  it('loads the repo config with 10 3D printers in 2 halls', () => {
    expect(a.park.machines).toHaveLength(10);
    expect(new Set(a.park.machines.map((m) => m.hall_id))).toEqual(new Set(['H1', 'H2']));
  });

  it('rejects inverted ranges', () => {
    const broken = structuredClone(a);
    broken.states.heat_up.duration_h = [2.5, 1.5];
    expect(Assumptions.safeParse(broken).success).toBe(false);
  });
});

describe('Rng', () => {
  it('is reproducible per seed and label', () => {
    const draw = (label: string) => {
      const r = Rng.derive(20190401, label);
      return [r.next(), r.next(), r.next()];
    };
    expect(draw('LS01')).toEqual(draw('LS01'));
    expect(draw('LS01')).not.toEqual(draw('LS02'));
  });

  it('stays inside ranges', () => {
    const r = new Rng(1);
    for (let i = 0; i < 10_000; i++) {
      const x = r.uniform([1.5, 2.5]);
      expect(x).toBeGreaterThanOrEqual(1.5);
      expect(x).toBeLessThan(2.5);
    }
  });
});

describe('phaseReading', () => {
  it('reproduces the active power from the phase currents', () => {
    const opts = { voltageNominal: 400, voltageJitterPct: 1.5, powerFactor: 0.93, imbalancePct: 4 };
    const r = phaseReading(7.5, opts, new Rng(7));
    const meanCurrent = (r.l1_a + r.l2_a + r.l3_a) / 3;
    const kw = (Math.sqrt(3) * r.voltage_v * meanCurrent * r.power_factor) / 1000;
    expect(kw).toBeCloseTo(7.5, 1);
  });
});
