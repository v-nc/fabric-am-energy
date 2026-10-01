import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parquetReadObjects } from 'hyparquet';
import { afterAll, describe, expect, it } from 'vitest';
import { loadAssumptions, type Assumptions } from '@am-energy/shared';
import { generateHistory, monthStarts, type GroundTruth, type HistoryRow } from '../src/generate.ts';
import { writeMonthParquet } from '../src/parquet.ts';

const dir = mkdtempSync(join(tmpdir(), 'am-energy-history-'));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

/** Three months, two 3D printers, frequent faults: small enough for a unit test, every fault shows up. */
function smallAssumptions(): Assumptions {
  const a = structuredClone(loadAssumptions());
  a.park.machines = a.park.machines.filter((m) => m.machine_id === 'LS06' || m.machine_id === 'LS07');
  a.volumes.history_months = 3;
  a.faults.schema_change.v2_from_month = 2;
  a.faults.counter_reset = [{ meter_id: 'EM07', at_month: 1 }];
  a.faults.gateway_outage.per_month = 4;
  a.faults.duplicates.probability_per_event = 0.01;
  return a;
}

const END = Date.UTC(2026, 9, 1);
const byMonth = new Map<string, HistoryRow[]>();
let truth: GroundTruth;
const run = generateHistory({ assumptions: smallAssumptions(), endMs: END, onMonth: (m, rows) => void byMonth.set(m, rows) }).then(
  (t) => (truth = t),
);

describe('monthStarts', () => {
  it('covers calendar months up to the end date', () => {
    expect(monthStarts(END, 3).map((ms) => new Date(ms).toISOString().slice(0, 7))).toEqual(['2026-07', '2026-08', '2026-09']);
  });
});

describe('generateHistory', () => {
  it('produces one batch per month, in arrival order', async () => {
    await run;
    expect([...byMonth.keys()]).toEqual(['2026-07', '2026-08', '2026-09']);
    for (const rows of byMonth.values()) {
      expect(rows.every((r, i) => i === 0 || r.ts_edge >= rows[i - 1]!.ts_edge)).toBe(true);
    }
  });

  it('switches to schema version 2 with power_factor from the configured month', async () => {
    await run;
    expect(byMonth.get('2026-08')!.every((r) => r.schema_version === 1 && r.power_factor === null)).toBe(true);
    expect(byMonth.get('2026-09')!.every((r) => r.schema_version === 2 && r.power_factor !== null)).toBe(true);
  });

  it('injects duplicates (same meter and seq), late out-of-order arrivals and dropout gaps', async () => {
    await run;
    const all = [...byMonth.values()].flat();
    const keys = all.map((r) => `${r.meter_id}/${r.seq}`);
    expect(keys.length - new Set(keys).size).toBe(truth.duplicates);
    expect(truth.duplicates).toBeGreaterThan(0);
    expect(truth.late_events).toBeGreaterThan(0);
    expect(all.some((r) => r.ts_edge - r.ts_source > 10 * 60_000)).toBe(true);
    const em06 = all.filter((r) => r.meter_id === 'EM06').sort((x, y) => x.seq - y.seq);
    expect(em06.some((r, i) => i > 0 && r.ts_source - em06[i - 1]!.ts_source > 5 * 60_000)).toBe(true);
  });

  it('resets the EM07 counter at the configured month and keeps it monotonic otherwise', async () => {
    await run;
    const em07 = [...byMonth.values()].flat().filter((r) => r.meter_id === 'EM07').sort((x, y) => x.ts_source - y.ts_source);
    const drops = em07.filter((r, i) => i > 0 && r.energy_kwh_total < em07[i - 1]!.energy_kwh_total);
    expect(drops).toHaveLength(1);
    expect(new Date(drops[0]!.ts_source).toISOString().slice(0, 7)).toBe('2026-08');
    expect(truth.counter_resets).toEqual([{ meter_id: 'EM07', at: '2026-08-01T00:00:00.000Z' }]);
  });

  it('records heat-ups with their overrun flag', async () => {
    await run;
    expect(truth.heatups.length).toBeGreaterThan(10);
    expect(truth.heatups.every((h) => Date.parse(h.end) > Date.parse(h.start))).toBe(true);
  });
});

describe('writeMonthParquet', () => {
  it('round-trips rows, with power_factor only in version 2 files', async () => {
    await run;
    for (const month of ['2026-08', '2026-09']) {
      const file = join(dir, `${month}.parquet`);
      const rows = byMonth.get(month)!.slice(0, 500);
      writeMonthParquet(file, rows);
      const buf = readFileSync(file);
      const back = await parquetReadObjects({ file: buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength) });
      expect(back).toHaveLength(500);
      expect(back[0]!.meter_id).toBe(rows[0]!.meter_id);
      expect(Number(back[0]!.seq)).toBe(rows[0]!.seq);
      expect(new Date(back[0]!.ts_source as Date).getTime()).toBe(rows[0]!.ts_source);
      expect('power_factor' in back[0]!).toBe(month === '2026-09');
    }
  });
});
