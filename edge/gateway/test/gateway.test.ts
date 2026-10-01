import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, describe, expect, it } from 'vitest';
import { MeterEvent, Rng, loadAssumptions } from '@am-energy/shared';
import { startSimServer } from '@am-energy/opcua-sim';
import {
  FaultySink,
  Forwarder,
  MemorySink,
  ReadingAssembler,
  StoreAndForwardBuffer,
  startGateway,
  toEvent,
  type PointUpdate,
  type Reading,
} from '../src/index.ts';

const a = loadAssumptions();
const dir = mkdtempSync(join(tmpdir(), 'am-energy-gw-'));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

const point = (field: PointUpdate['field'], value: unknown, ts: number, status: PointUpdate['status'] = 'Good'): PointUpdate => ({
  machineId: 'LS01',
  meterId: 'EM01',
  field,
  value,
  status,
  sourceTsMs: ts,
});
const fullReading = (ts: number, worst: PointUpdate['status'] = 'Good'): PointUpdate[] => [
  point('state', 'building', ts),
  point('job_id', 'LS01-20261001T1200', ts),
  point('l1_a', 7, ts),
  point('l2_a', 7, ts),
  point('l3_a', 7, ts),
  point('voltage_v', 400, ts),
  point('power_kw', 4.6, ts, worst),
  point('power_factor', 0.94, ts),
  point('energy_kwh_total', 12000.5, ts),
];

describe('ReadingAssembler', () => {
  it('emits one reading once all points of a timestamp are in, with the worst status', () => {
    const out: Reading[] = [];
    const asm = new ReadingAssembler((r) => out.push(r));
    const pts = fullReading(1000, 'Uncertain');
    pts.slice(0, 8).forEach((p) => asm.push(p));
    expect(out).toHaveLength(0);
    asm.push(pts[8]!);
    expect(out).toHaveLength(1);
    expect(out[0]).toMatchObject({ state: 'building', status: 'Uncertain', values: { power_kw: 4.6 } });
  });

  it('drops an incomplete reading when a newer one starts, and ignores stale points', () => {
    const out: Reading[] = [];
    const asm = new ReadingAssembler((r) => out.push(r));
    fullReading(1000).slice(0, 4).forEach((p) => asm.push(p));
    fullReading(2000).forEach((p) => asm.push(p));
    asm.push(point('power_kw', 1, 1500));
    expect(out.map((r) => r.tsMs)).toEqual([2000]);
    expect(asm.incompleteDropped).toBe(1);
    expect(asm.staleIgnored).toBe(1);
  });
});

describe('StoreAndForwardBuffer', () => {
  it('numbers events per meter and keeps the numbering across restarts', () => {
    const path = join(dir, 'seq.sqlite');
    const reading = (meterId: string): Reading => ({
      machineId: 'LS01', meterId, tsMs: 0, state: 'idle', jobId: null, status: 'Good',
      values: { l1_a: 1, l2_a: 1, l3_a: 1, voltage_v: 400, power_kw: 1, power_factor: 0.9, energy_kwh_total: 1 },
    });
    let b = new StoreAndForwardBuffer(path);
    expect(b.enqueue(toEvent(reading('EM01'), 1)).seq).toBe(0);
    expect(b.enqueue(toEvent(reading('EM01'), 1)).seq).toBe(1);
    expect(b.enqueue(toEvent(reading('EM02'), 1)).seq).toBe(0);
    b.close();
    b = new StoreAndForwardBuffer(path);
    expect(b.enqueue(toEvent(reading('EM01'), 1)).seq).toBe(2);
    expect(b.size()).toBe(4);
    b.close();
  });
});

describe('Forwarder across an outage', () => {
  it('delivers everything after the outage, live data first, so the backlog arrives late and out of order', async () => {
    const buffer = new StoreAndForwardBuffer(':memory:');
    const memory = new MemorySink();
    const sink = new FaultySink(memory, 0, new Rng(1));
    let now = Date.UTC(2026, 9, 1, 12);
    const fwd = new Forwarder(buffer, sink, { freshBatch: 5, backlogBatch: 20, intervalMs: 1000, now: () => now });
    const enqueue = () =>
      buffer.enqueue(
        toEvent(
          { machineId: 'LS01', meterId: 'EM01', tsMs: now, state: 'idle', jobId: null, status: 'Good',
            values: { l1_a: 2, l2_a: 2, l3_a: 2, voltage_v: 400, power_kw: 1.2, power_factor: 0.93, energy_kwh_total: 100 } },
          1,
        ),
      );

    for (let i = 0; i < 3; i++, now += 5000) { enqueue(); await fwd.flushOnce(); }
    sink.outage = true;
    for (let i = 0; i < 30; i++, now += 5000) { enqueue(); await fwd.flushOnce(); }
    expect(buffer.size()).toBe(30);
    sink.outage = false;
    for (let i = 0; i < 5; i++, now += 5000) { enqueue(); await fwd.flushOnce(); }
    while (buffer.size() > 0) await fwd.flushOnce();

    const seqs = memory.received.map((e) => e.seq);
    expect([...seqs].sort((x, y) => x - y)).toEqual(Array.from({ length: 38 }, (_, i) => i));
    const outOfOrder = seqs.some((s, i) => i > 0 && s < seqs[i - 1]!);
    expect(outOfOrder).toBe(true);
    const lateness = memory.received.map((e) => Date.parse(e.ts_edge) - Date.parse(e.ts_source));
    expect(Math.max(...lateness)).toBeGreaterThan(100_000);
  });

  it('resends some events as duplicates when acknowledgements get lost', async () => {
    const buffer = new StoreAndForwardBuffer(':memory:');
    const memory = new MemorySink();
    const fwd = new Forwarder(buffer, new FaultySink(memory, 0.5, new Rng(3)), { freshBatch: 100, backlogBatch: 100, intervalMs: 1000 });
    for (let i = 0; i < 50; i++) {
      buffer.enqueue(
        toEvent(
          { machineId: 'LS01', meterId: 'EM01', tsMs: Date.UTC(2026, 9, 1) + i * 5000, state: 'idle', jobId: null, status: 'Good',
            values: { l1_a: 2, l2_a: 2, l3_a: 2, voltage_v: 400, power_kw: 1.2, power_factor: 0.93, energy_kwh_total: 100 } },
          2,
        ),
      );
    }
    await fwd.flushOnce();
    expect(memory.received.length).toBeGreaterThan(50);
    expect(new Set(memory.received.map((e) => e.seq)).size).toBe(50);
  });
});

describe('gateway against the OPC UA simulator', () => {
  it('turns OPC UA subscriptions into valid, consecutively numbered events', async () => {
    const port = 49000 + Math.floor(Math.random() * 500);
    const sim = await startSimServer({ port, assumptions: a, pkiFolder: join(dir, 'sim-pki') });
    const memory = new MemorySink();
    const gw = await startGateway({
      endpointUrl: sim.endpointUrl,
      assumptions: a,
      buffer: new StoreAndForwardBuffer(':memory:'),
      sink: memory,
      pkiFolder: join(dir, 'gw-pki'),
      schemaVersion: 2,
      flushIntervalMs: 500,
    });
    await new Promise((r) => setTimeout(r, 14_000));
    await gw.stop();
    await sim.stop();

    expect(memory.received.length).toBeGreaterThanOrEqual(a.park.machines.length);
    for (const e of memory.received) expect(MeterEvent.safeParse(e).success).toBe(true);
    const meters = new Set(memory.received.map((e) => e.meter_id));
    expect(meters.size).toBe(a.park.machines.length);
    for (const m of meters) {
      const seqs = memory.received.filter((e) => e.meter_id === m).map((e) => e.seq);
      expect(seqs).toEqual(seqs.map((_, i) => i));
    }
    expect(memory.received.every((e) => (e.state === 'building') === (e.job_id !== null))).toBe(true);
  }, 60_000);
});
