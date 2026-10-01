import { MachineModel, MeterModel, Rng, type Assumptions, type MachineState, type OpcStatus } from '@am-energy/shared';

const MINUTE_MS = 60_000;

/** One history row. Same fields as the live MeterEvent; timestamps as epoch milliseconds. */
export interface HistoryRow {
  schema_version: 1 | 2;
  machine_id: string;
  meter_id: string;
  seq: number;
  ts_source: number;
  ts_edge: number;
  state: MachineState;
  job_id: string | null;
  l1_a: number;
  l2_a: number;
  l3_a: number;
  voltage_v: number;
  power_kw: number;
  energy_kwh_total: number;
  opc_status: OpcStatus;
  /** Only in schema_version 2. */
  power_factor: number | null;
}

/** What was injected, so the data-quality checks downstream can be tested against it. */
export interface GroundTruth {
  start: string;
  end: string;
  heatups: { machine_id: string; start: string; end: string; overrun: boolean }[];
  gateway_outages: { start: string; end: string }[];
  counter_resets: { meter_id: string; at: string }[];
  schema_v2_from: string;
  duplicates: number;
  late_events: number;
}

export interface HistoryOptions {
  assumptions: Assumptions;
  /** Exclusive end of the history (UTC). The start is history_months calendar months earlier. */
  endMs: number;
  /** Called once per calendar month with that month's rows, sorted by ts_edge (arrival order). */
  onMonth: (monthKey: string, rows: HistoryRow[]) => void | Promise<void>;
}

/**
 * Generates the backfill history with the same 3D-printer and meter models as the live simulator, sampled once per
 * history_interval_s, plus the faults a real gateway produces on the way: normal latency, outages that turn into late
 * and out-of-order arrivals, and duplicates. The schema switches to version 2 at the configured month.
 */
export async function generateHistory(opts: HistoryOptions): Promise<GroundTruth> {
  const a = opts.assumptions;
  const months = monthStarts(opts.endMs, a.volumes.history_months);
  const startMs = months[0]!;
  const v2FromMs = months[a.faults.schema_change.v2_from_month] ?? Infinity;
  const stepMs = a.volumes.history_interval_s * 1000;

  const gw = Rng.derive(a.seed, 'history/gateway');
  const outages = poissonWindows(gw, startMs, opts.endMs, a.faults.gateway_outage.per_month, a.faults.gateway_outage.duration_min);
  const resets = a.faults.counter_reset.map((r) => ({ meter_id: r.meter_id, atMs: months[r.at_month] ?? Infinity }));

  const meters = a.park.machines.map((m) => ({
    machineId: m.machine_id,
    meterId: m.meter_id,
    machine: new MachineModel(m.machine_id, a, startMs),
    meter: new MeterModel(m.meter_id, a, startMs),
    resetAt: resets.find((r) => r.meter_id === m.meter_id)?.atMs ?? Infinity,
    seq: 0,
    prevMs: startMs,
  }));

  let duplicates = 0;
  let late = 0;
  for (let i = 0; i < months.length; i++) {
    const monthStart = months[i]!;
    const monthEnd = months[i + 1] ?? opts.endMs;
    const rows: HistoryRow[] = [];
    for (const m of meters) {
      for (let t = monthStart; t < monthEnd; t += stepMs) {
        m.meter.advance(t, m.machine.energyKwhBetween(m.prevMs, t));
        m.prevMs = t;
        if (t >= m.resetAt) {
          m.meter.replace();
          m.resetAt = Infinity;
        }
        const seg = m.machine.segmentAt(t);
        const r = m.meter.read(t, m.machine.powerAt(t));
        if (!r) continue; // meter dropout: no reading, the counter keeps counting

        let tsEdge = t + gw.uniform([0.3, 3]) * 1000;
        const outage = outages.find((o) => t >= o.start && t < o.end);
        if (outage) {
          // Buffered at the edge, replayed after the outage behind the live data.
          tsEdge = outage.end + (t - outage.start) * 0.05 + gw.uniform([5, 60]) * 1000;
          late++;
        }
        const v2 = t >= v2FromMs;
        const row: HistoryRow = {
          schema_version: v2 ? 2 : 1,
          machine_id: m.machineId,
          meter_id: m.meterId,
          seq: m.seq++,
          ts_source: t,
          ts_edge: Math.round(tsEdge),
          state: seg.state,
          job_id: seg.state === 'building' ? seg.jobId : null,
          l1_a: r.l1_a,
          l2_a: r.l2_a,
          l3_a: r.l3_a,
          voltage_v: r.voltage_v,
          power_kw: r.power_kw,
          energy_kwh_total: r.energy_kwh_total,
          opc_status: r.opc_status,
          power_factor: v2 ? r.power_factor : null,
        };
        rows.push(row);
        if (gw.chance(a.faults.duplicates.probability_per_event)) {
          rows.push({ ...row, ts_edge: row.ts_edge + Math.round(gw.uniform([1, 30]) * 1000) });
          duplicates++;
        }
      }
    }
    rows.sort((x, y) => x.ts_edge - y.ts_edge);
    await opts.onMonth(monthKey(monthStart), rows);
  }

  const iso = (ms: number) => new Date(ms).toISOString();
  return {
    start: iso(startMs),
    end: iso(opts.endMs),
    heatups: meters.flatMap((m) =>
      m.machine.segments
        .filter((s) => s.state === 'heat_up' && s.startMs >= startMs && s.endMs <= opts.endMs)
        .map((s) => ({ machine_id: m.machineId, start: iso(s.startMs), end: iso(s.endMs), overrun: s.heatupOverrun })),
    ),
    gateway_outages: outages.map((o) => ({ start: iso(o.start), end: iso(o.end) })),
    counter_resets: resets.filter((r) => r.atMs < opts.endMs).map((r) => ({ meter_id: r.meter_id, at: iso(r.atMs) })),
    schema_v2_from: Number.isFinite(v2FromMs) ? iso(v2FromMs) : 'never',
    duplicates,
    late_events: late,
  };
}

/** UTC month starts: the start of the history and every following month, up to (excluding) endMs. */
export function monthStarts(endMs: number, months: number): number[] {
  const end = new Date(endMs);
  const first = Date.UTC(end.getUTCFullYear(), end.getUTCMonth() - months, 1);
  const out: number[] = [];
  for (let i = 0; ; i++) {
    const d = new Date(first);
    d.setUTCMonth(d.getUTCMonth() + i);
    if (d.getTime() >= endMs) break;
    out.push(d.getTime());
  }
  return out;
}

function monthKey(ms: number): string {
  return new Date(ms).toISOString().slice(0, 7);
}

function poissonWindows(rng: Rng, startMs: number, endMs: number, perMonth: number, durationMin: readonly [number, number]) {
  const windows: { start: number; end: number }[] = [];
  if (perMonth <= 0) return windows;
  const rate = perMonth / (30 * 24 * 60 * MINUTE_MS);
  for (let t = startMs + rng.exponential(rate); t < endMs; ) {
    const end = t + rng.uniform(durationMin) * MINUTE_MS;
    windows.push({ start: t, end });
    t = end + rng.exponential(rate);
  }
  return windows;
}
