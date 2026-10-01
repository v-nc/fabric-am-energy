import type { MachineState, MeterField, OpcStatus } from '@am-energy/shared';

export type PointField = 'state' | 'job_id' | MeterField;
export const POINT_FIELDS_PER_READING = 9; // state, job_id and 7 meter values

export interface PointUpdate {
  machineId: string;
  meterId: string;
  field: PointField;
  value: unknown;
  status: OpcStatus;
  sourceTsMs: number;
}

export interface Reading {
  machineId: string;
  meterId: string;
  tsMs: number;
  state: MachineState;
  jobId: string | null;
  values: Record<MeterField, number>;
  /** Worst status among the meter values. */
  status: OpcStatus;
}

const SEVERITY: Record<OpcStatus, number> = { Good: 0, Uncertain: 1, Bad: 2 };

interface Pending {
  tsMs: number;
  points: Map<PointField, PointUpdate>;
}

/**
 * OPC UA delivers one notification per variable. The simulator writes all variables of one reading with the same
 * sourceTimestamp, so updates are grouped per meter and timestamp, and a reading is emitted once all points are in.
 * An incomplete group is dropped (and counted) when a newer timestamp arrives for the same meter.
 */
export class ReadingAssembler {
  private readonly pending = new Map<string, Pending>();
  private readonly lastEmitted = new Map<string, number>();
  incompleteDropped = 0;
  staleIgnored = 0;

  constructor(private readonly onReading: (r: Reading) => void) {}

  push(u: PointUpdate): void {
    let p = this.pending.get(u.meterId);
    if ((p && u.sourceTsMs < p.tsMs) || u.sourceTsMs <= (this.lastEmitted.get(u.meterId) ?? -Infinity)) {
      this.staleIgnored++;
      return;
    }
    if (!p || u.sourceTsMs > p.tsMs) {
      if (p) this.incompleteDropped++;
      p = { tsMs: u.sourceTsMs, points: new Map() };
      this.pending.set(u.meterId, p);
    }
    p.points.set(u.field, u);
    if (p.points.size === POINT_FIELDS_PER_READING) {
      this.pending.delete(u.meterId);
      this.lastEmitted.set(u.meterId, p.tsMs);
      this.onReading(toReading(p));
    }
  }
}

function toReading(p: Pending): Reading {
  const get = (f: PointField) => p.points.get(f)!;
  const meterFields = [...p.points.keys()].filter((f): f is MeterField => f !== 'state' && f !== 'job_id');
  let status: OpcStatus = 'Good';
  for (const f of meterFields) if (SEVERITY[get(f).status] > SEVERITY[status]) status = get(f).status;
  const first = get('state');
  return {
    machineId: first.machineId,
    meterId: first.meterId,
    tsMs: p.tsMs,
    state: first.value as MachineState,
    jobId: (get('job_id').value as string | null) || null,
    values: Object.fromEntries(meterFields.map((f) => [f, Number(get(f).value)])) as Record<MeterField, number>,
    status,
  };
}
