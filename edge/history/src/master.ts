import type { Assumptions } from '@am-energy/shared';

export interface MasterRow {
  snapshot_date: string;
  machine_id: string;
  hall_id: string;
  meter_id: string;
  meter_serial: string;
}

/** Machine master data as of each month start, with the configured changes applied from their month on. */
export function masterSnapshots(a: Assumptions, monthStartsMs: readonly number[]): MasterRow[][] {
  return monthStartsMs.map((ms, month) =>
    a.park.machines.map((m) => {
      const row: MasterRow = {
        snapshot_date: new Date(ms).toISOString().slice(0, 10),
        machine_id: m.machine_id,
        hall_id: m.hall_id,
        meter_id: m.meter_id,
        meter_serial: `${m.meter_id}-SN-A`,
      };
      for (const c of a.master_data.changes) {
        if (c.machine_id === m.machine_id && c.at_month <= month) row[c.field] = c.value;
      }
      return row;
    }),
  );
}

export function toCsv(rows: readonly MasterRow[]): string {
  const cols = ['snapshot_date', 'machine_id', 'hall_id', 'meter_id', 'meter_serial'] as const;
  return [cols.join(','), ...rows.map((r) => cols.map((c) => r[c]).join(','))].join('\n') + '\n';
}
