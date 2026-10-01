import { z } from 'zod';

export const MACHINE_STATES = ['off', 'idle', 'heat_up', 'building', 'cool_down', 'unpacking'] as const;
export const MachineState = z.enum(MACHINE_STATES);
export type MachineState = z.infer<typeof MachineState>;

/** OPC UA StatusCode severity, reduced to the three classes the pipeline cares about. */
export const OpcStatus = z.enum(['Good', 'Uncertain', 'Bad']);
export type OpcStatus = z.infer<typeof OpcStatus>;

const fields = {
  machine_id: z.string().min(1),
  meter_id: z.string().min(1),
  /** Per-meter sequence number. Gaps mean lost events; repeats mean duplicates. */
  seq: z.int().nonnegative(),
  /** Meter time (UTC) of the measurement. */
  ts_source: z.iso.datetime(),
  /** Gateway time (UTC) when the event left the edge. Late events have ts_edge far after ts_source. */
  ts_edge: z.iso.datetime(),
  state: MachineState,
  /** Set for the whole job cycle (heat-up to unpacking) so energy can be attributed per job; null otherwise. */
  job_id: z.string().min(1).nullable(),
  l1_a: z.number().nonnegative(),
  l2_a: z.number().nonnegative(),
  l3_a: z.number().nonnegative(),
  voltage_v: z.number().nonnegative(),
  power_kw: z.number().nonnegative(),
  /** Monotonic meter counter. It restarts near zero when a meter is replaced. */
  energy_kwh_total: z.number().nonnegative(),
  opc_status: OpcStatus,
};

export const MeterEventV1 = z.strictObject({ schema_version: z.literal(1), ...fields });
/** Version 2 adds the measured power factor. */
export const MeterEventV2 = z.strictObject({
  schema_version: z.literal(2),
  ...fields,
  power_factor: z.number().min(0).max(1),
});

export const MeterEvent = z.discriminatedUnion('schema_version', [MeterEventV1, MeterEventV2]);
export type MeterEventV1 = z.infer<typeof MeterEventV1>;
export type MeterEventV2 = z.infer<typeof MeterEventV2>;
export type MeterEvent = z.infer<typeof MeterEvent>;
