import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { parse } from 'yaml';
import { z } from 'zod';

const range = z
  .tuple([z.number(), z.number()])
  .refine(([min, max]) => min <= max, { message: 'range must be [min, max] with min <= max' });

const stateSpec = z.strictObject({ power_kw: range, duration_h: range.optional() });

export const Assumptions = z.strictObject({
  seed: z.int(),
  park: z.strictObject({
    halls: z.array(z.strictObject({ hall_id: z.string(), name: z.string() })).min(1),
    machines: z.array(z.strictObject({ machine_id: z.string(), meter_id: z.string(), hall_id: z.string() })).min(1),
  }),
  electrical: z.strictObject({
    voltage_v_nominal: z.number().positive(),
    voltage_jitter_pct: z.number().nonnegative(),
    power_factor: range,
    phase_imbalance_pct: z.number().nonnegative(),
  }),
  states: z.strictObject({
    off: stateSpec,
    idle: stateSpec,
    heat_up: stateSpec.required({ duration_h: true }),
    building: z.strictObject({
      power_kw: range,
      exposure_extra_kw: range,
      layer_cycle_s: range,
      exposure_share: range,
      duration_h: range,
    }),
    cool_down: stateSpec.required({ duration_h: true }),
    unpacking: stateSpec.required({ duration_h: true }),
  }),
  schedule: z.strictObject({
    builds_per_machine_per_week: range,
    idle_between_jobs_h: range,
    off_share_of_gaps: z.number().min(0).max(1),
  }),
  faults: z.strictObject({
    heatup_overrun: z.strictObject({ probability_per_build: z.number().min(0).max(1), duration_factor: range }),
    meter_dropout: z.strictObject({ per_meter_per_month: z.number().nonnegative(), duration_min: range }),
    duplicates: z.strictObject({ probability_per_event: z.number().min(0).max(1) }),
    gateway_outage: z.strictObject({ per_month: z.number().nonnegative(), duration_min: range }),
    counter_reset: z.array(z.strictObject({ meter_id: z.string(), at_month: z.int().nonnegative() })),
    spikes: z.strictObject({ probability_per_event: z.number().min(0).max(1), factor: range }),
    opc_status: z.strictObject({
      uncertain_probability: z.number().min(0).max(1),
      bad_probability: z.number().min(0).max(1),
    }),
    schema_change: z.strictObject({ v2_from_month: z.int().nonnegative() }),
  }),
  volumes: z.strictObject({
    live_interval_s: range,
    history_interval_s: z.number().positive(),
    history_months: z.int().positive(),
  }),
  tariff: z.strictObject({
    currency: z.string(),
    peak: z.strictObject({
      chf_per_kwh: z.number().nonnegative(),
      weekdays: z.array(z.int().min(1).max(7)),
      from_hour: z.int().min(0).max(23),
      to_hour: z.int().min(1).max(24),
    }),
    off_peak: z.strictObject({ chf_per_kwh: z.number().nonnegative() }),
  }),
});
export type Assumptions = z.infer<typeof Assumptions>;

export const DEFAULT_ASSUMPTIONS_PATH = fileURLToPath(new URL('../../../config/assumptions.yaml', import.meta.url));

/** Loads and validates the assumptions file. ASSUMPTIONS_PATH overrides the repo default. */
export function loadAssumptions(path = process.env.ASSUMPTIONS_PATH ?? DEFAULT_ASSUMPTIONS_PATH): Assumptions {
  const assumptions = Assumptions.parse(parse(readFileSync(path, 'utf8')));
  checkReferences(assumptions);
  return assumptions;
}

function checkReferences(a: Assumptions): void {
  const halls = new Set(a.park.halls.map((h) => h.hall_id));
  const machines = new Set<string>();
  const meters = new Set<string>();
  for (const m of a.park.machines) {
    if (!halls.has(m.hall_id)) throw new Error(`machine ${m.machine_id}: unknown hall ${m.hall_id}`);
    if (machines.has(m.machine_id)) throw new Error(`duplicate machine_id ${m.machine_id}`);
    if (meters.has(m.meter_id)) throw new Error(`duplicate meter_id ${m.meter_id}`);
    machines.add(m.machine_id);
    meters.add(m.meter_id);
  }
  for (const r of a.faults.counter_reset) {
    if (!meters.has(r.meter_id)) throw new Error(`counter_reset: unknown meter ${r.meter_id}`);
  }
}
