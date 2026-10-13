import type { Assumptions } from '@am-energy/shared';

/**
 * Demo overrides for a live presentation: more frequent heat-up overruns and shorter idle gaps between jobs, so the
 * real-time alert can be shown within minutes. Read from environment variables; unset variables change nothing.
 *   HEATUP_OVERRUN_PROBABILITY=0.5   share of builds whose heat-up overruns (default from assumptions.yaml)
 *   IDLE_BETWEEN_JOBS_H=0.5,2        idle gap between jobs in hours, as min,max
 */
export function withDemoOverrides(a: Assumptions, env: Record<string, string | undefined>): Assumptions {
  const out = structuredClone(a);
  if (env.HEATUP_OVERRUN_PROBABILITY) {
    const p = Number(env.HEATUP_OVERRUN_PROBABILITY);
    if (!(p >= 0 && p <= 1)) throw new Error(`HEATUP_OVERRUN_PROBABILITY must be between 0 and 1, got ${env.HEATUP_OVERRUN_PROBABILITY}`);
    out.faults.heatup_overrun.probability_per_build = p;
  }
  if (env.IDLE_BETWEEN_JOBS_H) {
    const [lo, hi] = env.IDLE_BETWEEN_JOBS_H.split(',').map(Number);
    if (!(lo! >= 0 && hi! >= lo!)) throw new Error(`IDLE_BETWEEN_JOBS_H must be min,max hours, got ${env.IDLE_BETWEEN_JOBS_H}`);
    out.schedule.idle_between_jobs_h = [lo!, hi!];
  }
  return out;
}
