import { describe, expect, it } from 'vitest';
import { loadAssumptions } from '@am-energy/shared';
import { withDemoOverrides } from '../src/demo.ts';

describe('withDemoOverrides', () => {
  const a = loadAssumptions();

  it('changes nothing without demo variables', () => {
    expect(withDemoOverrides(a, {})).toEqual(a);
  });

  it('raises the overrun probability and shortens idle gaps, without touching the original', () => {
    const d = withDemoOverrides(a, { HEATUP_OVERRUN_PROBABILITY: '0.5', IDLE_BETWEEN_JOBS_H: '0.5,2' });
    expect(d.faults.heatup_overrun.probability_per_build).toBe(0.5);
    expect(d.schedule.idle_between_jobs_h).toEqual([0.5, 2]);
    expect(a.faults.heatup_overrun.probability_per_build).toBe(0.06);
  });

  it('rejects invalid values', () => {
    expect(() => withDemoOverrides(a, { HEATUP_OVERRUN_PROBABILITY: '2' })).toThrow();
    expect(() => withDemoOverrides(a, { IDLE_BETWEEN_JOBS_H: '3,1' })).toThrow();
  });
});
