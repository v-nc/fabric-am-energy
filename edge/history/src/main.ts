import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { loadAssumptions } from '@am-energy/shared';
import { generateHistory } from './generate.ts';
import { writeMonthParquet } from './parquet.ts';

// The end date is fixed by default so the history is identical on every run; HISTORY_END overrides it.
const endMs = Date.parse(process.env.HISTORY_END ?? '2026-10-01T00:00:00Z');
const outDir = process.env.HISTORY_DIR ?? '../../data/history';
mkdirSync(outDir, { recursive: true });

let total = 0;
const truth = await generateHistory({
  assumptions: loadAssumptions(),
  endMs,
  onMonth: (month, rows) => {
    const file = join(outDir, `meter_events_${month}.parquet`);
    writeMonthParquet(file, rows);
    total += rows.length;
    console.log(`${file}: ${rows.length} rows`);
  },
});
writeFileSync(join(outDir, 'ground_truth.json'), JSON.stringify(truth, null, 2));
console.log(
  `${total} rows, ${truth.duplicates} duplicates, ${truth.late_events} late events, ` +
    `${truth.heatups.filter((h) => h.overrun).length}/${truth.heatups.length} heat-up overruns, ` +
    `${truth.gateway_outages.length} gateway outages, schema v2 from ${truth.schema_v2_from}`,
);
