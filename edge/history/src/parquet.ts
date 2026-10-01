import { parquetWriteFile, type ColumnSource } from 'hyparquet-writer';
import type { HistoryRow } from './generate.ts';

/**
 * Writes one month of history. Months from schema_version 2 on carry the extra power_factor column, so the files
 * have two schemas, and the load into bronze has to merge them.
 */
export function writeMonthParquet(filename: string, rows: readonly HistoryRow[]): void {
  const col = <K extends keyof HistoryRow>(name: K, type: NonNullable<ColumnSource['type']>): ColumnSource => ({
    name,
    type,
    data: rows.map((r) => r[name]) as ColumnSource['data'],
  });
  const columns: ColumnSource[] = [
    col('schema_version', 'INT32'),
    col('machine_id', 'STRING'),
    col('meter_id', 'STRING'),
    { name: 'seq', type: 'INT64', data: rows.map((r) => BigInt(r.seq)) },
    { name: 'ts_source', type: 'TIMESTAMP', data: rows.map((r) => new Date(r.ts_source)) },
    { name: 'ts_edge', type: 'TIMESTAMP', data: rows.map((r) => new Date(r.ts_edge)) },
    col('state', 'STRING'),
    col('job_id', 'STRING'),
    col('l1_a', 'DOUBLE'),
    col('l2_a', 'DOUBLE'),
    col('l3_a', 'DOUBLE'),
    col('voltage_v', 'DOUBLE'),
    col('power_kw', 'DOUBLE'),
    col('energy_kwh_total', 'DOUBLE'),
    col('opc_status', 'STRING'),
  ];
  if (rows.some((r) => r.schema_version === 2)) columns.push(col('power_factor', 'DOUBLE'));
  parquetWriteFile({ filename, columnData: columns });
}
