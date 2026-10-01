import type { Assumptions } from '@am-energy/shared';
import { ReadingAssembler, type Reading } from './assembler.ts';
import type { DistributiveOmit, PendingEvent, StoreAndForwardBuffer } from './buffer.ts';
import { Forwarder } from './forwarder.ts';
import { subscribeToPark, type Source } from './opcua-source.ts';
import type { Sink } from './sinks.ts';

export interface GatewayOptions {
  endpointUrl: string;
  assumptions: Assumptions;
  buffer: StoreAndForwardBuffer;
  sink: Sink;
  pkiFolder: string;
  schemaVersion: 1 | 2;
  flushIntervalMs?: number;
  log?: (msg: string) => void;
}

export interface Gateway {
  assembler: ReadingAssembler;
  forwarder: Forwarder;
  stop(): Promise<void>;
}

/** OPC UA subscription → reading assembly → store-and-forward buffer → sink. */
export async function startGateway(opts: GatewayOptions): Promise<Gateway> {
  const assembler = new ReadingAssembler((r) => opts.buffer.enqueue(toEvent(r, opts.schemaVersion)));
  const forwarder = new Forwarder(opts.buffer, opts.sink, {
    freshBatch: 500,
    backlogBatch: 2000,
    intervalMs: opts.flushIntervalMs ?? 2000,
    ...(opts.log ? { log: opts.log } : {}),
  });
  const source: Source = await subscribeToPark({
    endpointUrl: opts.endpointUrl,
    machines: opts.assumptions.park.machines,
    pkiFolder: opts.pkiFolder,
    onUpdate: (u) => assembler.push(u),
    ...(opts.log ? { log: opts.log } : {}),
  });
  forwarder.start();
  return {
    assembler,
    forwarder,
    async stop() {
      forwarder.stop();
      await source.close();
      await forwarder.flushOnce();
      await opts.sink.close();
    },
  };
}

export function toEvent(r: Reading, schemaVersion: 1 | 2): DistributiveOmit<PendingEvent, 'seq'> {
  const common = {
    machine_id: r.machineId,
    meter_id: r.meterId,
    ts_source: new Date(r.tsMs).toISOString(),
    state: r.state,
    job_id: r.state === 'building' ? r.jobId : null,
    l1_a: r.values.l1_a,
    l2_a: r.values.l2_a,
    l3_a: r.values.l3_a,
    voltage_v: r.values.voltage_v,
    power_kw: r.values.power_kw,
    energy_kwh_total: r.values.energy_kwh_total,
    opc_status: r.status,
  };
  return schemaVersion === 2
    ? { schema_version: 2, ...common, power_factor: r.values.power_factor }
    : { schema_version: 1, ...common };
}
