import { Rng, loadAssumptions } from '@am-energy/shared';
import { StoreAndForwardBuffer } from './buffer.ts';
import { startGateway } from './gateway.ts';
import { EventHubsSink, FaultySink, FileSink, type Sink } from './sinks.ts';

const env = process.env;
const log = (msg: string) => console.log(`${new Date().toISOString()} ${msg}`);
const assumptions = loadAssumptions();

const kind = env.SINK ?? 'file';
let inner: Sink;
if (kind === 'eventhubs') {
  if (!env.EVENTSTREAM_CONNECTION_STRING) throw new Error('SINK=eventhubs needs EVENTSTREAM_CONNECTION_STRING');
  inner = new EventHubsSink(env.EVENTSTREAM_CONNECTION_STRING);
} else {
  inner = new FileSink(env.FILE_SINK_DIR ?? './data/out');
}
const sink = new FaultySink(
  inner,
  Number(env.DUPLICATE_PROBABILITY ?? assumptions.faults.duplicates.probability_per_event),
  Rng.derive(assumptions.seed, 'gateway/duplicates'),
);

const schemaVersion = Number(env.SCHEMA_VERSION ?? 1);
if (schemaVersion !== 1 && schemaVersion !== 2) throw new Error('SCHEMA_VERSION must be 1 or 2');
const buffer = new StoreAndForwardBuffer(env.BUFFER_PATH ?? './data/buffer.sqlite');

const gw = await startGateway({
  endpointUrl: env.OPCUA_ENDPOINT ?? 'opc.tcp://localhost:4840/UA/AMEnergySim',
  assumptions,
  buffer,
  sink,
  pkiFolder: env.PKI_FOLDER ?? './data/pki',
  schemaVersion,
  flushIntervalMs: Number(env.FLUSH_INTERVAL_MS ?? 2000),
  log,
});
log(`gateway running: sink=${kind}, schema_version=${schemaVersion}, ${buffer.size()} events already buffered`);

// `docker kill -s USR1 <gateway>` toggles a simulated network outage.
process.on('SIGUSR1', () => {
  sink.outage = !sink.outage;
  log(sink.outage ? 'simulated network outage STARTED' : 'simulated network outage ENDED');
});

setInterval(() => {
  const s = gw.forwarder.stats;
  log(`sent=${s.sent} buffered=${buffer.size()} failed_cycles=${s.failedCycles} incomplete_readings=${gw.assembler.incompleteDropped}`);
}, 30_000).unref();

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, async () => {
    await gw.stop();
    buffer.close();
    process.exit(0);
  });
}
