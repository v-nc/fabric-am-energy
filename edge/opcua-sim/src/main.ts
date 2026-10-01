import { loadAssumptions } from '@am-energy/shared';
import { startSimServer } from './server.ts';

const port = Number(process.env.OPCUA_PORT ?? 4840);
const durationScale = Number(process.env.DURATION_SCALE ?? 1);
const pkiFolder = process.env.PKI_FOLDER ?? './data/pki';

const sim = await startSimServer({
  port,
  assumptions: loadAssumptions(),
  pkiFolder,
  durationScale,
  onStateChange: (machine, from, to, jobId) =>
    console.log(`${new Date().toISOString()} ${machine} ${from ?? '-'} → ${to}${jobId ? ` (${jobId})` : ''}`),
});
console.log(`OPC UA simulator listening on ${sim.endpointUrl} (duration scale ${durationScale})`);

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, async () => {
    await sim.stop();
    process.exit(0);
  });
}
