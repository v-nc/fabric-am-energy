import { loadAssumptions } from '@am-energy/shared';
import { withDemoOverrides } from './demo.ts';
import { startSimServer } from './server.ts';

const port = Number(process.env.OPCUA_PORT ?? 4840);
const durationScale = Number(process.env.DURATION_SCALE ?? 1);
const pkiFolder = process.env.PKI_FOLDER ?? './data/pki';

const sim = await startSimServer({
  port,
  assumptions: withDemoOverrides(loadAssumptions(), process.env),
  pkiFolder,
  durationScale,
  onStateChange: (machine, from, to, jobId) =>
    console.log(`${new Date().toISOString()} ${machine} ${from ?? '-'} → ${to}${jobId ? ` (${jobId})` : ''}`),
});
console.log(`OPC UA simulator listening on ${sim.endpointUrl} (duration scale ${durationScale}` +
  (process.env.HEATUP_OVERRUN_PROBABILITY ? `, demo overrun probability ${process.env.HEATUP_OVERRUN_PROBABILITY}` : '') + ')');

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, async () => {
    await sim.stop();
    process.exit(0);
  });
}
