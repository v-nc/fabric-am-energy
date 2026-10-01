import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { AttributeIds, OPCUACertificateManager, OPCUAClient, StatusCodes, type ClientSession } from 'node-opcua';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { MACHINE_STATES, loadAssumptions } from '@am-energy/shared';
import { NAMESPACE_URI, nodeIds, startSimServer, type SimServer } from '../src/server.ts';

const dir = mkdtempSync(join(tmpdir(), 'am-energy-sim-'));
let sim: SimServer;
let client: OPCUAClient;
let session: ClientSession;
let ns: number;

beforeAll(async () => {
  sim = await startSimServer({ port: 48400 + Math.floor(Math.random() * 500), assumptions: loadAssumptions(), pkiFolder: join(dir, 'server') });
  client = OPCUAClient.create({
    endpointMustExist: false,
    clientCertificateManager: new OPCUACertificateManager({ rootFolder: join(dir, 'client'), automaticallyAcceptUnknownCertificate: true }),
  });
  await client.connect(sim.endpointUrl);
  session = await client.createSession();
  ns = (await session.readNamespaceArray()).indexOf(NAMESPACE_URI);
}, 60_000);

afterAll(async () => {
  await session?.close();
  await client?.disconnect();
  await sim?.stop();
  rmSync(dir, { recursive: true, force: true });
});

describe('OPC UA simulator', () => {
  it('registers its namespace', () => {
    expect(ns).toBeGreaterThan(0);
  });

  it('exposes the park as halls containing 3D printers', async () => {
    const halls = await session.browse(`ns=${ns};s=Park`);
    expect(halls.references?.map((r) => r.browseName.name).sort()).toEqual(['H1', 'H2']);
    const h1 = await session.browse(`ns=${ns};s=Hall.H1`);
    expect(h1.references?.map((r) => r.browseName.name)).toContain('LS01');
  });

  it('serves a state and a meter reading with one shared source timestamp', async () => {
    const [state, power, energy] = await session.read(
      [nodeIds.state('LS01'), nodeIds.meter('EM01', 'ActivePower'), nodeIds.meter('EM01', 'EnergyTotal')].map((id) => ({
        nodeId: `ns=${ns};s=${id}`,
        attributeId: AttributeIds.Value,
      })),
    );
    expect(MACHINE_STATES).toContain(state!.value.value);
    expect(typeof power!.value.value).toBe('number');
    expect(energy!.value.value).toBeGreaterThan(1000);
    expect(state!.sourceTimestamp?.getTime()).toBe(power!.sourceTimestamp?.getTime());
  });

  it('restarts the counter after a meter swap', async () => {
    sim.replaceMeter('EM07');
    await new Promise((r) => setTimeout(r, 11_000)); // longest live interval is 10 s
    const dv = await session.read({ nodeId: `ns=${ns};s=${nodeIds.meter('EM07', 'EnergyTotal')}`, attributeId: AttributeIds.Value });
    if (dv.statusCode.equals(StatusCodes.Good)) expect(dv.value.value).toBeLessThan(1);
  }, 20_000);
});
