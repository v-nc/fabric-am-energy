import {
  AttributeIds,
  ClientMonitoredItemGroup,
  ClientSubscription,
  DataChangeFilter,
  DataChangeTrigger,
  DeadbandType,
  OPCUACertificateManager,
  OPCUAClient,
  TimestampsToReturn,
  type ClientSession,
  type StatusCode,
} from 'node-opcua';
import { METER_VARIABLES, NAMESPACE_URI, nodeIds, type MeterVariable, type OpcStatus } from '@am-energy/shared';
import type { PointField, PointUpdate } from './assembler.ts';

export interface SourceOptions {
  endpointUrl: string;
  machines: readonly { machine_id: string; meter_id: string }[];
  pkiFolder: string;
  onUpdate: (u: PointUpdate) => void;
  log?: (msg: string) => void;
}

export interface Source {
  close(): Promise<void>;
}

interface Item {
  machineId: string;
  meterId: string;
  field: PointField;
  nodeId: string;
}

/**
 * Subscribes to State, JobId and the 7 meter variables of every 3D printer. The trigger is StatusValueTimestamp so a
 * new reading is reported even when its value equals the previous one (a 3D printer that stays idle).
 * node-opcua reconnects and re-creates the subscription by itself after a network interruption.
 */
export async function subscribeToPark(opts: SourceOptions): Promise<Source> {
  const log = opts.log ?? (() => {});
  const client = OPCUAClient.create({
    applicationName: 'am-energy-gateway',
    endpointMustExist: false,
    keepSessionAlive: true,
    connectionStrategy: { maxRetry: -1, initialDelay: 1000, maxDelay: 10_000 },
    clientCertificateManager: new OPCUACertificateManager({
      rootFolder: opts.pkiFolder,
      automaticallyAcceptUnknownCertificate: true,
    }),
  });
  client.on('backoff', (retry, delay) => log(`OPC UA connect retry ${retry}, next in ${delay} ms`));
  client.on('connection_lost', () => log('OPC UA connection lost'));
  client.on('connection_reestablished', () => log('OPC UA connection re-established'));

  await client.connect(opts.endpointUrl);
  const session: ClientSession = await client.createSession();
  const ns = (await session.readNamespaceArray()).indexOf(NAMESPACE_URI);
  if (ns < 0) throw new Error(`namespace ${NAMESPACE_URI} not found on ${opts.endpointUrl}`);

  const items: Item[] = opts.machines.flatMap((m) => [
    { machineId: m.machine_id, meterId: m.meter_id, field: 'state' as const, nodeId: nodeIds.state(m.machine_id) },
    { machineId: m.machine_id, meterId: m.meter_id, field: 'job_id' as const, nodeId: nodeIds.jobId(m.machine_id) },
    ...(Object.entries(METER_VARIABLES) as [MeterVariable, (typeof METER_VARIABLES)[MeterVariable]][]).map(([v, field]) => ({
      machineId: m.machine_id,
      meterId: m.meter_id,
      field,
      nodeId: nodeIds.meter(m.meter_id, v),
    })),
  ]);

  const subscription = ClientSubscription.create(session, {
    requestedPublishingInterval: 1000,
    requestedLifetimeCount: 600,
    requestedMaxKeepAliveCount: 20,
    maxNotificationsPerPublish: 2000,
    publishingEnabled: true,
    priority: 10,
  });
  const group = ClientMonitoredItemGroup.create(
    subscription,
    items.map((i) => ({ nodeId: `ns=${ns};s=${i.nodeId}`, attributeId: AttributeIds.Value })),
    {
      samplingInterval: 1000,
      queueSize: 20,
      discardOldest: true,
      filter: new DataChangeFilter({ trigger: DataChangeTrigger.StatusValueTimestamp, deadbandType: DeadbandType.None }),
    },
    TimestampsToReturn.Both,
  );
  group.on('changed', (_item, dataValue, index) => {
    const item = items[index];
    if (!item || !dataValue.sourceTimestamp) return;
    opts.onUpdate({
      machineId: item.machineId,
      meterId: item.meterId,
      field: item.field,
      value: dataValue.value.value,
      status: toOpcStatus(dataValue.statusCode),
      sourceTsMs: dataValue.sourceTimestamp.getTime(),
    });
  });
  log(`subscribed to ${items.length} OPC UA variables on ${opts.endpointUrl}`);

  return {
    async close() {
      await subscription.terminate();
      await session.close();
      await client.disconnect();
    },
  };
}

export function toOpcStatus(code: StatusCode): OpcStatus {
  if (code.isGood()) return 'Good';
  if (code.isBad()) return 'Bad';
  return 'Uncertain';
}
