import {
  DataType,
  OPCUACertificateManager,
  OPCUAServer,
  StatusCodes,
  type Namespace,
  type StatusCode,
  type UAObject,
  type UAVariable,
} from 'node-opcua';
import { MachineModel, MeterModel, Rng, type Assumptions, type MachineState, type OpcStatus } from '@am-energy/shared';

export const NAMESPACE_URI = 'urn:am-energy:opcua-sim';

/** Meter variables, keyed by OPC UA browse name, with the event field each one feeds. */
export const METER_VARIABLES = {
  L1Current: 'l1_a',
  L2Current: 'l2_a',
  L3Current: 'l3_a',
  Voltage: 'voltage_v',
  ActivePower: 'power_kw',
  PowerFactor: 'power_factor',
  EnergyTotal: 'energy_kwh_total',
} as const;
export type MeterVariable = keyof typeof METER_VARIABLES;

/** String node ids in the simulator namespace: "LS01.State", "LS01.JobId", "EM01.ActivePower", … */
export const nodeIds = {
  state: (machineId: string) => `${machineId}.State`,
  jobId: (machineId: string) => `${machineId}.JobId`,
  meter: (meterId: string, variable: MeterVariable) => `${meterId}.${variable}`,
};

const STATUS: Record<OpcStatus, StatusCode> = {
  Good: StatusCodes.Good,
  Uncertain: StatusCodes.UncertainLastUsableValue,
  Bad: StatusCodes.BadSensorFailure,
};

export interface SimServerOptions {
  port: number;
  assumptions: Assumptions;
  pkiFolder: string;
  /** Multiplies state durations; see MachineModelOptions. */
  durationScale?: number;
  onStateChange?: (machineId: string, from: MachineState | null, to: MachineState, jobId: string | null) => void;
}

export interface SimServer {
  endpointUrl: string;
  /** Simulates a meter swap: the counter restarts near zero. */
  replaceMeter(meterId: string): void;
  stop(): Promise<void>;
}

/**
 * OPC UA server for the simulated park. Per 3D printer it exposes State and JobId, and per energy meter the phase
 * currents, voltage, active power, power factor and kWh counter. All variables of one reading share one
 * sourceTimestamp, so a client can group them back into one event; a meter dropout simply produces no update.
 *
 * Lab shortcut: in a real plant the machine state comes from the 3D printer's controller and the electrical values
 * from a separate IP meter; here one server provides both.
 */
export async function startSimServer(opts: SimServerOptions): Promise<SimServer> {
  const a = opts.assumptions;
  const server = new OPCUAServer({
    port: opts.port,
    resourcePath: '/UA/AMEnergySim',
    buildInfo: { productName: 'am-energy OPC UA simulator', manufacturerName: 'fabric-am-energy (simulation)' },
    serverCertificateManager: new OPCUACertificateManager({
      rootFolder: opts.pkiFolder,
      automaticallyAcceptUnknownCertificate: true,
    }),
  });
  await server.initialize();
  const addressSpace = server.engine.addressSpace!;
  const ns = addressSpace.registerNamespace(NAMESPACE_URI);

  const park = ns.addFolder(addressSpace.rootFolder.objects, { browseName: 'Park', nodeId: 's=Park' });
  const halls = new Map<string, UAObject>();
  for (const h of a.park.halls) {
    halls.set(h.hall_id, ns.addFolder(park, { browseName: h.hall_id, displayName: h.name, nodeId: `s=Hall.${h.hall_id}` }));
  }

  const timers = new Set<NodeJS.Timeout>();
  const meters = new Map<string, MeterModel>();
  const startMs = Date.now();

  for (const m of a.park.machines) {
    const machineObj = ns.addObject({ organizedBy: halls.get(m.hall_id)!, browseName: m.machine_id, nodeId: `s=${m.machine_id}` });
    const meterObj = ns.addObject({ componentOf: machineObj, browseName: m.meter_id, nodeId: `s=${m.meter_id}` });
    const state = stringVariable(ns, machineObj, 'State', nodeIds.state(m.machine_id));
    const jobId = stringVariable(ns, machineObj, 'JobId', nodeIds.jobId(m.machine_id));
    const vars = Object.fromEntries(
      (Object.keys(METER_VARIABLES) as MeterVariable[]).map((v) => [v, doubleVariable(ns, meterObj, v, nodeIds.meter(m.meter_id, v))]),
    ) as Record<MeterVariable, UAVariable>;

    const model = new MachineModel(m.machine_id, a, startMs, { durationScale: opts.durationScale ?? 1 });
    const meter = new MeterModel(m.meter_id, a, startMs);
    meters.set(m.meter_id, meter);
    const interval = Rng.derive(a.seed, `${m.machine_id}/live-interval`);
    let prevMs = startMs;
    let prevState: MachineState | null = null;

    const tick = () => {
      const t = Date.now();
      meter.advance(t, model.energyKwhBetween(prevMs, t));
      prevMs = t;
      const seg = model.segmentAt(t);
      if (seg.state !== prevState) {
        opts.onStateChange?.(m.machine_id, prevState, seg.state, seg.jobId);
        prevState = seg.state;
      }
      const r = meter.read(t, model.powerAt(t));
      if (r) {
        const ts = new Date(t);
        const status = STATUS[r.opc_status];
        state.setValueFromSource({ dataType: DataType.String, value: seg.state }, StatusCodes.Good, ts);
        jobId.setValueFromSource({ dataType: DataType.String, value: seg.jobId }, StatusCodes.Good, ts);
        for (const [v, field] of Object.entries(METER_VARIABLES) as [MeterVariable, keyof typeof r][]) {
          vars[v].setValueFromSource({ dataType: DataType.Double, value: r[field] }, status, ts);
        }
      }
      const timer = setTimeout(tick, interval.uniform(a.volumes.live_interval_s) * 1000);
      timers.add(timer);
      timer.unref?.();
    };
    tick();
  }

  await server.start();
  return {
    endpointUrl: server.getEndpointUrl(),
    replaceMeter(meterId) {
      const meter = meters.get(meterId);
      if (!meter) throw new Error(`unknown meter ${meterId}`);
      meter.replace();
    },
    async stop() {
      for (const t of timers) clearTimeout(t);
      await server.shutdown(0);
    },
  };
}

function stringVariable(ns: Namespace, parent: UAObject, browseName: string, id: string): UAVariable {
  return ns.addVariable({ componentOf: parent, browseName, nodeId: `s=${id}`, dataType: 'String', minimumSamplingInterval: 1000 });
}

function doubleVariable(ns: Namespace, parent: UAObject, browseName: string, id: string): UAVariable {
  return ns.addVariable({ componentOf: parent, browseName, nodeId: `s=${id}`, dataType: 'Double', minimumSamplingInterval: 1000 });
}
