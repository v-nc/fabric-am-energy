/** Contract between the OPC UA simulator and the gateway: namespace and node ids. */
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
export type MeterField = (typeof METER_VARIABLES)[MeterVariable];

/** String node ids in the simulator namespace: "LS01.State", "LS01.JobId", "EM01.ActivePower", … */
export const nodeIds = {
  state: (machineId: string) => `${machineId}.State`,
  jobId: (machineId: string) => `${machineId}.JobId`,
  meter: (meterId: string, variable: MeterVariable) => `${meterId}.${variable}`,
};
