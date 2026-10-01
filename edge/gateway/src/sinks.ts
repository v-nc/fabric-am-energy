import { appendFile, mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { EventHubProducerClient } from '@azure/event-hubs';
import { Rng, type MeterEvent } from '@am-energy/shared';

export interface Sink {
  send(events: readonly MeterEvent[]): Promise<void>;
  close(): Promise<void>;
}

/**
 * Fabric Eventstream custom endpoint, spoken over the Event Hubs protocol. The partition key is the meter id, so the
 * events of one meter stay in order inside one partition.
 */
export class EventHubsSink implements Sink {
  private readonly producer: EventHubProducerClient;

  constructor(connectionString: string) {
    this.producer = new EventHubProducerClient(connectionString);
  }

  async send(events: readonly MeterEvent[]): Promise<void> {
    const byMeter = Map.groupBy(events, (e) => e.meter_id);
    for (const [meterId, group] of byMeter) {
      let batch = await this.producer.createBatch({ partitionKey: meterId });
      for (const e of group) {
        const data = { body: e, contentType: 'application/json' };
        if (!batch.tryAdd(data)) {
          await this.producer.sendBatch(batch);
          batch = await this.producer.createBatch({ partitionKey: meterId });
          if (!batch.tryAdd(data)) throw new Error(`event too large for one batch: ${meterId}/${e.seq}`);
        }
      }
      if (batch.count > 0) await this.producer.sendBatch(batch);
    }
  }

  close(): Promise<void> {
    return this.producer.close();
  }
}

/** JSON lines per UTC day, for working without Fabric. */
export class FileSink implements Sink {
  constructor(private readonly dir: string) {}

  async send(events: readonly MeterEvent[]): Promise<void> {
    await mkdir(this.dir, { recursive: true });
    const day = new Date().toISOString().slice(0, 10);
    await appendFile(join(this.dir, `events-${day}.jsonl`), events.map((e) => JSON.stringify(e)).join('\n') + '\n');
  }

  async close(): Promise<void> {}
}

export class MemorySink implements Sink {
  readonly received: MeterEvent[] = [];

  async send(events: readonly MeterEvent[]): Promise<void> {
    this.received.push(...events);
  }

  async close(): Promise<void> {}
}

/**
 * Wraps a sink with the network faults of the lab: an outage makes every send fail, and a lost acknowledgement makes
 * the gateway send some events a second time.
 */
export class FaultySink implements Sink {
  outage = false;

  constructor(
    private readonly inner: Sink,
    private readonly duplicateProbability: number,
    private readonly rng: Rng,
  ) {}

  async send(events: readonly MeterEvent[]): Promise<void> {
    if (this.outage) throw new Error('simulated network outage');
    await this.inner.send(events);
    const resent = events.filter(() => this.rng.chance(this.duplicateProbability));
    if (resent.length > 0) await this.inner.send(resent);
  }

  close(): Promise<void> {
    return this.inner.close();
  }
}
