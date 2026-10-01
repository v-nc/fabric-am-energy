import { MeterEvent } from '@am-energy/shared';
import type { BufferedRow, StoreAndForwardBuffer } from './buffer.ts';
import type { Sink } from './sinks.ts';

export interface ForwarderOptions {
  /** Newest events sent per cycle. Live data goes first. */
  freshBatch: number;
  /** Oldest events sent per cycle while a backlog exists. */
  backlogBatch: number;
  intervalMs: number;
  log?: (msg: string) => void;
  now?: () => number;
}

export interface ForwarderStats {
  sent: number;
  failedCycles: number;
  invalidDropped: number;
}

/**
 * Drains the buffer to the sink. Each cycle first sends the newest events, then one batch of the oldest. After an
 * outage that means live data is current again at once while the backlog is replayed behind it, which is why events
 * reach Fabric late and out of order: the pipeline has to cope with that, and does it with seq and ts_source.
 */
export class Forwarder {
  readonly stats: ForwarderStats = { sent: 0, failedCycles: 0, invalidDropped: 0 };
  private timer: NodeJS.Timeout | undefined;
  private failing = false;
  private readonly log: (msg: string) => void;
  private readonly now: () => number;

  constructor(
    private readonly buffer: StoreAndForwardBuffer,
    private readonly sink: Sink,
    private readonly opts: ForwarderOptions,
  ) {
    this.log = opts.log ?? (() => {});
    this.now = opts.now ?? Date.now;
  }

  start(): void {
    const loop = async () => {
      await this.flushOnce();
      this.timer = setTimeout(loop, this.opts.intervalMs);
    };
    this.timer = setTimeout(loop, this.opts.intervalMs);
  }

  stop(): void {
    clearTimeout(this.timer);
  }

  /** One cycle. Returns the number of events sent. */
  async flushOnce(): Promise<number> {
    const fresh = this.buffer.newest(this.opts.freshBatch);
    if (fresh.length === 0) return 0;
    let sent = await this.sendRows(fresh);
    if (sent > 0 && this.buffer.size() > 0) sent += await this.sendRows(this.buffer.oldest(this.opts.backlogBatch));
    return sent;
  }

  private async sendRows(rows: BufferedRow[]): Promise<number> {
    const tsEdge = new Date(this.now()).toISOString();
    const events: MeterEvent[] = [];
    const invalid: number[] = [];
    for (const r of rows) {
      const parsed = MeterEvent.safeParse({ ...r.event, ts_edge: tsEdge });
      if (parsed.success) events.push(parsed.data);
      else invalid.push(r.id);
    }
    if (invalid.length > 0) {
      this.buffer.ack(invalid);
      this.stats.invalidDropped += invalid.length;
      this.log(`dropped ${invalid.length} events that fail the schema`);
    }
    try {
      await this.sink.send(events);
    } catch (e) {
      this.stats.failedCycles++;
      if (!this.failing) this.log(`send failed, buffering: ${(e as Error).message}`);
      this.failing = true;
      return 0;
    }
    if (this.failing) this.log(`send recovered, ${this.buffer.size()} events buffered`);
    this.failing = false;
    this.buffer.ack(rows.map((r) => r.id));
    this.stats.sent += events.length;
    return events.length;
  }
}
