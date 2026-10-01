import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import type { MeterEvent } from '@am-energy/shared';

/** Omit that keeps a union a union (one member per schema version). */
export type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;

/** An event as stored at the edge; ts_edge is stamped when it is actually sent. */
export type PendingEvent = DistributiveOmit<MeterEvent, 'ts_edge'>;

export interface BufferedRow {
  id: number;
  event: PendingEvent;
}

/**
 * Store-and-forward buffer on SQLite. Events survive gateway restarts and network outages, and the per-meter sequence
 * number is persisted with them, so a resend after a lost acknowledgement is recognisable downstream as a duplicate.
 */
export class StoreAndForwardBuffer {
  private readonly db: DatabaseSync;

  constructor(path: string) {
    if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS meter_seq (meter_id TEXT PRIMARY KEY, next_seq INTEGER NOT NULL);
    `);
  }

  /** Assigns the next sequence number of the meter and stores the event, in one transaction. */
  enqueue(event: DistributiveOmit<PendingEvent, 'seq'>): PendingEvent {
    this.db.exec('BEGIN');
    try {
      const row = this.db.prepare('SELECT next_seq FROM meter_seq WHERE meter_id = ?').get(event.meter_id) as
        | { next_seq: number }
        | undefined;
      const seq = row?.next_seq ?? 0;
      this.db.prepare('INSERT OR REPLACE INTO meter_seq (meter_id, next_seq) VALUES (?, ?)').run(event.meter_id, seq + 1);
      const full = { ...event, seq } as PendingEvent;
      this.db.prepare('INSERT INTO events (payload) VALUES (?)').run(JSON.stringify(full));
      this.db.exec('COMMIT');
      return full;
    } catch (e) {
      this.db.exec('ROLLBACK');
      throw e;
    }
  }

  /** The newest events, returned oldest first. */
  newest(limit: number): BufferedRow[] {
    return this.rows('SELECT id, payload FROM events ORDER BY id DESC LIMIT ?', limit).reverse();
  }

  oldest(limit: number): BufferedRow[] {
    return this.rows('SELECT id, payload FROM events ORDER BY id ASC LIMIT ?', limit);
  }

  ack(ids: readonly number[]): void {
    if (ids.length === 0) return;
    this.db.prepare(`DELETE FROM events WHERE id IN (${ids.map(() => '?').join(',')})`).run(...ids);
  }

  size(): number {
    return (this.db.prepare('SELECT COUNT(*) AS n FROM events').get() as { n: number }).n;
  }

  close(): void {
    this.db.close();
  }

  private rows(sql: string, limit: number): BufferedRow[] {
    return (this.db.prepare(sql).all(limit) as { id: number; payload: string }[]).map((r) => ({
      id: r.id,
      event: JSON.parse(r.payload) as PendingEvent,
    }));
  }
}
