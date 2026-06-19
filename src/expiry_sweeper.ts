import type { Config } from "./config";
import type { StreamStore } from "./store/capabilities";

export class ExpirySweeper {
  private readonly cfg: Config;
  private readonly store: StreamStore;
  private timer: any | null = null;
  private tickPromise: Promise<void> | null = null;

  constructor(cfg: Config, store: StreamStore) {
    this.cfg = cfg;
    this.store = store;
  }

  start(): void {
    if (this.timer || this.cfg.expirySweepIntervalMs <= 0) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.cfg.expirySweepIntervalMs);
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    await this.tickPromise;
  }

  private async tick(): Promise<void> {
    if (this.tickPromise) return this.tickPromise;
    this.tickPromise = this.runTick().finally(() => {
      this.tickPromise = null;
    });
    return this.tickPromise;
  }

  private async runTick(): Promise<void> {
    const expired = await this.store.listExpiredStreams(this.cfg.expirySweepBatchLimit);
    if (expired.length === 0) return;
    for (const stream of expired) {
      try {
        await this.store.deleteStream(stream);
      } catch {
        // ignore deletion errors
      }
    }
  }
}
