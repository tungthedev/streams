import { SqliteDurableStore } from "./db";
import type { BootstrapRestoreStore } from "../store/bootstrap_restore_store";

export function createSqliteBootstrapRestoreStore(
  dbPath: string,
  opts: { cacheBytes?: number } = {}
): BootstrapRestoreStore {
  return new SqliteDurableStore(dbPath, opts);
}
