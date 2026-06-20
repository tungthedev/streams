import { Result } from "better-result";
import { resolveEnabledTouchCapability } from "../profiles/index.ts";
import type { StreamProfileSpec } from "../profiles/profile.ts";
import type { LiveTemplateStoreRow, TouchProcessorStore } from "../store/touch_store.ts";
import type { ProcessResult } from "./worker_protocol.ts";
import {
  encodeTemplateArg,
  membershipKeyFor,
  membershipKeyIdFor,
  projectedFieldKeyFor,
  projectedFieldKeyIdFor,
  tableKeyIdFor,
  templateKeyIdFor,
  watchKeyFor,
  watchKeyIdFor,
  type TemplateEncoding,
} from "./live_keys.ts";

const decoder = new TextDecoder();

type ActiveTemplate = {
  templateId: string;
  entity: string;
  fields: string[];
  encodings: TemplateEncoding[];
  activeFromSourceOffset: bigint;
};

export type TouchProcessorBatchError = { kind: "missing_old_value"; message: string };

export type ProcessTouchBatchRequest = {
  db: TouchProcessorStore;
  stream: string;
  fromOffset: bigint;
  toOffset: bigint;
  profile: StreamProfileSpec;
  maxRows: number;
  maxBytes: number;
  emitFineTouches?: boolean;
  fineTouchBudget?: number | null;
  fineGranularity?: "key" | "template";
  processingMode?: "full" | "hotTemplatesOnly";
  filterHotTemplates?: boolean;
  hotTemplateIds?: string[] | null;
};

const VALID_TEMPLATE_ENCODINGS = new Set<string>(["string", "int64", "bool", "datetime", "bytes"]);

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function invalidLiveTemplateMetadata(message: string): Result<ActiveTemplate, TouchProcessorBatchError> {
  return Result.err({ kind: "missing_old_value", message: `invalid live template metadata: ${message}` });
}

function parseJsonArray(raw: unknown, field: string, templateId: string): Result<unknown[], TouchProcessorBatchError> {
  try {
    const value = JSON.parse(String(raw ?? "[]"));
    if (!Array.isArray(value)) {
      return Result.err({
        kind: "missing_old_value",
        message: `invalid live template metadata: ${field} must be an array (templateId=${templateId})`,
      });
    }
    return Result.ok(value);
  } catch {
    return Result.err({
      kind: "missing_old_value",
      message: `invalid live template metadata: ${field} must be valid JSON (templateId=${templateId})`,
    });
  }
}

function parseActiveTemplateRow(row: LiveTemplateStoreRow): Result<ActiveTemplate, TouchProcessorBatchError> {
  const templateId = String(row.template_id ?? "");
  if (!/^[0-9a-f]{16}$/.test(templateId)) {
    return invalidLiveTemplateMetadata(`template_id must be 16 lowercase hex characters (templateId=${templateId || "<empty>"})`);
  }

  const entity = String(row.entity ?? "");
  if (entity.trim() === "") {
    return invalidLiveTemplateMetadata(`entity must be non-empty (templateId=${templateId})`);
  }

  const fieldsRes = parseJsonArray(row.fields_json, "fields_json", templateId);
  if (Result.isError(fieldsRes)) return fieldsRes;
  const encodingsRes = parseJsonArray(row.encodings_json, "encodings_json", templateId);
  if (Result.isError(encodingsRes)) return encodingsRes;

  const rawFields = fieldsRes.value;
  const rawEncodings = encodingsRes.value;
  if (rawFields.length === 0 || rawFields.length > 3) {
    return invalidLiveTemplateMetadata(`fields_json must contain 1 to 3 fields (templateId=${templateId})`);
  }
  if (rawFields.length !== rawEncodings.length) {
    return invalidLiveTemplateMetadata(`fields_json and encodings_json lengths differ (templateId=${templateId})`);
  }

  const fields: string[] = [];
  const encodings: TemplateEncoding[] = [];
  for (let i = 0; i < rawFields.length; i++) {
    const field = rawFields[i];
    if (typeof field !== "string" || field.trim() === "") {
      return invalidLiveTemplateMetadata(`fields_json[${i}] must be a non-empty string (templateId=${templateId})`);
    }
    const encoding = rawEncodings[i];
    if (typeof encoding !== "string" || !VALID_TEMPLATE_ENCODINGS.has(encoding)) {
      return invalidLiveTemplateMetadata(`encodings_json[${i}] is not supported (templateId=${templateId})`);
    }
    fields.push(field);
    encodings.push(encoding as TemplateEncoding);
  }

  let activeFromSourceOffset: bigint;
  try {
    activeFromSourceOffset =
      typeof row.active_from_source_offset === "bigint" ? row.active_from_source_offset : BigInt(row.active_from_source_offset ?? 0);
  } catch {
    return invalidLiveTemplateMetadata(`active_from_source_offset must be a bigint-compatible value (templateId=${templateId})`);
  }

  return Result.ok({ templateId, entity, fields, encodings, activeFromSourceOffset });
}

function isProjectedFieldValue(value: unknown): boolean {
  return (
    value === undefined ||
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    typeof value === "bigint" ||
    (typeof value === "number" && Number.isFinite(value))
  );
}

function projectedFieldValueEquals(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  return typeof a === "number" && typeof b === "number" && Number.isNaN(a) && Number.isNaN(b);
}

function changedProjectedFieldNames(args: {
  before: Record<string, unknown>;
  after: Record<string, unknown>;
  excluded: ReadonlySet<string>;
}): string[] {
  const names = new Set<string>([...Object.keys(args.before), ...Object.keys(args.after)]);
  const out: string[] = [];
  for (const name of names) {
    if (args.excluded.has(name)) continue;
    const beforeValue = Object.prototype.hasOwnProperty.call(args.before, name) ? args.before[name] : undefined;
    const afterValue = Object.prototype.hasOwnProperty.call(args.after, name) ? args.after[name] : undefined;
    if (!isProjectedFieldValue(beforeValue) || !isProjectedFieldValue(afterValue)) continue;
    if (!projectedFieldValueEquals(beforeValue, afterValue)) out.push(name);
  }
  return out;
}

function projectedFieldNamesFromAfter(args: { after: Record<string, unknown>; excluded: ReadonlySet<string> }): string[] {
  const out: string[] = [];
  for (const name of Object.keys(args.after)) {
    if (args.excluded.has(name)) continue;
    if (!isProjectedFieldValue(args.after[name])) continue;
    out.push(name);
  }
  return out;
}

export async function processTouchBatch(req: ProcessTouchBatchRequest): Promise<Result<ProcessResult, TouchProcessorBatchError>> {
  const { db, stream, fromOffset, toOffset, profile, maxRows, maxBytes } = req;
  const failProcess = (message: string): Result<ProcessResult, TouchProcessorBatchError> => {
    return Result.err({ kind: "missing_old_value", message });
  };
  const enabledTouch = resolveEnabledTouchCapability(profile);
  if (!enabledTouch) return failProcess("touch not enabled for profile");

  const { capability: touchCapability, touchCfg: touch } = enabledTouch;
  const fineBudgetRaw = req.fineTouchBudget ?? touch.fineTouchBudgetPerBatch;
  const fineBudget = fineBudgetRaw == null ? null : Math.max(0, Math.floor(fineBudgetRaw));
  const fineGranularity = req.fineGranularity === "template" ? "template" : "key";
  const processingMode = req.processingMode === "hotTemplatesOnly" ? "hotTemplatesOnly" : "full";
  const hotTemplatesOnly = fineGranularity === "template" && processingMode === "hotTemplatesOnly";

  const emitFineTouches = req.emitFineTouches !== false && fineBudget !== 0;
  let fineBudgetExhausted = fineBudget != null && fineBudget <= 0;
  let fineKeysBudgetRemaining = fineBudget;
  let fineTouchesSuppressedDueToBudget = false;
  const filterHotTemplates = req.filterHotTemplates === true;
  const hotTemplateIdsRaw = filterHotTemplates ? req.hotTemplateIds ?? [] : [];
  const hotTemplateIds = filterHotTemplates ? new Set(hotTemplateIdsRaw.filter((x): x is string => typeof x === "string" && /^[0-9a-f]{16}$/.test(x))) : null;

  const coarseIntervalMs = Math.max(1, Math.floor(touch.coarseIntervalMs ?? 100));
  const coalesceWindowMs = Math.max(1, Math.floor(touch.touchCoalesceWindowMs ?? 100));
  const onMissingBefore = touch.onMissingBefore ?? "coarse";

  const templatesByEntity = new Map<string, ActiveTemplate[]>();
  const coldTemplateCountByEntity = new Map<string, number>();
  if (emitFineTouches) {
    try {
      const rows = await db.listActiveLiveTemplates(stream);
      for (const row of rows) {
        const tplRes = parseActiveTemplateRow(row);
        if (Result.isError(tplRes)) return tplRes;
        const tpl = tplRes.value;
        if (hotTemplateIds && !hotTemplateIds.has(tpl.templateId)) {
          coldTemplateCountByEntity.set(tpl.entity, (coldTemplateCountByEntity.get(tpl.entity) ?? 0) + 1);
          continue;
        }
        const arr = templatesByEntity.get(tpl.entity) ?? [];
        arr.push(tpl);
        templatesByEntity.set(tpl.entity, arr);
      }
    } catch (error) {
      return failProcess(`failed to load active live templates: ${String((error as { message?: unknown })?.message ?? error)}`);
    }
  }

  let rowsRead = 0;
  let bytesRead = 0;
  let changes = 0;
  let maxSourceTsMs = 0;
  let processedThrough = fromOffset - 1n;

  type PendingTouch = {
    keyId: number;
    routingKey?: string;
    windowStartMs: number;
    watermark: string;
    entity: string;
    kind: "table" | "template";
    templateId?: string;
  };
  type EntityTemplateOnlyTouch = { offset: bigint; tsMs: number; watermark: string };

  const pending = new Map<string, PendingTouch>();
  const templateOnlyEntityTouch = new Map<string, EntityTemplateOnlyTouch>();
  const touches: Array<{ keyId: number; routingKey?: string; watermark: string; entity: string; kind: "table" | "template"; templateId?: string }> = [];
  let fineTouchesDroppedDueToBudget = 0;
  let fineTouchesSkippedColdTemplate = 0;

  const flush = (_mapKey: string, p: PendingTouch): void => {
    touches.push({
      keyId: p.keyId >>> 0,
      routingKey: p.routingKey,
      watermark: p.watermark,
      entity: p.entity,
      kind: p.kind,
      templateId: p.templateId,
    });
  };

  const queueTouch = (args: {
    keyId: number;
    routingKey?: string;
    tsMs: number;
    watermark: string;
    entity: string;
    kind: "table" | "template";
    templateId?: string;
    windowMs: number;
  }): void => {
    const mapKey = args.routingKey ? `r:${args.routingKey}` : `i:${args.keyId >>> 0}`;
    const prev = pending.get(mapKey);
    if (args.kind !== "table" && fineBudget != null && !fineBudgetExhausted && !prev) {
      const remaining = fineKeysBudgetRemaining ?? 0;
      if (remaining <= 0) {
        fineBudgetExhausted = true;
        fineTouchesSuppressedDueToBudget = true;
        fineTouchesDroppedDueToBudget += 1;
        return;
      }
      fineKeysBudgetRemaining = remaining - 1;
    } else if (args.kind !== "table" && fineBudget != null && !prev && fineBudgetExhausted) {
      fineTouchesSuppressedDueToBudget = true;
      fineTouchesDroppedDueToBudget += 1;
      return;
    }

    if (!prev) {
      pending.set(mapKey, {
        keyId: args.keyId >>> 0,
        routingKey: args.routingKey,
        windowStartMs: args.tsMs,
        watermark: args.watermark,
        entity: args.entity,
        kind: args.kind,
        templateId: args.templateId,
      });
      return;
    }
    if (args.tsMs - prev.windowStartMs < args.windowMs) {
      prev.watermark = args.watermark;
      return;
    }
    flush(mapKey, prev);
    pending.set(mapKey, {
      keyId: args.keyId >>> 0,
      routingKey: args.routingKey,
      windowStartMs: args.tsMs,
      watermark: args.watermark,
      entity: args.entity,
      kind: args.kind,
      templateId: args.templateId,
    });
  };

  for await (const row of db.readWalRange(stream, fromOffset, toOffset)) {
    const payload = row.payload as Uint8Array;
    const payloadLen = payload.byteLength;
    if (rowsRead > 0 && (rowsRead >= maxRows || bytesRead + payloadLen > maxBytes)) break;

    rowsRead++;
    bytesRead += payloadLen;
    const offset = typeof row.offset === "bigint" ? (row.offset as bigint) : BigInt(row.offset);
    processedThrough = offset;
    const tsMsRaw = row.tsMs;
    const tsMs = typeof tsMsRaw === "bigint" ? Number(tsMsRaw) : Number(tsMsRaw);
    if (!Number.isFinite(tsMs)) continue;
    if (tsMs > maxSourceTsMs) maxSourceTsMs = tsMs;

    let value: any;
    try {
      value = JSON.parse(decoder.decode(payload));
    } catch {
      continue;
    }

    const canonical = touchCapability.deriveCanonicalChanges(value, profile);
    changes += canonical.length;
    if (canonical.length === 0) continue;
    const watermark = offset.toString();

    for (const ch of canonical) {
      const entity = ch.entity;
      queueTouch({
        keyId: tableKeyIdFor(entity),
        tsMs,
        watermark,
        entity,
        kind: "table",
        windowMs: coarseIntervalMs,
      });

      if (!emitFineTouches) continue;
      if (fineBudgetExhausted) continue;

      const tpls = templatesByEntity.get(entity);
      if (filterHotTemplates) fineTouchesSkippedColdTemplate += coldTemplateCountByEntity.get(entity) ?? 0;
      if (!tpls || tpls.length === 0) continue;

      if (hotTemplatesOnly) {
        const prev = templateOnlyEntityTouch.get(entity);
        if (!prev || offset > prev.offset) templateOnlyEntityTouch.set(entity, { offset, tsMs, watermark });
        continue;
      }

      for (const tpl of tpls) {
        if (fineBudgetExhausted) break;
        if (offset < tpl.activeFromSourceOffset) continue;

        if (fineGranularity === "template") {
          queueTouch({
            keyId: templateKeyIdFor(tpl.templateId) >>> 0,
            tsMs,
            watermark,
            entity,
            kind: "template",
            templateId: tpl.templateId,
            windowMs: coalesceWindowMs,
          });
          if (fineBudgetExhausted) break;
          continue;
        }

        const afterObj = ch.after;
        const beforeObj = ch.before;

        const watchKeys = new Map<number, string>();
        const membershipKeys = new Map<number, string>();
        const projectedFieldKeys = new Map<number, string>();

        const computeArgs = (obj: unknown): string[] | null => {
          if (!obj || typeof obj !== "object" || Array.isArray(obj)) return null;
          const args: string[] = [];
          for (let i = 0; i < tpl.fields.length; i++) {
            const name = tpl.fields[i];
            const enc = tpl.encodings[i];
            const v = (obj as any)[name];
            const encoded = encodeTemplateArg(v, enc);
            if (encoded == null) return null;
            args.push(encoded);
          }
          return args;
        };
        const computeWatch = (args: string[]): { keyId: number; routingKey: string } => {
          const routingKey = watchKeyFor(tpl.templateId, args);
          return { keyId: watchKeyIdFor(tpl.templateId, args) >>> 0, routingKey };
        };
        const computeMembership = (args: string[]): { keyId: number; routingKey: string } => {
          const routingKey = membershipKeyFor(tpl.templateId, args);
          return { keyId: membershipKeyIdFor(tpl.templateId, args) >>> 0, routingKey };
        };
        const computeProjectedField = (fieldName: string, args: string[]): { keyId: number; routingKey: string } => {
          const routingKey = projectedFieldKeyFor(tpl.templateId, fieldName, args);
          return { keyId: projectedFieldKeyIdFor(tpl.templateId, fieldName, args) >>> 0, routingKey };
        };
        const afterArgs = computeArgs(afterObj);
        const beforeArgs = computeArgs(beforeObj);
        const watchAfter = afterArgs != null ? computeWatch(afterArgs) : null;
        const watchBefore = beforeArgs != null ? computeWatch(beforeArgs) : null;
        const membershipAfter = afterArgs != null ? computeMembership(afterArgs) : null;
        const membershipBefore = beforeArgs != null ? computeMembership(beforeArgs) : null;
        const sameTuple = watchBefore != null && watchAfter != null && watchBefore.routingKey === watchAfter.routingKey;
        const excludedProjectedFields = new Set(tpl.fields);

        if (ch.op === "insert") {
          if (watchAfter != null) watchKeys.set(watchAfter.keyId >>> 0, watchAfter.routingKey);
          if (membershipAfter != null) membershipKeys.set(membershipAfter.keyId >>> 0, membershipAfter.routingKey);
        } else if (ch.op === "delete") {
          if (watchBefore != null) watchKeys.set(watchBefore.keyId >>> 0, watchBefore.routingKey);
          if (membershipBefore != null) membershipKeys.set(membershipBefore.keyId >>> 0, membershipBefore.routingKey);
        } else {
          if (watchBefore != null) {
            watchKeys.set(watchBefore.keyId >>> 0, watchBefore.routingKey);
            if (watchAfter != null) watchKeys.set(watchAfter.keyId >>> 0, watchAfter.routingKey);
            if (membershipBefore != null && membershipAfter != null) {
              if (membershipBefore.routingKey !== membershipAfter.routingKey) {
                membershipKeys.set(membershipBefore.keyId >>> 0, membershipBefore.routingKey);
                membershipKeys.set(membershipAfter.keyId >>> 0, membershipAfter.routingKey);
              } else if (sameTuple && isPlainObject(beforeObj) && isPlainObject(afterObj) && afterArgs != null) {
                for (const fieldName of changedProjectedFieldNames({
                  before: beforeObj,
                  after: afterObj,
                  excluded: excludedProjectedFields,
                })) {
                  const projected = computeProjectedField(fieldName, afterArgs);
                  projectedFieldKeys.set(projected.keyId >>> 0, projected.routingKey);
                }
              }
            } else {
              if (membershipBefore != null) membershipKeys.set(membershipBefore.keyId >>> 0, membershipBefore.routingKey);
              if (membershipAfter != null) membershipKeys.set(membershipAfter.keyId >>> 0, membershipAfter.routingKey);
            }
          } else {
            if (beforeObj === undefined) {
              if (onMissingBefore === "error") {
                return failProcess(`missing old_value for update (entity=${entity}, templateId=${tpl.templateId})`);
              }
            } else if (onMissingBefore === "error") {
              return failProcess(`old_value missing required fields for update (entity=${entity}, templateId=${tpl.templateId})`);
            }

            if (onMissingBefore === "skipBefore") {
              if (watchAfter != null) watchKeys.set(watchAfter.keyId >>> 0, watchAfter.routingKey);
              if (membershipAfter != null) membershipKeys.set(membershipAfter.keyId >>> 0, membershipAfter.routingKey);
              if (afterArgs != null && isPlainObject(afterObj)) {
                for (const fieldName of projectedFieldNamesFromAfter({
                  after: afterObj,
                  excluded: excludedProjectedFields,
                })) {
                  const projected = computeProjectedField(fieldName, afterArgs);
                  projectedFieldKeys.set(projected.keyId >>> 0, projected.routingKey);
                }
              }
            }
          }
        }

        for (const [watchKeyId, routingKey] of watchKeys) {
          queueTouch({
            keyId: watchKeyId >>> 0,
            routingKey,
            tsMs,
            watermark,
            entity,
            kind: "template",
            templateId: tpl.templateId,
            windowMs: coalesceWindowMs,
          });
          if (fineBudgetExhausted) break;
        }
        for (const [membershipKeyId, routingKey] of membershipKeys) {
          queueTouch({
            keyId: membershipKeyId >>> 0,
            routingKey,
            tsMs,
            watermark,
            entity,
            kind: "template",
            templateId: tpl.templateId,
            windowMs: coalesceWindowMs,
          });
          if (fineBudgetExhausted) break;
        }
        for (const [projectedFieldKeyId, routingKey] of projectedFieldKeys) {
          queueTouch({
            keyId: projectedFieldKeyId >>> 0,
            routingKey,
            tsMs,
            watermark,
            entity,
            kind: "template",
            templateId: tpl.templateId,
            windowMs: coalesceWindowMs,
          });
          if (fineBudgetExhausted) break;
        }
      }
    }
  }

  if (emitFineTouches && hotTemplatesOnly && !fineBudgetExhausted && templateOnlyEntityTouch.size > 0) {
    for (const [entity, t] of templateOnlyEntityTouch.entries()) {
      if (fineBudgetExhausted) break;
      const tpls = templatesByEntity.get(entity) ?? [];
      for (const tpl of tpls) {
        if (fineBudgetExhausted) break;
        if (t.offset < tpl.activeFromSourceOffset) continue;
        queueTouch({
          keyId: templateKeyIdFor(tpl.templateId) >>> 0,
          tsMs: t.tsMs,
          watermark: t.watermark,
          entity,
          kind: "template",
          templateId: tpl.templateId,
          windowMs: coalesceWindowMs,
        });
      }
    }
  }

  for (const [k, p] of pending) flush(k, p);

  touches.sort((a, b) => {
    const ak = a.keyId >>> 0;
    const bk = b.keyId >>> 0;
    if (ak < bk) return -1;
    if (ak > bk) return 1;
    const ar = a.routingKey ?? "";
    const br = b.routingKey ?? "";
    if (ar < br) return -1;
    if (ar > br) return 1;
    const aw = BigInt(a.watermark);
    const bw = BigInt(b.watermark);
    if (aw < bw) return -1;
    if (aw > bw) return 1;
    return 0;
  });

  let tableTouchesEmitted = 0;
  let templateTouchesEmitted = 0;
  for (const t of touches) {
    if (t.kind === "table") tableTouchesEmitted++;
    else templateTouchesEmitted++;
  }

  return Result.ok({
    type: "result",
    id: 0,
    stream,
    processedThrough,
    touches,
    stats: {
      rowsRead,
      bytesRead,
      changes,
      touchesEmitted: touches.length,
      tableTouchesEmitted,
      templateTouchesEmitted,
      maxSourceTsMs,
      fineTouchesDroppedDueToBudget,
      fineTouchesSuppressedDueToBudget,
      fineTouchesSkippedColdTemplate,
    },
  });
}
