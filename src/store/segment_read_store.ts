import type { SearchCompanionPlanRow, SearchSegmentCompanionRow, SegmentRow, StreamRow } from "./rows";

export type StreamReadRow = StreamRow;
export type SegmentReadRow = SegmentRow;
export type SearchCompanionPlanReadRow = SearchCompanionPlanRow;
export type SearchSegmentCompanionReadRow = SearchSegmentCompanionRow;

export interface StreamReadStore {
  nowMsForRead(): Promise<bigint>;
  getStreamForRead(stream: string): Promise<StreamReadRow | null>;
  isDeleted(row: StreamReadRow): boolean;
}

export interface SegmentReadStore {
  listSegmentsForRead(stream: string): Promise<SegmentReadRow[]>;
  getSegmentByIndexForRead(stream: string, segmentIndex: number): Promise<SegmentReadRow | null>;
  findSegmentForOffsetForRead(stream: string, offset: bigint): Promise<SegmentReadRow | null>;
  countSegmentsForRead(stream: string): Promise<number>;
  getSearchCompanionPlanForRead(stream: string): Promise<SearchCompanionPlanReadRow | null>;
  listSearchSegmentCompanionsForRead(stream: string): Promise<SearchSegmentCompanionReadRow[]>;
  getSearchSegmentCompanionForRead(stream: string, segmentIndex: number): Promise<SearchSegmentCompanionReadRow | null>;
}
