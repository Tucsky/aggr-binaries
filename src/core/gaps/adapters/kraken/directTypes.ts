import type { GapWindow, RecoveredTrade } from "../types.js";

export interface KrakenDirectRequest {
  symbol: string;
  windows: GapWindow[];
}

export interface KrakenDirectResult {
  trades: RecoveredTrade[];
  coverageEndTs?: number;
}

export interface KrakenDirectSource {
  recover(req: KrakenDirectRequest): Promise<KrakenDirectResult>;
}

export type KrakenFileSource = "full" | "quarterly";

export interface KrakenManifestFile {
  id: string;
  name: string;
  source: KrakenFileSource;
  sizeBytes: number;
  lastModifiedTs: number;
  quarterStartTs?: number;
  quarterEndTs?: number;
}

export interface KrakenManifest {
  version: number;
  refreshedDay: string;
  fullFileId: string;
  quarterlyFolderId: string;
  files: KrakenManifestFile[];
}

export interface KrakenSelectedFile extends KrakenManifestFile {
  rangeStartTs: number;
  rangeEndTs: number;
}
