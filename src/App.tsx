import { useEffect, useMemo, useState } from "react";
import mondaySdk from "monday-sdk-js";
import * as XLSX from "xlsx";

const monday = mondaySdk();
const COLUMN_ID = "color_mksw618w";
const MARCOMMS_BOARD_ID = "8440693148";
const STEP_DELAY_MS = 120;
const APP_VERSION = "1.2.6";
const UPDATE_CONCURRENCY = 3;
const UPDATE_DELAY_MS = 40;
const UPDATE_RETRY_LIMIT = 2;
const UPDATE_RETRY_BACKOFF_MS = 250;
const DEPLOY_CONCURRENCY = 6;
const FETCH_CURSOR_MAX_PAGES = 80;
const EXTERNAL_FETCH_TIMEOUT_MS = 12000;
const MONDAY_API_TIMEOUT_MS = 25000;
const TRAILER_RUN_TIMEOUT_MS = 300000;
const TRAILER_TEST_GROUP_LIMIT = 10;
const COL_CONTENT_TYPE = "status_1_mkn3yyv4";
const COL_FOREIGN_TITLE = "text_mks31sjy";
const COL_SEASON_YEAR_ALBUM = "text_mksd2s7y";
const COL_TRAILER_LINK = "link_mks3yxj3";
const COL_IMDB_LINK = "link_mm0wf2nf";
const COL_SUGGESTED_IMAGE_FILE = "file_mkszyxdh";
const COL_CYCLE = "text_mkxga9d";
const COL_CYCLE_EXPIRED = "text_mm0pw9kx";
const COL_CYCLE_EXPIRED_FALLBACK = "lookup_mm0p6m5c";
const COL_CAT_PAC = "text_mkrzy59w";
const COL_CAT_THALES = "text_mkrz4kqf";
const COL_FLAG_EX3 = "boolean_mkrramxw";
const COL_FLAG_EX2 = "boolean_mkrra4nz";
const COL_FLAG_L3 = "boolean_mkrr1hwr";
const COL_FLAG_THALES = "boolean_mkrrpfvg";
const TMDB_API_KEY = "15565da9094b23c59adcd5f62435786d";
const YOUTUBE_API_KEY = "AIzaSyCRKi5aWBsGMujtb0u-HjtuXHrzHC7_txA";

type Scope = "selected" | "group" | "board";
type TrailerMode = "auto_mark_na" | "auto_only";
type TrailerChoice = "best_tmdb" | "alt_1" | "alt_2" | "youtube" | "no_trailer";
type Workflow = "home" | "align" | "pg" | "archive";
type Group = { id: string; title: string };
type Progress = { done: number; total: number; ok: number; failed: number };
type FailedUpdate = { itemId: string; message: string };
type BoardOption = { id: string; name: string };
type BoardColumn = { id: string; title: string; type?: string | null };
type MondayLinkedItem = { id?: string | number };
type MondayColumn = {
  id: string;
  text?: string | null;
  value?: unknown;
  display_value?: string | null;
  type?: string | null;
  linked_items?: MondayLinkedItem[];
};
type MondaySubitem = {
  id: string;
  name?: string | null;
  board?: { id?: string | number };
  column_values?: MondayColumn[];
};
type MondayItemWithSubitems = {
  id: string;
  name?: string | null;
  subitems?: MondaySubitem[];
};
type AlignScanStats = {
  sourceBoardId: string;
  parentItems: number;
  subitems: number;
  linkedRelations: number;
  uniqueLinkedItems: number;
};
type PgMappingRow = {
  header: string;
  mapped: boolean;
  columnId?: string;
  columnTitle?: string;
  columnType?: string | null;
  reason?: string;
};
type PgSheetCheckRow = {
  requiredSheet: string;
  matchedSheet: string;
  found: boolean;
  headerRow: number;
  note: string;
};
type PgHeaderVerifyRow = {
  requiredSheet: string;
  matchedSheet: string;
  headerRow: number;
  titleCols: string[];
  systemCols: Record<string, string>;
  cycleCol: string;
  cycleExpiredCol: string;
  categoryRange: [string, string];
  titleHeaders: string[];
  systemHeaderLabels: string[];
};
type PgHeaderMappingDetailRow = {
  requiredSheet: string;
  matchedSheet: string;
  mapping: string;
  columnFound: string;
  valueFound: string;
  note: string;
};
type PgSheetOverride = {
  headerRow?: number;
  titleCols?: [string, string];
  systemCols?: Partial<Record<"EX3" | "EX2" | "L3" | "Thales", string>>;
  cycleCol?: string;
  cycleExpiredCol?: string;
  categoryRange?: [string, string];
};
type MondayBoardItem = {
  id: string;
  name?: string | null;
  column_values?: Array<{ id: string; text?: string | null; value?: unknown }>;
};
type TrailerReviewRow = {
  itemId: string;
  itemName: string;
  searchTitle: string;
  translatedTitle: string;
  yearText: string;
  matchedOn: string;
  matchScore: number;
  bestTmdbUrl: string;
  bestLabel: string;
  alt1Url: string;
  alt1Label: string;
  alt2Url: string;
  alt2Label: string;
  youtubeUrl: string;
  youtubeLabel: string;
  imdbUrl: string;
  imdbLabel: string;
  posterUrl: string;
  confirmImdb: boolean;
  confirmImage: boolean;
  selectedChoice: TrailerChoice;
  status: "auto_applied" | "pending_review" | "applied" | "no_trailer" | "failed";
  note: string;
};
type PgDeployUpdate = {
  columnId: string;
  columnTitle: string;
  currentValue: string;
  newValue: string;
  source: string;
  type: "text" | "checkbox";
  value: string | { checked: "true" | "false" };
};
type PgDeployRow = {
  itemId: string;
  itemName: string;
  itemMeta: string;
  matchKey: string;
  matchedSheet: string;
  matchedRow: number;
  pacMatchedRow: number;
  thalesMatchedRow: number;
  system: string;
  status: "ready" | "no_match" | "ambiguous" | "no_changes" | "deployed" | "failed";
  reason: string;
  computed: {
    cycleAdded: string;
    cycleExpiring: string;
    pacCategories: string;
    thalesCategories: string;
    ex3: string;
    ex2: string;
    l3: string;
    thales: string;
  };
  updates: PgDeployUpdate[];
};
type PgDeploySummary = {
  inMarcomms: number;
  matched: number;
  ready: number;
  noMatch: number;
  ambiguous: number;
  noChanges: number;
};
type PgOverrideMappingKey =
  | "header_row"
  | "title_1"
  | "title_2"
  | "system_ex3"
  | "system_ex2"
  | "system_l3"
  | "system_thales"
  | "cycle"
  | "cycle_expired"
  | "cat_start"
  | "cat_end";

const REQUIRED_PG_SHEETS = [
  "Movies_PAC",
  "Movies_Thales",
  "TV_PAC",
  "TV_Thales",
  "Audio eX-Series_PAC",
  "Audio_Thales",
  "Audio S3Ki_PAC",
  "Emirates World_PAC",
  "Emirates World_Thales",
] as const;
type RequiredPgSheet = (typeof REQUIRED_PG_SHEETS)[number];

const TITLE_COLUMN_MAP: Record<RequiredPgSheet, string[]> = {
  "Audio eX-Series_PAC": ["F", "G"],
  "Audio_Thales": ["E", "F"],
  "Movies_PAC": ["E", "I"],
  "Movies_Thales": ["E", "I"],
  "TV_PAC": ["E", "G"],
  "TV_Thales": ["E", "G"],
  "Audio S3Ki_PAC": ["F", "G"],
  "Emirates World_PAC": ["D", "E"],
  "Emirates World_Thales": ["D", "E"],
};

const SYSTEM_COLUMN_MAP: Record<RequiredPgSheet, Record<string, string>> = {
  "Movies_PAC": { EX3: "CU", EX2: "CW", L3: "CY" },
  "TV_PAC": { EX3: "BL", EX2: "BN", L3: "BR" },
  "Audio eX-Series_PAC": { EX3: "CB", EX2: "CC" },
  "Movies_Thales": { Thales: "N" },
  "TV_Thales": { Thales: "S" },
  "Audio_Thales": { Thales: "D" },
  "Audio S3Ki_PAC": {},
  "Emirates World_PAC": { EX3: "K", EX2: "L", L3: "N" },
  "Emirates World_Thales": { Thales: "C" },
};

const START_DATE_COLUMN_MAP: Record<RequiredPgSheet, string> = {
  "Movies_PAC": "M",
  "Movies_Thales": "M",
  "TV_PAC": "P",
  "TV_Thales": "R",
  "Audio eX-Series_PAC": "D",
  "Audio_Thales": "C",
  "Audio S3Ki_PAC": "D",
  "Emirates World_PAC": "C",
  "Emirates World_Thales": "C",
};

const END_DATE_COLUMN_MAP: Record<RequiredPgSheet, string> = {
  "Movies_PAC": "N",
  "Movies_Thales": "N",
  "TV_PAC": "Q",
  "TV_Thales": "S",
  "Audio eX-Series_PAC": "E",
  "Audio_Thales": "D",
  "Audio S3Ki_PAC": "E",
  "Emirates World_PAC": "D",
  "Emirates World_Thales": "D",
};

const CATEGORY_RANGE_MAP: Record<RequiredPgSheet, [string, string]> = {
  "Movies_PAC": ["S", "CM"],
  "Movies_Thales": ["S", "GK"],
  "TV_PAC": ["T", "BJ"],
  "TV_Thales": ["Y", "DK"],
  "Audio eX-Series_PAC": ["P", "CA"],
  "Audio_Thales": ["AJ", "DQ"],
  "Audio S3Ki_PAC": ["P", "CA"],
  "Emirates World_PAC": ["F", "I"],
  "Emirates World_Thales": ["F", "I"],
};

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | null = null;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function qs(base: string, params: Record<string, string | number | undefined>) {
  const query = Object.entries(params)
    .filter(([, v]) => v !== undefined && v !== null && String(v).trim() !== "")
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
    .join("&");
  return query ? `${base}?${query}` : base;
}

function formatApiError(error: any, fallback = "Unknown API error"): string {
  const messageParts: string[] = [];
  const directMessage = error?.message ? String(error.message) : "";
  if (directMessage) messageParts.push(directMessage);

  const gqlErrors =
    error?.errors ??
    error?.response?.errors ??
    error?.data?.errors ??
    error?.response?.data?.errors ??
    [];

  if (Array.isArray(gqlErrors)) {
    for (const gqlError of gqlErrors) {
      const msg = gqlError?.message ? String(gqlError.message) : "";
      if (msg) messageParts.push(msg);
    }
  }

  const joined = Array.from(new Set(messageParts.filter(Boolean))).join(" | ");
  if (!joined) return fallback;

  if (joined.toLowerCase().includes("graphql validation errors")) {
    return `${joined}. Likely cause: item is not in board ${MARCOMMS_BOARD_ID} or permission mismatch.`;
  }

  return joined;
}

async function fetchGroups(boardId: number): Promise<Group[]> {
  const query = `
    query ($boardId: [ID!]) {
      boards(ids: $boardId) {
        groups {
          id
          title
        }
      }
    }
  `;

  const res = await monday.api(query, { variables: { boardId } });
  return res?.data?.boards?.[0]?.groups ?? [];
}

async function fetchAvailableBoards(): Promise<BoardOption[]> {
  const query = `
    query ($page: Int!, $limit: Int!) {
      boards(page: $page, limit: $limit) {
        id
        name
      }
    }
  `;

  const limit = 100;
  const maxPages = 15;
  const allBoards: BoardOption[] = [];

  for (let page = 1; page <= maxPages; page += 1) {
    const res = await monday.api(query, { variables: { page, limit } });
    const boards = (res?.data?.boards ?? []) as Array<{ id?: string | number; name?: string }>;
    if (!boards.length) break;

    for (const board of boards) {
      if (board?.id === undefined || board?.id === null) continue;
      allBoards.push({ id: String(board.id), name: String(board.name ?? "") });
    }

    if (boards.length < limit) break;
    await sleep(60);
  }

  const seen = new Set<string>();
  return allBoards.filter((board) => {
    if (seen.has(board.id)) return false;
    seen.add(board.id);
    return true;
  });
}

async function fetchBoardColumns(boardId: number): Promise<BoardColumn[]> {
  const query = `
    query ($boardId: [ID!]) {
      boards(ids: $boardId) {
        columns {
          id
          title
          type
        }
      }
    }
  `;

  const res = await monday.api(query, { variables: { boardId } });
  return (res?.data?.boards?.[0]?.columns ?? []) as BoardColumn[];
}

function parseHeaderColumnId(header: string): string | null {
  const match = header.match(/\[([a-zA-Z0-9_]+)\]\s*$/);
  return match?.[1] ?? null;
}

function normalizeSheetName(value: string): string {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function tokenSetScore(a: string, b: string): number {
  const aSet = new Set(normalizeSheetName(a).split(/\s+/).filter(Boolean));
  const bSet = new Set(normalizeSheetName(b).split(/\s+/).filter(Boolean));
  if (!aSet.size || !bSet.size) return 0;
  let shared = 0;
  for (const t of aSet) if (bSet.has(t)) shared += 1;
  return Math.round((2 * shared * 100) / (aSet.size + bSet.size));
}

function autoBestMatchSheet(required: string, available: string[]): string {
  if (available.includes(required)) return required;
  if (!available.length) return "";
  let best = available[0];
  let bestScore = -1;
  for (const name of available) {
    const score = tokenSetScore(required, name);
    if (score > bestScore) {
      best = name;
      bestScore = score;
    }
  }
  return best;
}

function findHeaderRowIndex(rows: any[][]): number {
  const maxScan = Math.min(rows.length, 30);
  for (let r = 0; r < maxScan; r += 1) {
    const row = rows[r] || [];
    let titleCount = 0;
    for (const cell of row) {
      if (String(cell ?? "").trim().toLowerCase() === "title") titleCount += 1;
    }
    if (titleCount === 1) return r;
  }
  for (let r = 0; r < maxScan; r += 1) {
    const row = rows[r] || [];
    if (row.some((cell) => String(cell ?? "").trim() !== "")) return r;
  }
  return 0;
}

function colLetterToIndex(letter: string): number {
  let n = 0;
  for (let i = 0; i < letter.length; i += 1) {
    n = n * 26 + (letter.charCodeAt(i) - 64);
  }
  return n - 1;
}

function indexToColLetter(n: number): string {
  if (n < 0) return "";
  let s = "";
  n += 1;
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

function normHeader(x: any): string {
  return String(x ?? "").trim().toLowerCase();
}

function findHeaderExact(rows: any[][], headerIdx: number, name: string): number {
  const row = rows[headerIdx] || [];
  const target = normHeader(name);
  for (let c = 0; c < row.length; c += 1) {
    if (normHeader(row[c]) === target) return c;
  }
  return -1;
}

function findHeaderContains(rows: any[][], headerIdx: number, text: string, rightmost = false): number {
  const row = rows[headerIdx] || [];
  const target = normHeader(text);
  let found = -1;
  for (let c = 0; c < row.length; c += 1) {
    if (normHeader(row[c]).includes(target)) {
      if (!rightmost) return c;
      found = c;
    }
  }
  return found;
}

function findHeaderByCandidates(rows: any[][], headerIdx: number, candidates: string[]): number {
  for (const candidate of candidates) {
    const exact = findHeaderExact(rows, headerIdx, candidate);
    if (exact !== -1) return exact;
  }
  for (const candidate of candidates) {
    const contains = findHeaderContains(rows, headerIdx, candidate);
    if (contains !== -1) return contains;
  }
  return -1;
}

function autoSuggestTitleCols(key: RequiredPgSheet, rows: any[][], headerIdx: number): string[] {
  if (key.includes("Audio")) {
    const artist = findHeaderExact(rows, headerIdx, "Artist");
    const album = findHeaderExact(rows, headerIdx, "Album");
    const title = findHeaderExact(rows, headerIdx, "Title");
    if (artist !== -1 && album !== -1) return [indexToColLetter(artist), indexToColLetter(album)];
    if (artist !== -1 && title !== -1) return [indexToColLetter(artist), indexToColLetter(title)];
    return TITLE_COLUMN_MAP[key];
  }
  if (key.includes("Movies")) {
    const title = findHeaderExact(rows, headerIdx, "Title");
    const year = findHeaderExact(rows, headerIdx, "Year");
    const originalTitle = findHeaderExact(rows, headerIdx, "Original Title");
    if (title !== -1 && year !== -1) return [indexToColLetter(title), indexToColLetter(year)];
    if (originalTitle !== -1 && year !== -1) return [indexToColLetter(originalTitle), indexToColLetter(year)];
    return TITLE_COLUMN_MAP[key];
  }
  if (key.includes("TV")) {
    const title = findHeaderExact(rows, headerIdx, "Title");
    const season = findHeaderExact(rows, headerIdx, "Season");
    const series = findHeaderExact(rows, headerIdx, "Series");
    if (title !== -1 && season !== -1) return [indexToColLetter(title), indexToColLetter(season)];
    if (series !== -1 && season !== -1) return [indexToColLetter(series), indexToColLetter(season)];
    return TITLE_COLUMN_MAP[key];
  }
  if (key.includes("Emirates World")) {
    const artist = findHeaderExact(rows, headerIdx, "Artist");
    const title = findHeaderExact(rows, headerIdx, "Title");
    if (artist !== -1 && title !== -1) return [indexToColLetter(artist), indexToColLetter(title)];
    return TITLE_COLUMN_MAP[key];
  }
  return TITLE_COLUMN_MAP[key];
}

function getTitleFieldLabels(key: RequiredPgSheet): [string, string] {
  if (key.includes("Movies")) return ["Title", "Year"];
  if (key.includes("TV")) return ["Title", "Season No."];
  if (key.includes("Audio")) return ["Artist", "Album"];
  if (key.includes("Emirates World")) return ["Artist", "Title"];
  return ["Title 1", "Title 2"];
}

function autoSuggestSystemCols(key: RequiredPgSheet, rows: any[][], headerIdx: number): Record<string, string> {
  if (key === "Audio eX-Series_PAC") {
    const ex3 = findHeaderExact(rows, headerIdx, "ex3");
    const ex2 = findHeaderExact(rows, headerIdx, "ex2");
    const out: Record<string, string> = {};
    if (ex3 !== -1) out.EX3 = indexToColLetter(ex3);
    if (ex2 !== -1) out.EX2 = indexToColLetter(ex2);
    return Object.keys(out).length ? out : SYSTEM_COLUMN_MAP[key];
  }

  if (key === "Audio S3Ki_PAC") {
    // Presence-only sheet: title match implies L3 availability.
    return { L3: "-" };
  }

  if (key === "Movies_Thales" || key === "TV_Thales") {
    const end = findHeaderExact(rows, headerIdx, "End");
    if (end !== -1) return { Thales: indexToColLetter(end) };
    return SYSTEM_COLUMN_MAP[key];
  }

  const map = { ...SYSTEM_COLUMN_MAP[key] };

  const ex3Candidates = ["ex3 from", "e x3 from", "aod ex3", "ex3"];
  const ex2Candidates = ["ex2 from", "e x2 from", "aod ex2", "ex2"];
  const l3Candidates = ["3ki from", "l3 from", "x series from", "3ki", "l3", "x series"];
  const thalesCandidates = ["thales from", "thales", "thales file name"];

  const ex3 = findHeaderByCandidates(rows, headerIdx, ex3Candidates);
  const ex2 = findHeaderByCandidates(rows, headerIdx, ex2Candidates);
  const l3 = findHeaderByCandidates(rows, headerIdx, l3Candidates);
  const thales = findHeaderByCandidates(rows, headerIdx, thalesCandidates);

  // Movies: explicitly prefer "... From" headers where present.
  if (key.includes("Movies")) {
    const ex3From = findHeaderByCandidates(rows, headerIdx, ["ex3 from"]);
    const ex2From = findHeaderByCandidates(rows, headerIdx, ["ex2 from"]);
    const threeKiFrom = findHeaderByCandidates(rows, headerIdx, ["3ki from", "l3 from"]);
    if (ex3From !== -1) map.EX3 = indexToColLetter(ex3From);
    if (ex2From !== -1) map.EX2 = indexToColLetter(ex2From);
    if (threeKiFrom !== -1) map.L3 = indexToColLetter(threeKiFrom);
  }

  if (ex3 !== -1) map.EX3 = indexToColLetter(ex3);
  if (ex2 !== -1) map.EX2 = indexToColLetter(ex2);
  if (l3 !== -1) map.L3 = indexToColLetter(l3);
  if (thales !== -1) map.Thales = indexToColLetter(thales);
  return map;
}

function autoSuggestCycle(key: RequiredPgSheet, rows: any[][], headerIdx: number): string {
  if (key === "TV_Thales") {
    const start = findHeaderExact(rows, headerIdx, "Start");
    if (start !== -1) return indexToColLetter(start);
  }
  const startDate = findHeaderExact(rows, headerIdx, "Start Date");
  if (startDate !== -1) return indexToColLetter(startDate);
  const start = findHeaderExact(rows, headerIdx, "Start");
  if (start !== -1) return indexToColLetter(start);
  return START_DATE_COLUMN_MAP[key];
}

function autoSuggestCycleExpired(key: RequiredPgSheet, rows: any[][], headerIdx: number): string {
  const endDate = findHeaderExact(rows, headerIdx, "End Date");
  if (endDate !== -1) return indexToColLetter(endDate);
  const end = findHeaderExact(rows, headerIdx, "End");
  if (end !== -1) return indexToColLetter(end);
  return END_DATE_COLUMN_MAP[key];
}

function autoSuggestCategoryRange(key: RequiredPgSheet, rows: any[][], headerIdx: number): [string, string] {
  if (key.includes("Emirates World")) {
    return ["H", "H"];
  }

  if (key === "Audio eX-Series_PAC" || key === "Audio S3Ki_PAC") {
    const channelNos = findHeaderContains(rows, headerIdx, "channel nos", true);
    const weRecommend = findHeaderContains(rows, headerIdx, "we recommend", true);
    if (channelNos !== -1 && weRecommend !== -1 && weRecommend > channelNos + 1) {
      return [indexToColLetter(channelNos + 1), indexToColLetter(weRecommend - 1)];
    }
  }

  if (key === "Audio_Thales") {
    const top5 = findHeaderContains(rows, headerIdx, "top 5", true);
    const islandMode = findHeaderContains(rows, headerIdx, "island mode");
    if (top5 !== -1 && islandMode !== -1 && islandMode > top5 + 1) {
      return [indexToColLetter(top5 + 1), indexToColLetter(islandMode - 1)];
    }
  }

  if (key === "TV_PAC") {
    const subs = findHeaderExact(rows, headerIdx, "Subs");
    const firstFrom = findHeaderExact(rows, headerIdx, "From");
    if (subs !== -1 && firstFrom !== -1 && firstFrom > subs + 1) {
      return [indexToColLetter(subs + 1), indexToColLetter(firstFrom - 1)];
    }
  }

  const subs = findHeaderContains(rows, headerIdx, "subtitle");
  const weRecommend = findHeaderContains(rows, headerIdx, "we recommend", true);

  // Prefer ending at the column immediately before "We recommend...".
  if (weRecommend !== -1) {
    const end = Math.max(0, weRecommend - 1);
    if (subs !== -1 && subs + 1 <= end) {
      return [indexToColLetter(subs + 1), indexToColLetter(end)];
    }
    const defaultStart = colLetterToIndex(CATEGORY_RANGE_MAP[key][0]);
    if (defaultStart <= end) {
      return [indexToColLetter(defaultStart), indexToColLetter(end)];
    }
  }

  // Prefer explicit section anchors if present.
  const top5 = findHeaderContains(rows, headerIdx, "top 5", true);
  const islandMode = findHeaderContains(rows, headerIdx, "island mode");
  if (top5 !== -1 && islandMode !== -1 && islandMode > top5 + 1) {
    return [indexToColLetter(top5 + 1), indexToColLetter(islandMode - 1)];
  }

  // Thales sheets often start categories right after Top 5 and end before a trailing mode/control column.
  if (key.includes("Thales") && top5 !== -1) {
    const start = top5 + 1;
    const fallbackEnd = colLetterToIndex(CATEGORY_RANGE_MAP[key][1]);
    const end = islandMode !== -1 ? islandMode - 1 : fallbackEnd;
    if (end >= start) {
      return [indexToColLetter(start), indexToColLetter(end)];
    }
  }

  if (subs !== -1 && weRecommend !== -1 && weRecommend > subs) {
    return [indexToColLetter(subs + 1), indexToColLetter(weRecommend - 1)];
  }
  return CATEGORY_RANGE_MAP[key];
}

function categoryLabelRowIndexForSheet(key: RequiredPgSheet, headerIdx: number): number {
  if (key === "Audio_Thales") return 4;
  if (key === "TV_Thales") return 3;
  return headerIdx;
}

function findSheetWithItemIdHeader(workbookRows: Record<string, any[][]>): string | null {
  for (const [sheetName, rows] of Object.entries(workbookRows)) {
    const maxRows = Math.min(rows.length, 8);
    for (let r = 0; r < maxRows; r += 1) {
      const row = rows[r] || [];
      const hasItemId = row.some((cell) => String(cell ?? "").trim() === "Item ID");
      if (hasItemId) return sheetName;
    }
  }
  return null;
}

function normalizeKeyPart(input: string): string {
  return String(input || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function normalizeTitleAscii(input: string): string {
  return String(input || "")
    .replace(/\s*[-–—]\s*[A-Za-z]{1,2}\s*$/, "")
    .replace(/[()]/g, " ")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/\[[^\]]*\]/g, " ")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function movieTokenSetScore(a: string, b: string): number {
  const aa = new Set(normalizeTitleAscii(a).split(/\s+/).filter(Boolean));
  const bb = new Set(normalizeTitleAscii(b).split(/\s+/).filter(Boolean));
  if (!aa.size || !bb.size) return 0;
  let shared = 0;
  for (const token of aa) {
    if (bb.has(token)) shared += 1;
  }
  return Math.round((2 * shared * 100) / (aa.size + bb.size));
}

function boostIfOnlyAuxDiff(baseScore: number, aNorm: string, bNorm: string): number {
  if (baseScore >= 100) return baseScore;
  const aSet = new Set(aNorm.split(/\s+/).filter(Boolean));
  const bSet = new Set(bNorm.split(/\s+/).filter(Boolean));
  const missing = [...aSet].filter((t) => !bSet.has(t));
  const extra = [...bSet].filter((t) => !aSet.has(t));
  const isYear = (t: string) => /^\d{4}$/.test(t);
  const missingNoYear = missing.filter((t) => !isYear(t));
  const extraNoYear = extra.filter((t) => !isYear(t));
  const shared = [...aSet].filter((t) => bSet.has(t)).length;
  const yearsA = [...aSet].filter(isYear);
  const yearsB = [...bSet].filter(isYear);
  const yearMatch = yearsA.some((y) => yearsB.includes(y));
  const hasYearMismatch = yearsA.length > 0 && yearsB.length > 0 && !yearMatch;

  if (hasYearMismatch) return Math.min(baseScore, 88);
  if (missingNoYear.length === 0 && extraNoYear.length === 0) return 100;
  if (missingNoYear.length === 0 && extraNoYear.length > 0 && extraNoYear.length <= 2 && shared >= 3) return Math.max(baseScore, 95);
  if (missingNoYear.length === 0 && extraNoYear.length > 0 && shared >= 2) return Math.max(baseScore, 92);
  return baseScore;
}

function parseAnyDate(value: any): Date | null {
  if (value === null || value === undefined || value === "") return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
  if (typeof value === "number") {
    const epoch = new Date(Date.UTC(1899, 11, 30));
    return new Date(epoch.getTime() + value * 86400000);
  }
  const s = String(value).trim();
  const m = s.match(/^(\d{1,2})[\/\\\-.](\d{1,2}|\w{3})[\/\\\-.](\d{2,4})$/i);
  if (m) {
    const d = Number(m[1]);
    const monthRaw = m[2];
    const y = Number(m[3].length === 2 ? `20${m[3]}` : m[3]);
    const monthIndex = Number.isNaN(Number(monthRaw))
      ? ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"].indexOf(String(monthRaw).slice(0, 3).toLowerCase())
      : Number(monthRaw) - 1;
    if (monthIndex >= 0) {
      const dt = new Date(y, monthIndex, d);
      return Number.isNaN(dt.getTime()) ? null : dt;
    }
  }
  const dt = new Date(s);
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function cycleFromDate(date: Date | null): string {
  if (!date) return "";
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const yy = String(date.getFullYear()).slice(-2);
  return `EK${mm}${yy}`;
}

function cutoffFromCycleCode(cycleCode: string): Date | null {
  const m = cycleCode.match(/^EK(\d{2})(\d{2})$/i);
  if (!m) return null;
  const month = Number(m[1]);
  const year = Number(`20${m[2]}`);
  const last = new Date(year, month, 0);
  return new Date(last.getFullYear(), last.getMonth(), last.getDate(), 23, 59, 59, 999);
}

function isTbdRaw(raw: any): boolean {
  if (raw === null || raw === undefined) return false;
  const s = String(raw).trim().toUpperCase();
  return s === "TBD" || s.includes("TBD");
}

function isTruthyMark(value: any): boolean {
  const s = String(value ?? "")
    .trim()
    .toLowerCase();
  return /^(x|✓|✔|true|yes|y|1)$/.test(s);
}

function getCellAsString(row: any[] | undefined, letter: string): string {
  if (!row) return "";
  const idx = colLetterToIndex(letter);
  return String(row[idx] ?? "").trim();
}

function isInMarcommsValue(text?: string | null, value?: unknown): boolean {
  if (String(text ?? "").trim().toLowerCase() === "in marcomms") return true;
  try {
    const parsed = typeof value === "string" ? JSON.parse(value) : value;
    const index = (parsed as any)?.index;
    return Number(index) === 1;
  } catch {
    return false;
  }
}

async function fetchBoardItemsForDeploy(boardId: number, onPage?: (loaded: number) => void): Promise<MondayBoardItem[]> {
  const query = `
    query ($boardId: [ID!], $cursor: String) {
      boards(ids: $boardId) {
        items_page(limit: 200, cursor: $cursor) {
          cursor
          items {
            id
            name
            column_values {
              id
              text
              value
            }
          }
        }
      }
    }
  `;

  const allItems: MondayBoardItem[] = [];
  let cursor: string | null = null;
  const seenCursors = new Set<string>();
  let pages = 0;
  while (true) {
    pages += 1;
    if (pages > FETCH_CURSOR_MAX_PAGES) break;
    if (cursor && seenCursors.has(cursor)) break;
    if (cursor) seenCursors.add(cursor);
    const res = await withTimeout(
      monday.api(query, { variables: { boardId, cursor } }),
      MONDAY_API_TIMEOUT_MS,
      "Timed out fetching board items from Monday API."
    );
    const page = res?.data?.boards?.[0]?.items_page;
    const items = (page?.items ?? []) as MondayBoardItem[];
    allItems.push(...items);
    onPage?.(allItems.length);
    cursor = page?.cursor ?? null;
    if (!cursor) break;
    await sleep(50);
  }
  return allItems;
}

async function fetchBoardItemsByIds(boardId: number, itemIds: string[], onBatch?: (done: number, total: number) => void): Promise<MondayBoardItem[]> {
  if (!itemIds.length) return [];
  const query = `
    query ($boardId: [ID!], $itemIds: [ID!]) {
      boards(ids: $boardId) {
        items_page(limit: 500, query_params: { ids: $itemIds }) {
          items {
            id
            name
            column_values {
              id
              text
              value
            }
          }
        }
      }
    }
  `;

  const uniqueIds = Array.from(new Set(itemIds.filter(Boolean)));
  const BATCH = 100;
  const out: MondayBoardItem[] = [];

  for (let i = 0; i < uniqueIds.length; i += BATCH) {
    const batch = uniqueIds.slice(i, i + BATCH);
    const res = await withTimeout(
      monday.api(query, { variables: { boardId, itemIds: batch } }),
      MONDAY_API_TIMEOUT_MS,
      "Timed out fetching item details from Monday API."
    );
    const items = (res?.data?.boards?.[0]?.items_page?.items ?? []) as MondayBoardItem[];
    out.push(...items);
    onBatch?.(Math.min(i + batch.length, uniqueIds.length), uniqueIds.length);
    await sleep(40);
  }

  return out;
}

async function fetchJsonWithTimeout(url: string, timeoutMs = EXTERNAL_FETCH_TIMEOUT_MS): Promise<any | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchItemIds(boardId: number, scope: Scope, groupId?: string): Promise<number[]> {
  const query = `
    query ($boardId: [ID!], $cursor: String) {
      boards(ids: $boardId) {
        items_page(limit: 500, cursor: $cursor) {
          cursor
          items {
            id
            group {
              id
            }
          }
        }
      }
    }
  `;

  const ids: number[] = [];
  let cursor: string | null = null;

  while (true) {
    const res = await withTimeout(
      monday.api(query, { variables: { boardId, cursor } }),
      MONDAY_API_TIMEOUT_MS,
      "Timed out fetching scoped item IDs from Monday API."
    );
    const page = res?.data?.boards?.[0]?.items_page;
    const items = page?.items ?? [];

    for (const item of items) {
      if (scope === "group") {
        if (item?.group?.id === groupId) ids.push(Number(item.id));
      } else {
        ids.push(Number(item.id));
      }
    }

    cursor = page?.cursor ?? null;
    if (!cursor) break;
    await sleep(STEP_DELAY_MS);
  }

  return ids;
}

function parseLinkedPulseIds(raw?: unknown): string[] {
  const collected = new Set<string>();

  const visit = (input: unknown): void => {
    if (input === null || input === undefined) return;
    if (typeof input === "string") {
      const trimmed = input.trim();
      if (!trimmed) return;
      try {
        visit(JSON.parse(trimmed));
      } catch {
        return;
      }
      return;
    }
    if (typeof input !== "object") return;

    const obj = input as Record<string, unknown>;
    const linked = obj["linkedPulseIds"];
    if (Array.isArray(linked)) {
      for (const entry of linked) {
        if (entry && typeof entry === "object") {
          const id = (entry as Record<string, unknown>)["linkedPulseId"];
          if (id !== undefined && id !== null) collected.add(String(id));
        } else if (entry !== undefined && entry !== null) {
          collected.add(String(entry));
        }
      }
    }

    const nestedValue = obj["value"];
    if (nestedValue !== undefined) visit(nestedValue);

    const linkedPulseId = obj["linkedPulseId"];
    if (linkedPulseId !== undefined && linkedPulseId !== null) collected.add(String(linkedPulseId));

    const itemIds = obj["item_ids"];
    if (Array.isArray(itemIds)) {
      for (const value of itemIds) {
        if (value !== undefined && value !== null) collected.add(String(value));
      }
    }

    const pulses = obj["pulses"];
    if (Array.isArray(pulses)) {
      for (const entry of pulses) {
        if (entry && typeof entry === "object") {
          const id = (entry as Record<string, unknown>)["id"];
          if (id !== undefined && id !== null) collected.add(String(id));
        }
      }
    }
  };

  visit(raw ?? null);
  return Array.from(collected);
}

function collectLinkedIdsFromColumn(column?: MondayColumn | null): string[] {
  if (!column) return [];
  const unique = new Set<string>();

  if (Array.isArray(column.linked_items)) {
    for (const item of column.linked_items) {
      if (!item) continue;
      if (item.id !== undefined && item.id !== null) unique.add(String(item.id));
    }
  }

  const values: unknown[] = [column.value, column.text, column.display_value];
  for (const value of values) {
    for (const linkedId of parseLinkedPulseIds(value)) {
      unique.add(linkedId);
    }
  }

  return Array.from(unique);
}

async function fetchBoardItemsWithSubitems(boardId: string): Promise<MondayItemWithSubitems[]> {
  const query = `
    query ($boardIds: [ID!], $cursor: String) {
      boards(ids: $boardIds) {
        items_page(limit: 200, cursor: $cursor) {
          cursor
          items {
            id
            name
            subitems {
              id
              name
              board { id }
              column_values {
                id
                text
                value
                type
                ... on MirrorValue { display_value }
                ... on BoardRelationValue { linked_items { id } }
              }
            }
          }
        }
      }
    }
  `;

  const items: MondayItemWithSubitems[] = [];
  let cursor: string | null = null;

  while (true) {
    const res = await monday.api(query, { variables: { boardIds: [boardId], cursor } });
    const page = res?.data?.boards?.[0]?.items_page;
    const batch = page?.items ?? [];
    items.push(...batch);

    cursor = page?.cursor ?? null;
    if (!cursor) break;
    await sleep(STEP_DELAY_MS);
  }

  return items;
}

async function collectLinkedItemIdsFromBoardSubitems(boardId: string): Promise<{ ids: string[]; stats: AlignScanStats }> {
  const items = await fetchBoardItemsWithSubitems(boardId);
  const uniqueLinked = new Set<string>();
  let subitemCount = 0;
  let relationCount = 0;

  for (const item of items) {
    for (const subitem of item.subitems ?? []) {
      subitemCount += 1;
      for (const column of subitem.column_values ?? []) {
        const ids = collectLinkedIdsFromColumn(column);
        if (!ids.length) continue;
        relationCount += ids.length;
        for (const id of ids) uniqueLinked.add(id);
      }
    }
  }

  return {
    ids: Array.from(uniqueLinked),
    stats: {
      sourceBoardId: boardId,
      parentItems: items.length,
      subitems: subitemCount,
      linkedRelations: relationCount,
      uniqueLinkedItems: uniqueLinked.size,
    },
  };
}

async function changeItemsColumnValue(
  boardId: number,
  itemIds: Array<number | string>,
  indexOrNull: number | null,
  onProgress: (p: Progress) => void
): Promise<{ ok: number; failed: number; failedItems: FailedUpdate[] }> {
  const mutation = `
    mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
        id
      }
    }
  `;

  const value = indexOrNull === null ? "{}" : JSON.stringify({ index: indexOrNull });
  const total = itemIds.length;
  let done = 0;
  let ok = 0;
  let failed = 0;
  const failedItems: FailedUpdate[] = [];
  let cursor = 0;

  async function updateOneItem(itemId: number | string): Promise<{ ok: boolean; message?: string }> {
    for (let attempt = 0; attempt <= UPDATE_RETRY_LIMIT; attempt += 1) {
      try {
        const res = await monday.api(mutation, {
          variables: {
            boardId,
            itemId,
            columnId: COLUMN_ID,
            value,
          },
        });
        const gqlErrors = res?.errors ?? [];
        if (Array.isArray(gqlErrors) && gqlErrors.length) {
          throw { errors: gqlErrors };
        }
        return { ok: true };
      } catch (error: any) {
        const message = formatApiError(error);
        const isLastAttempt = attempt >= UPDATE_RETRY_LIMIT;
        if (isLastAttempt) return { ok: false, message };
        await sleep(UPDATE_RETRY_BACKOFF_MS * (attempt + 1));
      }
    }
    return { ok: false, message: "Unknown update error" };
  }

  async function worker() {
    while (true) {
      const current = cursor;
      cursor += 1;
      if (current >= itemIds.length) break;

      const itemId = itemIds[current];
      const result = await updateOneItem(itemId);
      if (result.ok) {
        ok += 1;
      } else {
        failed += 1;
        failedItems.push({ itemId: String(itemId), message: result.message ?? "Unknown error" });
      }

      done += 1;
      onProgress({ done, total, ok, failed });
      await sleep(UPDATE_DELAY_MS);
    }
  }

  const workerCount = Math.min(UPDATE_CONCURRENCY, itemIds.length);
  await Promise.all(Array.from({ length: workerCount }, () => worker()));

  return { ok, failed, failedItems };
}

export default function App() {
  const [boardId, setBoardId] = useState<number | null>(null);
  const [selectedItemIds, setSelectedItemIds] = useState<number[]>([]);
  const [scope, setScope] = useState<Scope>("selected");
  const [trailerScope, setTrailerScope] = useState<Scope>("selected");
  const [trailerMode, setTrailerMode] = useState<TrailerMode>("auto_mark_na");
  const [trailerLogs, setTrailerLogs] = useState<string[]>([]);
  const [trailerReviewRows, setTrailerReviewRows] = useState<TrailerReviewRow[]>([]);
  const [groups, setGroups] = useState<Group[]>([]);
  const [groupId, setGroupId] = useState<string>("");
  const [trailerGroupId, setTrailerGroupId] = useState<string>("");
  const [workflow, setWorkflow] = useState<Workflow>("home");
  const [alignStep, setAlignStep] = useState<1 | 2>(1);
  const [pgStep, setPgStep] = useState<1 | 2 | 3>(1);
  const [pgFileName, setPgFileName] = useState<string>("");
  const [pgRows, setPgRows] = useState<any[][]>([]);
  const [pgHeaders, setPgHeaders] = useState<string[]>([]);
  const [pgWorkbookSheets, setPgWorkbookSheets] = useState<Record<string, any[][]>>({});
  const [pgActiveSheet, setPgActiveSheet] = useState<string>("");
  const [pgParseError, setPgParseError] = useState<string>("");
  const [pgMappings, setPgMappings] = useState<PgMappingRow[]>([]);
  const [pgSheetChecks, setPgSheetChecks] = useState<PgSheetCheckRow[]>([]);
  const [pgHeaderVerifyRows, setPgHeaderVerifyRows] = useState<PgHeaderVerifyRow[]>([]);
  const [pgHeaderMappingDetails, setPgHeaderMappingDetails] = useState<PgHeaderMappingDetailRow[]>([]);
  const [pgMappingSummary, setPgMappingSummary] = useState<{ mapped: number; unmapped: number }>({ mapped: 0, unmapped: 0 });
  const [pgOverrides, setPgOverrides] = useState<Partial<Record<RequiredPgSheet, PgSheetOverride>>>({});
  const [pgOverrideSheet, setPgOverrideSheet] = useState<RequiredPgSheet>("Movies_PAC");
  const [pgOverrideMapping, setPgOverrideMapping] = useState<PgOverrideMappingKey>("header_row");
  const [pgOverrideValue, setPgOverrideValue] = useState("");
  const [pgDeployPlan, setPgDeployPlan] = useState<PgDeployRow[]>([]);
  const [pgDeploySummary, setPgDeploySummary] = useState<PgDeploySummary>({
    inMarcomms: 0,
    matched: 0,
    ready: 0,
    noMatch: 0,
    ambiguous: 0,
    noChanges: 0,
  });
  const [previewFilterContentType, setPreviewFilterContentType] = useState("all");
  const [previewFilterCycleAdded, setPreviewFilterCycleAdded] = useState("all");
  const [previewFilterCycleExpiring, setPreviewFilterCycleExpiring] = useState("all");
  const [previewFilterRowStatus, setPreviewFilterRowStatus] = useState("all");
  const [previewSearch, setPreviewSearch] = useState("");
  const [previewDebugItemId, setPreviewDebugItemId] = useState("");
  const [previewDebugMap, setPreviewDebugMap] = useState<Record<string, string>>({});
  const [previewDebugText, setPreviewDebugText] = useState("");

  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState<string>("Waiting for context...");
  const [progress, setProgress] = useState<Progress>({ done: 0, total: 0, ok: 0, failed: 0 });
  const [failedUpdates, setFailedUpdates] = useState<FailedUpdate[]>([]);
  const [alignSourceBoardId, setAlignSourceBoardId] = useState<string>("");
  const [alignScanStats, setAlignScanStats] = useState<AlignScanStats | null>(null);
  const [availableBoards, setAvailableBoards] = useState<BoardOption[]>([]);
  const [boardsLoading, setBoardsLoading] = useState(false);
  const [onlyMarcommsBoards, setOnlyMarcommsBoards] = useState(true);

  useEffect(() => {
    const unlistenContext = monday.listen("context", (res: any) => {
      const context = res?.data ?? {};
      const id = context.boardId ?? context.boardIds?.[0];
      if (id) setBoardId(Number(id));

      const idsFromObjects = (context.selectedItems ?? []).map((item: any) => Number(item?.id));
      const idsFromIds = (context.selectedItemIds ?? []).map((idValue: any) => Number(idValue));
      const nextIds = idsFromObjects.length ? idsFromObjects : idsFromIds;
      setSelectedItemIds(nextIds.filter((n: number) => Number.isFinite(n)));
    });

    const unlistenItems = monday.listen("itemIds", (res: any) => {
      const ids = (res?.data ?? []).map((idValue: any) => Number(idValue)).filter((n: number) => Number.isFinite(n));
      setSelectedItemIds(ids);
    });

    return () => {
      try {
        unlistenContext?.();
      } catch {}
      try {
        unlistenItems?.();
      } catch {}
    };
  }, []);

  useEffect(() => {
    if (!boardId) return;

    let mounted = true;
    (async () => {
      try {
        const nextGroups = await fetchGroups(boardId);
        if (!mounted) return;
        setGroups(nextGroups);
        if (nextGroups.length && !groupId) {
          setGroupId(nextGroups[0].id);
        }
        if (nextGroups.length && !trailerGroupId) {
          setTrailerGroupId(nextGroups[0].id);
        }
      } catch (error: any) {
        if (!mounted) return;
        setStatus(`Failed to fetch groups: ${error?.message ?? String(error)}`);
      }
    })();

    return () => {
      mounted = false;
    };
  }, [boardId, groupId, trailerGroupId]);

  useEffect(() => {
    let mounted = true;
    (async () => {
      setBoardsLoading(true);
      try {
        const boards = await fetchAvailableBoards();
        if (!mounted) return;
        setAvailableBoards(boards);
      } catch (error: any) {
        if (!mounted) return;
        setStatus(`Failed to fetch boards list: ${formatApiError(error)}`);
      } finally {
        if (mounted) setBoardsLoading(false);
      }
    })();

    return () => {
      mounted = false;
    };
  }, []);

  const selectableBoards = useMemo(() => {
    const withoutSubitemBoards = availableBoards.filter((board) => {
      const name = board.name.toLowerCase();
      return !name.startsWith("subitems of ");
    });

    if (!onlyMarcommsBoards) return withoutSubitemBoards;

    return withoutSubitemBoards.filter((board) => {
      const name = board.name.toLowerCase();
      return name.includes("marcomms") && !name.includes("timeline");
    });
  }, [availableBoards, onlyMarcommsBoards]);

  const scopeHint = useMemo(() => {
    if (scope === "selected") return `Selected items: ${selectedItemIds.length}`;
    if (scope === "group") return `Group: ${groupId || "None selected"}`;
    return "Entire board";
  }, [scope, selectedItemIds.length, groupId]);
  const trailerScopeHint = useMemo(() => {
    if (trailerScope === "selected") return `Selected items: ${selectedItemIds.length}`;
    if (trailerScope === "group") return `Group: ${trailerGroupId || "None selected"}`;
    return "Entire board";
  }, [trailerScope, selectedItemIds.length, trailerGroupId]);
  const selectedTitleLabels = useMemo(() => getTitleFieldLabels(pgOverrideSheet), [pgOverrideSheet]);

  const selectedSheetOverride = pgOverrides[pgOverrideSheet];
  const selectedSheetOverrideRows = useMemo(() => {
    const rows: Array<{ key: PgOverrideMappingKey; label: string; value: string }> = [];
    if (!selectedSheetOverride) return rows;

    if (selectedSheetOverride.headerRow) rows.push({ key: "header_row", label: "Header row", value: String(selectedSheetOverride.headerRow) });
    if (selectedSheetOverride.titleCols?.[0]) rows.push({ key: "title_1", label: selectedTitleLabels[0], value: selectedSheetOverride.titleCols[0] });
    if (selectedSheetOverride.titleCols?.[1]) rows.push({ key: "title_2", label: selectedTitleLabels[1], value: selectedSheetOverride.titleCols[1] });
    if (selectedSheetOverride.systemCols?.EX3) rows.push({ key: "system_ex3", label: "System EX3", value: selectedSheetOverride.systemCols.EX3 });
    if (selectedSheetOverride.systemCols?.EX2) rows.push({ key: "system_ex2", label: "System EX2", value: selectedSheetOverride.systemCols.EX2 });
    if (selectedSheetOverride.systemCols?.L3) rows.push({ key: "system_l3", label: "System L3/3Ki", value: selectedSheetOverride.systemCols.L3 });
    if (selectedSheetOverride.systemCols?.Thales) rows.push({ key: "system_thales", label: "System Thales", value: selectedSheetOverride.systemCols.Thales });
    if (selectedSheetOverride.cycleCol) rows.push({ key: "cycle", label: "Cycle", value: selectedSheetOverride.cycleCol });
    if (selectedSheetOverride.cycleExpiredCol)
      rows.push({ key: "cycle_expired", label: "Cycle Expired", value: selectedSheetOverride.cycleExpiredCol });
    if (selectedSheetOverride.categoryRange?.[0]) rows.push({ key: "cat_start", label: "Category start", value: selectedSheetOverride.categoryRange[0] });
    if (selectedSheetOverride.categoryRange?.[1]) rows.push({ key: "cat_end", label: "Category end", value: selectedSheetOverride.categoryRange[1] });
    return rows;
  }, [selectedSheetOverride, selectedTitleLabels]);

  function clearSingleOverride(sheet: RequiredPgSheet, key: PgOverrideMappingKey) {
    setPgOverrides((prev) => {
      const current = prev[sheet];
      if (!current) return prev;
      const nextCurrent: PgSheetOverride = {
        headerRow: current.headerRow,
        titleCols: current.titleCols ? [current.titleCols[0], current.titleCols[1]] : undefined,
        systemCols: current.systemCols ? { ...current.systemCols } : undefined,
        cycleCol: current.cycleCol,
        cycleExpiredCol: current.cycleExpiredCol,
        categoryRange: current.categoryRange ? [current.categoryRange[0], current.categoryRange[1]] : undefined,
      };

      if (key === "header_row") delete nextCurrent.headerRow;
      if (key === "title_1" || key === "title_2") {
        const cols = nextCurrent.titleCols ?? ["", ""];
        if (key === "title_1") cols[0] = "";
        if (key === "title_2") cols[1] = "";
        nextCurrent.titleCols = cols[0] || cols[1] ? ([cols[0], cols[1]] as [string, string]) : undefined;
      }
      if (key.startsWith("system_")) {
        nextCurrent.systemCols = { ...(nextCurrent.systemCols ?? {}) };
        if (key === "system_ex3") delete nextCurrent.systemCols.EX3;
        if (key === "system_ex2") delete nextCurrent.systemCols.EX2;
        if (key === "system_l3") delete nextCurrent.systemCols.L3;
        if (key === "system_thales") delete nextCurrent.systemCols.Thales;
        if (!Object.keys(nextCurrent.systemCols).length) delete nextCurrent.systemCols;
      }
      if (key === "cycle") delete nextCurrent.cycleCol;
      if (key === "cycle_expired") delete nextCurrent.cycleExpiredCol;
      if (key === "cat_start" || key === "cat_end") {
        const range = nextCurrent.categoryRange ?? ["", ""];
        if (key === "cat_start") range[0] = "";
        if (key === "cat_end") range[1] = "";
        nextCurrent.categoryRange = range[0] || range[1] ? ([range[0], range[1]] as [string, string]) : undefined;
      }

      const hasAny =
        nextCurrent.headerRow ||
        (nextCurrent.titleCols && (nextCurrent.titleCols[0] || nextCurrent.titleCols[1])) ||
        (nextCurrent.systemCols && Object.keys(nextCurrent.systemCols).length) ||
        nextCurrent.cycleCol ||
        nextCurrent.cycleExpiredCol ||
        (nextCurrent.categoryRange && (nextCurrent.categoryRange[0] || nextCurrent.categoryRange[1]));

      const next = { ...prev };
      if (!hasAny) {
        delete next[sheet];
      } else {
        next[sheet] = nextCurrent;
      }
      return next;
    });
  }

  async function runAction(indexOrNull: number | null) {
    if (!boardId) {
      setStatus("Board context not ready yet.");
      return;
    }

    setBusy(true);
    setProgress({ done: 0, total: 0, ok: 0, failed: 0 });
    setFailedUpdates([]);
    setStatus("Preparing item list...");

    try {
      let itemIds: number[] = [];

      if (scope === "selected") {
        itemIds = selectedItemIds;
      } else if (scope === "group") {
        if (!groupId) {
          setStatus("Choose a group first.");
          return;
        }
        itemIds = await fetchItemIds(boardId, "group", groupId);
      } else {
        itemIds = await fetchItemIds(boardId, "board");
      }

      if (!itemIds.length) {
        setStatus("No items found for the selected scope.");
        return;
      }

      setProgress({ done: 0, total: itemIds.length, ok: 0, failed: 0 });
      setStatus(`Updating ${itemIds.length} item(s)...`);

      const result = await changeItemsColumnValue(boardId, itemIds, indexOrNull, (nextProgress) => {
        setProgress(nextProgress);
        setStatus(`Updating... ${nextProgress.done}/${nextProgress.total}`);
      });

      if (result.failed > 0) {
        setFailedUpdates(result.failedItems);
        setStatus(`Completed with errors. Updated: ${result.ok}, Failed: ${result.failed}.`);
      } else {
        setFailedUpdates([]);
        setStatus(`Completed. Updated ${result.ok} item(s).`);
      }
    } catch (error: any) {
      setStatus(`Error: ${error?.message ?? String(error)}`);
    } finally {
      setBusy(false);
    }
  }

  async function runAlignMarcommsFromSubitemLinks() {
    const sourceBoardId = alignSourceBoardId.trim();
    if (!sourceBoardId) {
      setStatus("Enter a source board ID first.");
      return;
    }

    setBusy(true);
    setProgress({ done: 0, total: 0, ok: 0, failed: 0 });
    setFailedUpdates([]);
    setAlignScanStats(null);
    setStatus(`Scanning board ${sourceBoardId} for subitem links...`);

    try {
      const { ids, stats } = await collectLinkedItemIdsFromBoardSubitems(sourceBoardId);
      setAlignScanStats(stats);

      if (!ids.length) {
        setStatus("No linked items found on subitems for this board.");
        return;
      }

      setProgress({ done: 0, total: ids.length, ok: 0, failed: 0 });
      setStatus(`Updating ${ids.length} linked item(s) on Marcomms board ${MARCOMMS_BOARD_ID}...`);

      const result = await changeItemsColumnValue(Number(MARCOMMS_BOARD_ID), ids, 1, (nextProgress) => {
        setProgress(nextProgress);
        setStatus(`Updating linked items... ${nextProgress.done}/${nextProgress.total}`);
      });

      if (result.failed > 0) {
        setFailedUpdates(result.failedItems);
        setStatus(`Completed with errors. Updated: ${result.ok}, Failed: ${result.failed}.`);
      } else {
        setStatus(`Completed. Updated ${result.ok} linked item(s) to In Marcomms.`);
      }
    } catch (error: any) {
      setStatus(`Error: ${error?.message ?? String(error)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handlePgFileChange(file: File) {
    setBusy(true);
    setPgParseError("");
    setPgMappings([]);
    setPgSheetChecks([]);
    setPgMappingSummary({ mapped: 0, unmapped: 0 });

    try {
      const buffer = await file.arrayBuffer();
      const workbook = XLSX.read(buffer, { type: "array", cellDates: false });
      const sheetNames = workbook.SheetNames || [];
      if (!sheetNames.length) throw new Error("Workbook has no sheets.");

      const sheetMap: Record<string, any[][]> = {};
      for (const name of sheetNames) {
        const worksheet = workbook.Sheets[name];
        sheetMap[name] = XLSX.utils.sheet_to_json(worksheet, { header: 1, raw: true }) as any[][];
      }

      const itemIdSheet = findSheetWithItemIdHeader(sheetMap);
      const activeSheet = itemIdSheet ?? sheetNames[0];
      const rows = sheetMap[activeSheet] ?? [];
      if (!rows.length) throw new Error(`Sheet "${activeSheet}" is empty.`);

      const headerRow = findHeaderRowIndex(rows);
      const headers = (rows[headerRow] ?? []).map((h) => String(h ?? "").trim());
      setPgFileName(file.name);
      setPgWorkbookSheets(sheetMap);
      setPgActiveSheet(activeSheet);
      setPgRows(rows);
      setPgHeaders(headers);
      setStatus(
        itemIdSheet
          ? `Loaded "${file.name}". Active sheet: ${activeSheet} (${Math.max(rows.length - 1, 0)} data row(s)).`
          : `Loaded "${file.name}". Active sheet: ${activeSheet}.`
      );
      setPgStep(2);
    } catch (error: any) {
      const msg = error?.message ?? String(error);
      setPgParseError(msg);
      setStatus(`Failed to load sheet: ${msg}`);
    } finally {
      setBusy(false);
    }
  }

  async function runPgMappingCheck() {
    if (!boardId) {
      setStatus("Board context not ready yet.");
      return;
    }
    if (!pgRows.length || !pgHeaders.length) {
      setStatus("Load a Programme Grid workbook first.");
      return;
    }

    setBusy(true);
    setStatus("Checking mappings against board columns...");
    setProgress({ done: 0, total: Math.max(pgHeaders.length, 1), ok: 0, failed: 0 });
    setPgHeaderVerifyRows([]);
    setPgHeaderMappingDetails([]);

    try {
      const itemIdPresent = pgHeaders.some((h) => h === "Item ID");
      if (!itemIdPresent) {
        const available = Object.keys(pgWorkbookSheets);
        const checks: PgSheetCheckRow[] = [];
        let found = 0;
        for (let i = 0; i < REQUIRED_PG_SHEETS.length; i += 1) {
          const required = REQUIRED_PG_SHEETS[i];
          const match = autoBestMatchSheet(required, available);
          const rows = match ? pgWorkbookSheets[match] ?? [] : [];
          const headerRow = rows.length ? findHeaderRowIndex(rows) : 0;
          const score = match ? tokenSetScore(required, match) : 0;
          const ok = Boolean(match) && score >= 45;
          if (ok) found += 1;
          checks.push({
            requiredSheet: required,
            matchedSheet: match || "(none)",
            found: ok,
            headerRow: rows.length ? headerRow + 1 : 0,
            note: ok ? `Token match ${score}%` : `Low match score ${score}%`,
          });
          setProgress({ done: i + 1, total: REQUIRED_PG_SHEETS.length, ok: found, failed: i + 1 - found });
        }
        setPgSheetChecks(checks);
        setPgMappings([]);
        setPgMappingSummary({ mapped: found, unmapped: REQUIRED_PG_SHEETS.length - found });
        setStatus(
          `Checked Programme Grid sheet mapping. Matched ${found}/${REQUIRED_PG_SHEETS.length} required sheets.`
        );
        return;
      }

      const columns = await fetchBoardColumns(boardId);
      const byId = new Map<string, BoardColumn>();
      const byTitle = new Map<string, BoardColumn>();
      for (const col of columns) {
        byId.set(col.id, col);
        byTitle.set(col.title.trim().toLowerCase(), col);
      }

      const mappings: PgMappingRow[] = [];
      let mapped = 0;
      let unmapped = 0;

      for (let i = 0; i < pgHeaders.length; i += 1) {
        const header = pgHeaders[i];
        if (!header) {
          mappings.push({ header: "(blank)", mapped: false, reason: "Blank header" });
          unmapped += 1;
          setProgress({ done: i + 1, total: pgHeaders.length, ok: mapped, failed: unmapped });
          continue;
        }

        if (header === "Item ID" || header === "Item Name" || header === "Group" || header === "Is Subitem" || header === "Parent Item ID") {
          mappings.push({ header, mapped: true, reason: "Control column (not updated)" });
          mapped += 1;
          setProgress({ done: i + 1, total: pgHeaders.length, ok: mapped, failed: unmapped });
          continue;
        }

        const explicitId = parseHeaderColumnId(header);
        const cleanTitle = header.replace(/\s*\[[a-zA-Z0-9_]+\]\s*$/, "").trim().toLowerCase();

        let hit: BoardColumn | undefined;
        if (explicitId && byId.has(explicitId)) hit = byId.get(explicitId);
        if (!hit && byTitle.has(cleanTitle)) hit = byTitle.get(cleanTitle);

        if (hit) {
          mappings.push({
            header,
            mapped: true,
            columnId: hit.id,
            columnTitle: hit.title,
            columnType: hit.type,
          });
          mapped += 1;
        } else {
          mappings.push({
            header,
            mapped: false,
            reason: explicitId ? `Column id "${explicitId}" not found on board` : "No column title/id match",
          });
          unmapped += 1;
        }

        setProgress({ done: i + 1, total: pgHeaders.length, ok: mapped, failed: unmapped });
      }

      const itemIdIdx = pgHeaders.findIndex((h) => h === "Item ID");
      if (itemIdIdx === -1) {
        mappings.unshift({ header: "Item ID", mapped: false, reason: 'Required header "Item ID" is missing' });
        unmapped += 1;
      }

      setPgMappings(mappings);
      setPgSheetChecks([]);
      setPgMappingSummary({ mapped, unmapped });
      setStatus(`Mapping check complete. Mapped ${mapped}, Unmapped ${unmapped}.`);
    } catch (error: any) {
      setStatus(`Mapping check failed: ${formatApiError(error)}`);
    } finally {
      setBusy(false);
    }
  }

  function runPgHeaderVerification() {
    if (!pgSheetChecks.length) {
      setStatus("Run sheet mapping check first.");
      return;
    }

    const rowsOut: PgHeaderVerifyRow[] = [];
    const details: PgHeaderMappingDetailRow[] = [];
    for (const check of pgSheetChecks) {
      if (!check.found) continue;
      const req = check.requiredSheet as RequiredPgSheet;
      const sheetRows = pgWorkbookSheets[check.matchedSheet] ?? [];
      if (!sheetRows.length) continue;
      const override = pgOverrides[req];

      const detectedHeaderIdx = findHeaderRowIndex(sheetRows);
      const headerIdx = override?.headerRow && override.headerRow > 0 ? Math.max(0, override.headerRow - 1) : detectedHeaderIdx;
      const headerRow = sheetRows[headerIdx] ?? [];
      const categoryHeaderRow = sheetRows[categoryLabelRowIndexForSheet(req, headerIdx)] ?? headerRow;

      const detectedTitleCols = autoSuggestTitleCols(req, sheetRows, headerIdx);
      const titleCols: [string, string] = [
        override?.titleCols?.[0] || detectedTitleCols[0],
        override?.titleCols?.[1] || detectedTitleCols[1],
      ];
      const titleFieldLabels = getTitleFieldLabels(req);
      const detectedSystemCols = autoSuggestSystemCols(req, sheetRows, headerIdx);
      const systemCols = { ...detectedSystemCols, ...(override?.systemCols ?? {}) };
      const cycleCol = override?.cycleCol ?? autoSuggestCycle(req, sheetRows, headerIdx);
      const cycleExpiredCol = override?.cycleExpiredCol ?? autoSuggestCycleExpired(req, sheetRows, headerIdx);
      const detectedCategoryRange = autoSuggestCategoryRange(req, sheetRows, headerIdx);
      const categoryRange: [string, string] = [
        override?.categoryRange?.[0] || detectedCategoryRange[0],
        override?.categoryRange?.[1] || detectedCategoryRange[1],
      ];
      const categoryStartOverridden = Boolean(override?.categoryRange?.[0]);
      const categoryEndOverridden = Boolean(override?.categoryRange?.[1]);

      const titleHeaders = titleCols.map((letter) => {
        const idx = colLetterToIndex(letter);
        return `${letter}="${String(headerRow[idx] ?? "").trim() || "—"}"`;
      });
      const systemHeaderLabels = Object.entries(systemCols).map(([sys, letter]) => {
        const idx = colLetterToIndex(letter);
        return `${sys}:${letter} (${String(headerRow[idx] ?? "").trim() || "—"})`;
      });

      rowsOut.push({
        requiredSheet: req,
        matchedSheet: check.matchedSheet,
        headerRow: headerIdx + 1,
        titleCols,
        systemCols,
        cycleCol,
        cycleExpiredCol,
        categoryRange,
        titleHeaders,
        systemHeaderLabels,
      });

      details.push({
        requiredSheet: req,
        matchedSheet: check.matchedSheet,
        mapping: "Header row",
        columnFound: "-",
        valueFound: String(headerIdx + 1),
        note: override?.headerRow ? "Manual override" : "Detected row containing title/header markers",
      });
      for (let i = 0; i < titleCols.length; i += 1) {
        const letter = titleCols[i];
        const idx = colLetterToIndex(letter);
        details.push({
          requiredSheet: req,
          matchedSheet: check.matchedSheet,
          mapping: titleFieldLabels[i],
          columnFound: letter,
          valueFound: String(headerRow[idx] ?? "").trim() || "—",
          note: override?.titleCols?.[i] ? "Manual override" : "Title column match",
        });
      }
      if (!req.includes("Thales")) {
        for (const [sys, letter] of Object.entries(systemCols)) {
          const idx = letter === "-" ? -1 : colLetterToIndex(letter);
          details.push({
            requiredSheet: req,
            matchedSheet: check.matchedSheet,
            mapping: `System (${sys})`,
            columnFound: letter,
            valueFound: idx >= 0 ? String(headerRow[idx] ?? "").trim() || "—" : "Presence-only",
            note:
              req === "Audio S3Ki_PAC" && sys === "L3"
                ? "Presence-only: title match in this sheet means L3 = true"
                : override?.systemCols?.[sys as "EX3" | "EX2" | "L3" | "Thales"]
                  ? "Manual override"
                  : 'Prefers "... From" headers then fallbacks',
          });
        }
      }
      {
        const idx = colLetterToIndex(cycleCol);
        details.push({
          requiredSheet: req,
          matchedSheet: check.matchedSheet,
          mapping: "Cycle",
          columnFound: cycleCol,
          valueFound: String(headerRow[idx] ?? "").trim() || "—",
          note: override?.cycleCol ? "Manual override" : "Start Date mapping",
        });
      }
      {
        const idx = colLetterToIndex(cycleExpiredCol);
        details.push({
          requiredSheet: req,
          matchedSheet: check.matchedSheet,
        mapping: "Cycle Expiring",
        columnFound: cycleExpiredCol,
        valueFound: String(headerRow[idx] ?? "").trim() || "—",
        note: override?.cycleExpiredCol ? "Manual override" : "End Date mapping",
      });
      }
      {
        const startIdx = colLetterToIndex(categoryRange[0]);
        const endIdx = colLetterToIndex(categoryRange[1]);
        const prevStartIdx = Math.max(startIdx - 1, 0);
        const nextEndIdx = Math.min(endIdx + 1, Math.max((headerRow?.length ?? 1) - 1, 0));
        const prevStartLetter = indexToColLetter(prevStartIdx);
        const nextEndLetter = indexToColLetter(nextEndIdx);
        const prevStartHeader = String(categoryHeaderRow[prevStartIdx] ?? "").trim() || "—";
        const nextEndHeader = String(categoryHeaderRow[nextEndIdx] ?? "").trim() || "—";

        details.push({
          requiredSheet: req,
          matchedSheet: check.matchedSheet,
          mapping: "Category range start",
          columnFound: categoryRange[0],
          valueFound: String(categoryHeaderRow[startIdx] ?? "").trim() || "—",
          note: `${categoryStartOverridden ? "Manual override. " : ""}Prev ${prevStartLetter}="${prevStartHeader}"`,
        });
        details.push({
          requiredSheet: req,
          matchedSheet: check.matchedSheet,
          mapping: "Category range end",
          columnFound: categoryRange[1],
          valueFound: String(categoryHeaderRow[endIdx] ?? "").trim() || "—",
          note: `${categoryEndOverridden ? "Manual override. " : ""}Next ${nextEndLetter}="${nextEndHeader}"`,
        });
      }
    }

    setPgHeaderVerifyRows(rowsOut);
    setPgHeaderMappingDetails(details);
    setStatus(`Header verification complete for ${rowsOut.length} sheet(s).`);
  }

  function renderResetMarcommsStep() {
    const percent = progress.total > 0 ? Math.round((progress.done / progress.total) * 100) : 0;

    return (
      <>
        <h3 style={{ margin: "0 0 8px 0" }}>Step 1. Reset Marcomms</h3>
        <p style={{ marginTop: 0, opacity: 0.75 }}>
          Choose scope and set/clear the Marcomms status value on column <strong>{COLUMN_ID}</strong>.
        </p>

        <div style={{ marginBottom: 12 }}>
          <div>Board ID: {boardId ?? "Loading..."}</div>
          <div>Column ID: {COLUMN_ID}</div>
        </div>

        <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 12, flexWrap: "wrap" }}>
          <label>
            Scope{" "}
            <select disabled={busy} value={scope} onChange={(e) => setScope(e.target.value as Scope)}>
              <option value="selected">Selected items</option>
              <option value="group">Entire group</option>
              <option value="board">Entire board</option>
            </select>
          </label>

          {scope === "group" && (
            <label>
              Group{" "}
              <select disabled={busy} value={groupId} onChange={(e) => setGroupId(e.target.value)}>
                {groups.map((group) => (
                  <option key={group.id} value={group.id}>
                    {group.title}
                  </option>
                ))}
              </select>
            </label>
          )}

          <span style={{ opacity: 0.7 }}>{scopeHint}</span>
        </div>

        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <button disabled={busy} onClick={() => runAction(1)}>
            Set index 1 (In Marcomms)
          </button>
          <button disabled={busy} onClick={() => runAction(0)}>
            Set index 0 (Not in Marcomms)
          </button>
          <button disabled={busy} onClick={() => runAction(null)}>
            Clear to default
          </button>
        </div>

        <div style={{ marginTop: 12, fontSize: 13 }}>
          <div>Status: {busy ? "Working" : "Idle"}</div>
          <div>{status}</div>
          <progress value={progress.done} max={Math.max(progress.total, 1)} style={{ width: 360, height: 14 }} />
          <div>{percent}% complete</div>
          <div>
            Progress: {progress.done}/{progress.total} | Success: {progress.ok} | Failed: {progress.failed}
          </div>
          {!busy && failedUpdates.length > 0 && (
            <details open style={{ marginTop: 8 }}>
              <summary>Failed items ({failedUpdates.length})</summary>
              <div style={{ marginTop: 6, maxHeight: 180, overflowY: "auto", border: "1px solid #ddd", padding: 8 }}>
                {failedUpdates.map((f) => (
                  <div key={f.itemId}>
                    Item {f.itemId}: {f.message}
                  </div>
                ))}
              </div>
              <div style={{ marginTop: 6 }}>
                Failed IDs: {failedUpdates.map((f) => f.itemId).join(", ")}
              </div>
            </details>
          )}
        </div>
      </>
    );
  }

  const previewRowsWithComputed = useMemo(() => {
    return pgDeployPlan.map((row) => {
      return {
        row,
        cycleAdded: row.computed?.cycleAdded || "-",
        cycleExpiring: row.computed?.cycleExpiring || "-",
      };
    });
  }, [pgDeployPlan]);

  const alphaNumCollator = useMemo(() => new Intl.Collator(undefined, { numeric: true, sensitivity: "base" }), []);

  const previewContentTypeOptions = useMemo(() => {
    return Array.from(new Set(previewRowsWithComputed.map((x) => x.row.system || "-"))).sort((a, b) => alphaNumCollator.compare(a, b));
  }, [previewRowsWithComputed, alphaNumCollator]);

  const previewCycleAddedOptions = useMemo(() => {
    return Array.from(new Set(previewRowsWithComputed.map((x) => x.cycleAdded))).sort((a, b) => alphaNumCollator.compare(a, b));
  }, [previewRowsWithComputed, alphaNumCollator]);

  const previewCycleExpiringOptions = useMemo(() => {
    return Array.from(new Set(previewRowsWithComputed.map((x) => x.cycleExpiring))).sort((a, b) => alphaNumCollator.compare(a, b));
  }, [previewRowsWithComputed, alphaNumCollator]);

  const previewRowStatusOptions = useMemo(() => {
    return Array.from(new Set(previewRowsWithComputed.map((x) => x.row.status))).sort((a, b) => alphaNumCollator.compare(a, b));
  }, [previewRowsWithComputed, alphaNumCollator]);

  const filteredPreviewRows = useMemo(() => {
    const query = previewSearch.trim().toLowerCase();
    return previewRowsWithComputed.filter(({ row, cycleAdded, cycleExpiring }) => {
      if (previewFilterContentType !== "all" && (row.system || "-") !== previewFilterContentType) return false;
      if (previewFilterCycleAdded !== "all" && cycleAdded !== previewFilterCycleAdded) return false;
      if (previewFilterCycleExpiring !== "all" && cycleExpiring !== previewFilterCycleExpiring) return false;
      if (previewFilterRowStatus !== "all" && row.status !== previewFilterRowStatus) return false;

      if (!query) return true;
      const hay = [
        row.itemName,
        row.itemId,
        row.itemMeta,
        row.system,
        cycleAdded,
        cycleExpiring,
        row.pacMatchedRow ? String(row.pacMatchedRow) : "",
        row.thalesMatchedRow ? String(row.thalesMatchedRow) : "",
      ]
        .join(" ")
        .toLowerCase();
      return hay.includes(query);
    });
  }, [previewRowsWithComputed, previewFilterContentType, previewFilterCycleAdded, previewFilterCycleExpiring, previewFilterRowStatus, previewSearch]);

  function getItemColumnText(item: MondayBoardItem, columnId: string): string {
    const hit = item.column_values?.find((c) => c.id === columnId);
    return String(hit?.text ?? "").trim();
  }

  function getItemColumn(item: MondayBoardItem, columnId: string) {
    return item.column_values?.find((c) => c.id === columnId);
  }

  function getItemCheckboxValue(item: MondayBoardItem, columnId: string): string {
    const col = getItemColumn(item, columnId);
    try {
      const parsed = typeof col?.value === "string" ? JSON.parse(col.value) : col?.value;
      const checked = (parsed as any)?.checked;
      if (checked === "true" || checked === true) return "true";
      if (checked === "false" || checked === false) return "false";
    } catch {}
    const text = String(col?.text ?? "")
      .trim()
      .toLowerCase();
    if (["v", "true", "yes", "checked"].includes(text)) return "true";
    if (["false", "no", "unchecked"].includes(text)) return "false";
    return "";
  }

  async function buildPgDeployPreview() {
    if (!boardId) {
      setStatus("Board context not ready yet.");
      return;
    }
    if (!pgHeaderVerifyRows.length) {
      setStatus("Run Step 2 mapping + Verify Headers first.");
      return;
    }

    setBusy(true);
    setPgDeployPlan([]);
    setPreviewDebugMap({});
    setPreviewDebugText("");
    setStatus("Building preview...");
    setProgress({ done: 0, total: 1, ok: 0, failed: 0 });

    try {
      const boardColumns = await fetchBoardColumns(boardId);
      const boardColumnById = new Map(boardColumns.map((c) => [c.id, c]));
      const items = await fetchBoardItemsForDeploy(boardId);
      const inMarcommsItems = items.filter((item) => {
        const marcommsCol = item.column_values?.find((c) => c.id === COLUMN_ID);
        const itemName = String(item.name ?? "").trim();
        const lowerName = itemName.toLowerCase();
        const isBranding = lowerName.includes("branding");
        const isCollection = / collection$/i.test(itemName);
        return isInMarcommsValue(marcommsCol?.text, marcommsCol?.value) && !isBranding && !isCollection;
      });
      const cycleExpiredTargetId = boardColumnById.has(COL_CYCLE_EXPIRED) ? COL_CYCLE_EXPIRED : COL_CYCLE_EXPIRED_FALLBACK;
      const targetColumns = [
        COL_CYCLE,
        cycleExpiredTargetId,
        COL_CAT_PAC,
        COL_CAT_THALES,
        COL_FLAG_EX3,
        COL_FLAG_EX2,
        COL_FLAG_L3,
        COL_FLAG_THALES,
      ].filter((id) => boardColumnById.has(id));

      const byRequiredSheet = new Map(pgHeaderVerifyRows.map((r) => [r.requiredSheet as RequiredPgSheet, r]));
      const contentRoutes: Record<
        "movies" | "tv" | "audio" | "emirates",
        { pac: RequiredPgSheet; thales: RequiredPgSheet; s3?: RequiredPgSheet }
      > = {
        movies: { pac: "Movies_PAC", thales: "Movies_Thales" },
        tv: { pac: "TV_PAC", thales: "TV_Thales" },
        audio: { pac: "Audio eX-Series_PAC", thales: "Audio_Thales", s3: "Audio S3Ki_PAC" },
        emirates: { pac: "Emirates World_PAC", thales: "Emirates World_Thales" },
      };
      const resolveRoute = (contentType: string): keyof typeof contentRoutes | null => {
        const t = String(contentType || "").toLowerCase();
        if (t.includes("emirates world")) return "emirates";
        if (t.includes("dubai")) return "emirates";
        if (t.includes("movie")) return "movies";
        if (t.includes("tv")) return "tv";
        if (t.includes("music") || t.includes("podcast")) return "audio";
        return null;
      };

      const buildIndex = (config: PgHeaderVerifyRow) => {
        const rows = pgWorkbookSheets[config.matchedSheet] ?? [];
        const headerIdx = Math.max(config.headerRow - 1, 0);
        const titleIdx = config.titleCols.map((l) => colLetterToIndex(l));
        const titleKeys: string[] = [];
        const titleIndex: Record<string, number> = {};
        for (let r = headerIdx + 1; r < rows.length; r += 1) {
          const row = rows[r] ?? [];
          const raw = titleIdx.map((idx) => String(row[idx] ?? "").trim()).filter(Boolean).join(" ");
          const key = normalizeTitleAscii(raw);
          if (!key) continue;
          titleKeys.push(key);
          titleIndex[key] = r;
          if (config.requiredSheet.includes("Emirates World") && titleIdx.length === 2) {
            const swappedRaw = [titleIdx[1], titleIdx[0]]
              .map((idx) => String(row[idx] ?? "").trim())
              .filter(Boolean)
              .join(" ");
            const swapped = normalizeTitleAscii(swappedRaw);
            if (swapped && !titleIndex[swapped]) {
              titleKeys.push(swapped);
              titleIndex[swapped] = r;
            }
          }
        }
        const rawSystemIdx = Object.fromEntries(Object.entries(config.systemCols).map(([k, v]) => [k, colLetterToIndex(v)]));
        const fallbackSystemIdx: Record<string, number> = {};
        for (const [k, v] of Object.entries(SYSTEM_COLUMN_MAP[config.requiredSheet as RequiredPgSheet] || {})) {
          if (rawSystemIdx[k] === undefined || Number.isNaN(rawSystemIdx[k])) {
            fallbackSystemIdx[k] = colLetterToIndex(v);
          }
        }
        return {
          config,
          rows,
          headerIdx,
          titleKeys,
          titleIndex,
          cycleIdx: colLetterToIndex(config.cycleCol),
          cycleExpiryIdx: colLetterToIndex(config.cycleExpiredCol),
          catStartIdx: colLetterToIndex(config.categoryRange[0]),
          catEndIdx: colLetterToIndex(config.categoryRange[1]),
          systemIdx: { ...rawSystemIdx, ...fallbackSystemIdx },
          catLabelRowIdx: categoryLabelRowIndexForSheet(config.requiredSheet as RequiredPgSheet, headerIdx),
        };
      };

      const indexCache = new Map<string, ReturnType<typeof buildIndex>>();
      const getIndex = (sheet: RequiredPgSheet) => {
        const cached = indexCache.get(sheet);
        if (cached) return cached;
        const config = byRequiredSheet.get(sheet);
        if (!config) return null;
        const idx = buildIndex(config);
        indexCache.set(sheet, idx);
        return idx;
      };
      const boolToCell = (v: boolean | null) => (v === null ? "" : v ? "true" : "false");

      const bestMatch = (candidates: Set<string>, allKeys: string[], minScore = 90, allowContainment = false) => {
        let best: { key: string; score: number } | null = null;
        for (const key of allKeys) {
          let bestForCandidate = 0;
          for (const candidate of candidates) {
            const base = movieTokenSetScore(candidate, key);
            const sc = boostIfOnlyAuxDiff(base, normalizeTitleAscii(candidate), key);
            const containmentBoost =
              allowContainment && (candidate.includes(key) || key.includes(candidate)) && Math.min(candidate.length, key.length) >= 8
                ? Math.max(sc, 92)
                : sc;
            if (containmentBoost > bestForCandidate) bestForCandidate = containmentBoost;
          }
          if (!best || bestForCandidate > best.score) best = { key, score: bestForCandidate };
        }
        return best && best.score >= minScore ? best : null;
      };

      const plan: PgDeployRow[] = [];
      const debugMap: Record<string, string> = {};
      let matched = 0;
      let ready = 0;
      let noMatch = 0;
      const ambiguous = 0;
      let noChanges = 0;

      for (let i = 0; i < inMarcommsItems.length; i += 1) {
        const item = inMarcommsItems[i];
        const contentType = getItemColumnText(item, COL_CONTENT_TYPE);
        const routeKey = resolveRoute(contentType);
        const name = String(item.name ?? "").trim();
        const foreign = getItemColumnText(item, COL_FOREIGN_TITLE);
        const seasonYear = getItemColumnText(item, COL_SEASON_YEAR_ALBUM);
        const debugLines: string[] = [
          `Item: ${name} (${item.id})`,
          `Content Type: ${contentType || "-"}`,
          `Route: ${routeKey ?? "none"}`,
          `Foreign Title: ${foreign || "-"}`,
          `Year/Season/Album: ${seasonYear || "-"}`,
        ];
        if (!routeKey) {
          debugLines.push("Outcome: no_match (unsupported content type route)");
          debugMap[item.id] = debugLines.join("\n");
          plan.push({
            itemId: item.id,
            itemName: name || "(no name)",
            itemMeta: seasonYear,
            matchKey: "",
            matchedSheet: "-",
            matchedRow: 0,
            pacMatchedRow: 0,
            thalesMatchedRow: 0,
            system: contentType || "Unknown",
            status: "no_match",
            reason: "Unsupported content type route for PG matching.",
            computed: {
              cycleAdded: "-",
              cycleExpiring: "-",
              pacCategories: "-",
              thalesCategories: "-",
              ex3: "-",
              ex2: "-",
              l3: "-",
              thales: "-",
            },
            updates: [],
          });
          noMatch += 1;
          setProgress({ done: i + 1, total: inMarcommsItems.length, ok: matched, failed: noMatch });
          continue;
        }
        const route = contentRoutes[routeKey];
        const pacIndex = getIndex(route.pac);
        const thalesIndex = getIndex(route.thales);
        const s3Index = route.s3 ? getIndex(route.s3) : null;
        if (!pacIndex || !thalesIndex) {
          debugLines.push(`Outcome: no_match (missing verified sheet config for route ${routeKey})`);
          debugMap[item.id] = debugLines.join("\n");
          plan.push({
            itemId: item.id,
            itemName: name || "(no name)",
            itemMeta: seasonYear,
            matchKey: "",
            matchedSheet: "-",
            matchedRow: 0,
            pacMatchedRow: 0,
            thalesMatchedRow: 0,
            system: contentType || "Unknown",
            status: "no_match",
            reason: `Missing verified sheet config for route ${routeKey}.`,
            computed: {
              cycleAdded: "-",
              cycleExpiring: "-",
              pacCategories: "-",
              thalesCategories: "-",
              ex3: "-",
              ex2: "-",
              l3: "-",
              thales: "-",
            },
            updates: [],
          });
          noMatch += 1;
          setProgress({ done: i + 1, total: inMarcommsItems.length, ok: matched, failed: noMatch });
          continue;
        }
        const candidates = new Set<string>();
        const fragments = [foreign, name].map((s) => String(s || "").trim()).filter(Boolean);
        for (const frag of fragments) {
          const key = normalizeTitleAscii(frag);
          if (key) candidates.add(key);
          if (seasonYear) {
            const withYear = normalizeTitleAscii(`${frag} ${seasonYear}`);
            if (withYear) candidates.add(withYear);
          }
        }
        debugLines.push(`Search keys: ${Array.from(candidates).join(" | ") || "-"}`);
        if (!candidates.size) {
          debugLines.push("Outcome: no_match (no search keys from item title/year)");
          debugMap[item.id] = debugLines.join("\n");
          plan.push({
            itemId: item.id,
            itemName: name || "(no name)",
            itemMeta: seasonYear,
            matchKey: "",
            matchedSheet: "-",
            matchedRow: 0,
            pacMatchedRow: 0,
            thalesMatchedRow: 0,
            system: contentType || routeKey,
            status: "no_match",
            reason: "No search keys from item title/year.",
            computed: {
              cycleAdded: "-",
              cycleExpiring: "-",
              pacCategories: "-",
              thalesCategories: "-",
              ex3: "-",
              ex2: "-",
              l3: "-",
              thales: "-",
            },
            updates: [],
          });
          noMatch += 1;
          setProgress({ done: i + 1, total: inMarcommsItems.length, ok: matched, failed: noMatch });
          continue;
        }

        const minScore = routeKey === "emirates" ? 80 : 90;
        const allowContainment = routeKey === "emirates";
        const bestPac = bestMatch(candidates, pacIndex.titleKeys, minScore, allowContainment);
        const bestThales = bestMatch(candidates, thalesIndex.titleKeys, minScore, allowContainment);
        const bestS3 = s3Index ? bestMatch(candidates, s3Index.titleKeys, minScore, allowContainment) : null;
        const pacRow = bestPac ? pacIndex.rows[pacIndex.titleIndex[bestPac.key]] ?? null : null;
        const thalesRow = bestThales ? thalesIndex.rows[thalesIndex.titleIndex[bestThales.key]] ?? null : null;
        const s3Row = s3Index && bestS3 ? s3Index.rows[s3Index.titleIndex[bestS3.key]] ?? null : null;
        debugLines.push(`Threshold: ${minScore} (containment ${allowContainment ? "on" : "off"})`);
        debugLines.push(`Best PAC: ${bestPac ? `${bestPac.key} (score ${bestPac.score}, row ${pacIndex.titleIndex[bestPac.key] + 1})` : "none"}`);
        debugLines.push(`Best Thales: ${bestThales ? `${bestThales.key} (score ${bestThales.score}, row ${thalesIndex.titleIndex[bestThales.key] + 1})` : "none"}`);
        debugLines.push(`Best S3: ${bestS3 && s3Index ? `${bestS3.key} (score ${bestS3.score}, row ${s3Index.titleIndex[bestS3.key] + 1})` : "none"}`);

        if (!pacRow && !thalesRow && !s3Row) {
          debugLines.push("Outcome: no_match (no PG rows matched keys)");
          debugMap[item.id] = debugLines.join("\n");
          plan.push({
            itemId: item.id,
            itemName: name || "(no name)",
            itemMeta: seasonYear,
            matchKey: Array.from(candidates).join(" | "),
            matchedSheet: "-",
            matchedRow: 0,
            pacMatchedRow: 0,
            thalesMatchedRow: 0,
            system: contentType || routeKey,
            status: "no_match",
            reason: "No Programme Grid row matched item title/year.",
            computed: {
              cycleAdded: "-",
              cycleExpiring: "-",
              pacCategories: "-",
              thalesCategories: "-",
              ex3: "-",
              ex2: "-",
              l3: "-",
              thales: "-",
            },
            updates: [],
          });
          noMatch += 1;
          setProgress({ done: i + 1, total: inMarcommsItems.length, ok: matched, failed: noMatch });
          continue;
        }

        matched += 1;
        const updates: PgDeployUpdate[] = [];
        const pushIfChanged = (columnId: string, type: "text" | "checkbox", newValue: string, source: string) => {
          const colMeta = boardColumnById.get(columnId);
          if (!colMeta) return;
          if (newValue === "" && type === "text") return;
          const currentValue = type === "checkbox" ? getItemCheckboxValue(item, columnId) : getItemColumnText(item, columnId);
          if (currentValue === newValue) return;
          updates.push({
            columnId,
            columnTitle: colMeta.title,
            currentValue,
            newValue,
            source,
            type,
            value: type === "checkbox" ? { checked: newValue === "true" ? "true" : "false" } : newValue,
          });
        };

        let foundCycle = "";
        if (pacRow) foundCycle = cycleFromDate(parseAnyDate(pacRow[pacIndex.cycleIdx]));
        if (!foundCycle && thalesRow) foundCycle = cycleFromDate(parseAnyDate(thalesRow[thalesIndex.cycleIdx]));
        if (!foundCycle && s3Row && s3Index) foundCycle = cycleFromDate(parseAnyDate(s3Row[s3Index.cycleIdx]));

        const pacExpiryDate = pacRow ? parseAnyDate(pacRow[pacIndex.cycleExpiryIdx]) : null;
        const thExpiryDate = thalesRow ? parseAnyDate(thalesRow[thalesIndex.cycleExpiryIdx]) : null;
        const s3ExpiryDate = s3Row && s3Index ? parseAnyDate(s3Row[s3Index.cycleExpiryIdx]) : null;
        const earliestExpiry =
          [pacExpiryDate, thExpiryDate, s3ExpiryDate].filter(Boolean).sort((a: any, b: any) => a.getTime() - b.getTime())[0] || null;
        const foundCycleExpired = cycleFromDate(earliestExpiry);
        debugLines.push(
          `Cycle source raw: PAC=${pacRow ? String(pacRow[pacIndex.cycleIdx] ?? "") : "-"} | Thales=${thalesRow ? String(thalesRow[thalesIndex.cycleIdx] ?? "") : "-"} | S3=${s3Row && s3Index ? String(s3Row[s3Index.cycleIdx] ?? "") : "-"}`
        );
        debugLines.push(`Cycle computed: ${foundCycle || "-"}`);
        debugLines.push(
          `Cycle Expiring raw dates: PAC=${pacExpiryDate ? pacExpiryDate.toISOString() : "-"} | Thales=${thExpiryDate ? thExpiryDate.toISOString() : "-"} | S3=${s3ExpiryDate ? s3ExpiryDate.toISOString() : "-"}`
        );
        debugLines.push(`Cycle Expiring computed: ${foundCycleExpired || "-"}`);

        let ex3Ok: boolean | null = null;
        let ex2Ok: boolean | null = null;
        let l3Ok: boolean | null = null;
        let thOk: boolean | null = null;
        const sysDebug: string[] = [];

        if (pacRow) {
          for (const [sys, idx] of Object.entries(pacIndex.systemIdx)) {
            const raw = pacRow[idx];
            const ok = String(raw ?? "").trim() !== "";
            sysDebug.push(`PAC ${sys}: raw="${String(raw ?? "")}" => ${ok}`);
            if (sys.toUpperCase() === "EX3") ex3Ok = ok;
            if (sys.toUpperCase() === "EX2") ex2Ok = ok;
            if (sys.toUpperCase() === "L3") l3Ok = ok;
          }
        }
        if (s3Row && s3Index && routeKey === "audio") {
          l3Ok = true;
          sysDebug.push("S3 L3 presence match => true");
        }
        if (thalesRow) {
          thOk = true;
          sysDebug.push("Thales row matched => Thales=true");
        }
        if (sysDebug.length) debugLines.push(`System checks: ${sysDebug.join(" | ")}`);

        const getCats = (index: ReturnType<typeof buildIndex>, row: any[] | null): string[] => {
          if (!row) return [];
          const headerRow = index.rows[index.catLabelRowIdx] ?? index.rows[index.headerIdx] ?? [];
          const out: string[] = [];
          for (let c = index.catStartIdx; c <= index.catEndIdx; c += 1) {
            if (!isTruthyMark(row[c])) continue;
            const label = String(headerRow[c] ?? "").trim();
            if (label) out.push(label);
          }
          return out;
        };

        const pacCats = getCats(pacIndex, pacRow);
        const thalesCats = getCats(thalesIndex, thalesRow);
        const s3Cats = s3Index ? getCats(s3Index, s3Row) : [];
        const allPacCats = Array.from(new Set([...pacCats, ...s3Cats]));
        debugLines.push(`PAC categories: ${allPacCats.join(", ") || "-"}`);
        debugLines.push(`Thales categories: ${thalesCats.join(", ") || "-"}`);
        const sourceTag = `${route.pac}:${bestPac ? pacIndex.titleIndex[bestPac.key] + 1 : "-"} | ${route.thales}:${bestThales ? thalesIndex.titleIndex[bestThales.key] + 1 : "-"}${route.s3 ? ` | ${route.s3}:${bestS3 && s3Index ? s3Index.titleIndex[bestS3.key] + 1 : "-"}` : ""}`;

        const hasAnyComputed =
          Boolean(foundCycle) ||
          Boolean(foundCycleExpired) ||
          allPacCats.length > 0 ||
          thalesCats.length > 0 ||
          ex3Ok !== null ||
          ex2Ok !== null ||
          l3Ok !== null ||
          thOk !== null;

        if (hasAnyComputed) {
          pushIfChanged(COL_CYCLE, "text", foundCycle, sourceTag);
          pushIfChanged(cycleExpiredTargetId, "text", foundCycleExpired, sourceTag);
          pushIfChanged(COL_CAT_PAC, "text", allPacCats.join(", "), sourceTag);
          pushIfChanged(COL_CAT_THALES, "text", thalesCats.join(", "), sourceTag);
          if (ex3Ok !== null) pushIfChanged(COL_FLAG_EX3, "checkbox", boolToCell(ex3Ok), sourceTag);
          if (ex2Ok !== null) pushIfChanged(COL_FLAG_EX2, "checkbox", boolToCell(ex2Ok), sourceTag);
          if (l3Ok !== null) pushIfChanged(COL_FLAG_L3, "checkbox", boolToCell(l3Ok), sourceTag);
          if (thOk !== null) pushIfChanged(COL_FLAG_THALES, "checkbox", boolToCell(thOk), sourceTag);
        }
        if (updates.length) {
          debugLines.push("Planned updates:");
          for (const update of updates) {
            debugLines.push(
              `- ${update.columnTitle} [${update.columnId}]: current="${update.currentValue || "-"}" -> new="${update.newValue || "-"}"`
            );
          }
        } else {
          debugLines.push("Planned updates: none");
        }
        debugLines.push(`Final status: ${updates.length ? "ready" : "no_changes"}`);
        debugMap[item.id] = debugLines.join("\n");

        if (!updates.length) {
          plan.push({
            itemId: item.id,
            itemName: name || "(no name)",
            itemMeta: seasonYear,
            matchKey: Array.from(candidates).join(" | "),
            matchedSheet: `${route.pac} / ${route.thales}`,
            matchedRow: bestPac ? pacIndex.titleIndex[bestPac.key] + 1 : bestThales ? thalesIndex.titleIndex[bestThales.key] + 1 : 0,
            pacMatchedRow: bestPac ? pacIndex.titleIndex[bestPac.key] + 1 : 0,
            thalesMatchedRow: bestThales ? thalesIndex.titleIndex[bestThales.key] + 1 : 0,
            system: contentType || routeKey,
            status: "no_changes",
            reason: hasAnyComputed ? "Matched row found, but all target values already match." : "Matched row, but no mapped output values were produced.",
            computed: {
              cycleAdded: foundCycle || "-",
              cycleExpiring: foundCycleExpired || "-",
              pacCategories: allPacCats.join(", ") || "-",
              thalesCategories: thalesCats.join(", ") || "-",
              ex3: boolToCell(ex3Ok) || "-",
              ex2: boolToCell(ex2Ok) || "-",
              l3: boolToCell(l3Ok) || "-",
              thales: boolToCell(thOk) || "-",
            },
            updates: [],
          });
          noChanges += 1;
        } else {
          plan.push({
            itemId: item.id,
            itemName: name || "(no name)",
            itemMeta: seasonYear,
            matchKey: Array.from(candidates).join(" | "),
            matchedSheet: `${route.pac} / ${route.thales}`,
            matchedRow: bestPac ? pacIndex.titleIndex[bestPac.key] + 1 : bestThales ? thalesIndex.titleIndex[bestThales.key] + 1 : 0,
            pacMatchedRow: bestPac ? pacIndex.titleIndex[bestPac.key] + 1 : 0,
            thalesMatchedRow: bestThales ? thalesIndex.titleIndex[bestThales.key] + 1 : 0,
            system: contentType || routeKey,
            status: "ready",
            reason: `${updates.length} fixed field(s) will update.`,
            computed: {
              cycleAdded: foundCycle || "-",
              cycleExpiring: foundCycleExpired || "-",
              pacCategories: allPacCats.join(", ") || "-",
              thalesCategories: thalesCats.join(", ") || "-",
              ex3: boolToCell(ex3Ok) || "-",
              ex2: boolToCell(ex2Ok) || "-",
              l3: boolToCell(l3Ok) || "-",
              thales: boolToCell(thOk) || "-",
            },
            updates,
          });
          ready += 1;
        }
        setProgress({ done: i + 1, total: inMarcommsItems.length, ok: matched, failed: noMatch });
      }

      setPgDeployPlan(plan);
      setPreviewDebugMap(debugMap);
      setPgDeploySummary({
        inMarcomms: inMarcommsItems.length,
        matched,
        ready,
        noMatch,
        ambiguous,
        noChanges,
      });
      const targetList = targetColumns.join(", ");
      setStatus(`Preview ready. In Marcomms: ${inMarcommsItems.length}, Ready: ${ready}. Target columns: ${targetList}.`);
    } catch (error: any) {
      setStatus(`Preview failed: ${formatApiError(error)}`);
    } finally {
      setBusy(false);
    }
  }

  async function runPgDeploy() {
    if (!boardId) {
      setStatus("Board context not ready yet.");
      return;
    }
    const targets = pgDeployPlan.filter((row) => row.status === "ready" && row.updates.length > 0);
    if (!targets.length) {
      setStatus("No ready updates in preview. Build preview first.");
      return;
    }

    setBusy(true);
    setStatus(`Deploying ${targets.length} item(s)...`);
    setProgress({ done: 0, total: targets.length, ok: 0, failed: 0 });

    const mutation = `
      mutation ($boardId: ID!, $itemId: ID!, $vals: JSON!) {
        change_multiple_column_values(board_id: $boardId, item_id: $itemId, column_values: $vals) {
          id
        }
      }
    `;

    let ok = 0;
    let failed = 0;
    const rowStatus = new Map<string, { status: PgDeployRow["status"]; reason: string }>();
    let cursor = 0;

    try {
      const worker = async () => {
        while (true) {
          const idx = cursor;
          cursor += 1;
          if (idx >= targets.length) break;

          const row = targets[idx];
          try {
            const valuesByColumn: Record<string, unknown> = {};
            for (const update of row.updates) {
              valuesByColumn[update.columnId] = update.value;
            }
            await monday.api(mutation, {
              variables: {
                boardId,
                itemId: row.itemId,
                vals: JSON.stringify(valuesByColumn),
              },
            });
            ok += 1;
            rowStatus.set(row.itemId, { status: "deployed", reason: `Updated ${row.updates.length} column(s).` });
          } catch (error: any) {
            failed += 1;
            rowStatus.set(row.itemId, { status: "failed", reason: formatApiError(error) });
          }
          setProgress({ done: ok + failed, total: targets.length, ok, failed });
        }
      };

      const workerCount = Math.max(1, Math.min(DEPLOY_CONCURRENCY, targets.length));
      await Promise.all(Array.from({ length: workerCount }, () => worker()));

      setPgDeployPlan((prev) =>
        prev.map((row) => {
          const hit = rowStatus.get(row.itemId);
          if (!hit) return row;
          return { ...row, status: hit.status, reason: hit.reason };
        })
      );
      setStatus(failed ? `Deploy completed with errors. Updated: ${ok}, Failed: ${failed}.` : `Deploy completed. Updated ${ok} item(s).`);
    } finally {
      setBusy(false);
    }
  }

  async function setTrailerLinkValue(itemId: string, url: string, text: string) {
    if (!boardId) throw new Error("Board context not ready yet.");
    const mutation = `
      mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
        change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
          id
        }
      }
    `;
    await monday.api(mutation, {
      variables: {
        boardId,
        itemId,
        columnId: COL_TRAILER_LINK,
        value: JSON.stringify({ url, text }),
      },
    });
  }

  async function setLinkColumnValue(itemId: string, columnId: string, url: string, text: string) {
    if (!boardId) throw new Error("Board context not ready yet.");
    const mutation = `
      mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
        change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
          id
        }
      }
    `;
    await monday.api(mutation, {
      variables: {
        boardId,
        itemId,
        columnId,
        value: JSON.stringify({ url, text }),
      },
    });
  }

  async function setFileColumnFromUrl(itemId: string, imageUrl: string) {
    if (!boardId) throw new Error("Board context not ready yet.");
    const mutation = `
      mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
        change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
          id
        }
      }
    `;
    await monday.api(mutation, {
      variables: {
        boardId,
        itemId,
        columnId: COL_SUGGESTED_IMAGE_FILE,
        value: JSON.stringify({ files: [{ url: imageUrl, name: "imdb_preview.jpg" }] }),
      },
    });
  }

  async function clearTrailerLinkValue(itemId: string) {
    if (!boardId) throw new Error("Board context not ready yet.");
    const mutation = `
      mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
        change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
          id
        }
      }
    `;
    await monday.api(mutation, {
      variables: {
        boardId,
        itemId,
        columnId: COL_TRAILER_LINK,
        value: "{}",
      },
    });
  }

  function setTrailerRowChoice(itemId: string, choice: TrailerChoice) {
    setTrailerReviewRows((prev) => prev.map((row) => (row.itemId === itemId ? { ...row, selectedChoice: choice } : row)));
  }

  async function applyTrailerReviewSelections() {
    if (!trailerReviewRows.length) {
      setStatus("No trailer review rows to apply.");
      return;
    }
    if (!boardId) {
      setStatus("Board context not ready yet.");
      return;
    }

    setBusy(true);
    setProgress({ done: 0, total: trailerReviewRows.length, ok: 0, failed: 0 });
    let done = 0;
    let ok = 0;
    let failed = 0;

    for (const row of trailerReviewRows) {
      try {
        let noteParts: string[] = [];
        let appliedAny = false;

        if (row.confirmImdb && row.imdbUrl) {
          await setLinkColumnValue(row.itemId, COL_IMDB_LINK, row.imdbUrl, "IMDb");
          appliedAny = true;
          noteParts.push("IMDb link applied.");
        }

        if (row.confirmImage && row.posterUrl) {
          try {
            await setFileColumnFromUrl(row.itemId, row.posterUrl);
            appliedAny = true;
            noteParts.push("Poster image sent to file column.");
          } catch (fileErr: any) {
            noteParts.push(`Poster write failed: ${fileErr?.message ?? "unsupported by API context"}.`);
          }
        }

        if (appliedAny) {
          ok += 1;
          setTrailerReviewRows((prev) =>
            prev.map((x) => (x.itemId === row.itemId ? { ...x, status: "applied", note: noteParts.join(" ") || "Applied." } : x))
          );
        } else {
          setTrailerReviewRows((prev) =>
            prev.map((x) => (x.itemId === row.itemId ? { ...x, status: "no_trailer", note: "Nothing selected to apply." } : x))
          );
        }
      } catch (error: any) {
        failed += 1;
        setTrailerReviewRows((prev) =>
          prev.map((x) => (x.itemId === row.itemId ? { ...x, status: "failed", note: error?.message ?? "Failed to apply." } : x))
        );
      } finally {
        done += 1;
        setProgress({ done, total: trailerReviewRows.length, ok, failed });
      }
    }
    setStatus(`Trailer review apply complete. Applied: ${ok}, Failed: ${failed}.`);
    setBusy(false);
  }

  async function removeTrailersForSelectedGroup() {
    if (!boardId) {
      setStatus("Board context not ready yet.");
      return;
    }
    if (!trailerGroupId) {
      setStatus("Select a group first.");
      return;
    }

    setBusy(true);
    setProgress({ done: 0, total: 0, ok: 0, failed: 0 });
    setStatus(`Loading items for group ${trailerGroupId}...`);
    try {
      const ids = await fetchItemIds(boardId, "group", trailerGroupId);
      if (!ids.length) {
        setStatus("No items in selected group.");
        return;
      }
      setProgress({ done: 0, total: ids.length, ok: 0, failed: 0 });
      let done = 0;
      let ok = 0;
      let failed = 0;
      for (const id of ids) {
        try {
          await clearTrailerLinkValue(String(id));
          ok += 1;
        } catch {
          failed += 1;
        } finally {
          done += 1;
          setProgress({ done, total: ids.length, ok, failed });
        }
      }
      setStatus(`Removed trailer links for group items. Cleared: ${ok}, Failed: ${failed}.`);
    } finally {
      setBusy(false);
    }
  }

  async function runTrailerLinks() {
    if (!boardId) {
      setStatus("Board context not ready yet.");
      return;
    }
    if (busy) return;
    setBusy(true);
    setFailedUpdates([]);
    setTrailerLogs([]);
    setTrailerReviewRows([]);
    setProgress({ done: 0, total: 0, ok: 0, failed: 0 });
    setStatus("Trailer links run started...");
    const log = (line: string) => {
      const stamp = new Date().toLocaleTimeString("en-GB", { hour12: false });
      setTrailerLogs((prev) => [...prev.slice(-299), `[${stamp}] ${line}`]);
    };

    const getExistingLinkUrl = (item: MondayBoardItem): string => {
      const col = getItemColumn(item, COL_TRAILER_LINK);
      const raw = col?.value;
      if (!raw) return "";
      try {
        const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
        return String((parsed as any)?.url ?? "").trim();
      } catch {
        return "";
      }
    };
    const getYearText = (item: MondayBoardItem): string => getItemColumnText(item, COL_SEASON_YEAR_ALBUM);
    const getYear = (item: MondayBoardItem): number | undefined => {
      const yearText = getYearText(item);
      const match = yearText.match(/\b(19|20)\d{2}\b/);
      return match ? Number(match[0]) : undefined;
    };
    const tmdbSearch = async (title: string, mediaType: "movie" | "tv", year?: number) => {
      const url = qs(`https://api.themoviedb.org/3/search/${mediaType}`, {
        api_key: TMDB_API_KEY,
        query: title,
        year: mediaType === "movie" ? year : undefined,
      });
      const json = await fetchJsonWithTimeout(url);
      return Array.isArray(json?.results) ? json.results : [];
    };
    const tmdbFindTrailer = async (id: number, mediaType: "movie" | "tv"): Promise<string> => {
      const url = qs(`https://api.themoviedb.org/3/${mediaType}/${id}/videos`, { api_key: TMDB_API_KEY });
      const json = await fetchJsonWithTimeout(url);
      const videos = Array.isArray(json?.results) ? json.results : [];
      for (const video of videos) {
        const isYt = String(video?.site ?? "") === "YouTube";
        const isTrailer = String(video?.type ?? "") === "Trailer";
        const key = String(video?.key ?? "");
        if (isYt && isTrailer && key) return `https://www.youtube.com/watch?v=${key}`;
      }
      return "";
    };
    const tmdbExternalMeta = async (
      id: number,
      mediaType: "movie" | "tv"
    ): Promise<{ imdbUrl: string; imdbLabel: string; posterUrl: string; translatedTitle: string }> => {
      const detailsUrl = qs(`https://api.themoviedb.org/3/${mediaType}/${id}`, { api_key: TMDB_API_KEY });
      const extUrl = qs(`https://api.themoviedb.org/3/${mediaType}/${id}/external_ids`, { api_key: TMDB_API_KEY });
      const [details, ext] = await Promise.all([fetchJsonWithTimeout(detailsUrl), fetchJsonWithTimeout(extUrl)]);
      const imdbId = String(ext?.imdb_id ?? "").trim();
      const imdbUrl = imdbId ? `https://www.imdb.com/title/${imdbId}/` : "";
      const posterPath = String(details?.poster_path ?? "").trim();
      const posterUrl = posterPath ? `https://image.tmdb.org/t/p/w154${posterPath}` : "";
      const localizedTitle = String(details?.title ?? details?.name ?? "").trim();
      const originalTitle = String(details?.original_title ?? details?.original_name ?? "").trim();
      const imdbLabel = imdbId ? (originalTitle ? `IMDb (${originalTitle})` : "IMDb title") : "IMDb search";
      return { imdbUrl, imdbLabel, posterUrl, translatedTitle: localizedTitle || originalTitle || "" };
    };
    const youtubeFallback = async (title: string): Promise<{ url: string; label: string }> => {
      const searchUrl = qs("https://www.googleapis.com/youtube/v3/search", {
        key: YOUTUBE_API_KEY,
        q: `${title} trailer`,
        part: "snippet",
        maxResults: 1,
        type: "video",
      });
      const json = await fetchJsonWithTimeout(searchUrl);
      const id = json?.items?.[0]?.id?.videoId;
      const ytTitle = String(json?.items?.[0]?.snippet?.title ?? "").trim();
      return id ? { url: `https://www.youtube.com/watch?v=${id}`, label: ytTitle || "YouTube result" } : { url: "", label: "" };
    };

    try {
      await withTimeout(
        (async () => {
          log(`Run started. Scope=${trailerScope}, mode=${trailerMode}`);
          let scopedIds: string[] | null = null;
          if (trailerScope === "selected") {
            scopedIds = selectedItemIds.map((id) => String(id));
            log(`Selected scope IDs: ${scopedIds.length}.`);
          } else if (trailerScope === "group") {
            if (!trailerGroupId) {
              setStatus("Choose a trailer group first.");
              return;
            }
            log(`Resolving group item IDs for group ${trailerGroupId}...`);
            scopedIds = (await fetchItemIds(boardId, "group", trailerGroupId)).map((id) => String(id));
            log(`Group scope IDs: ${scopedIds.length}.`);
          }

          let scopedItems: MondayBoardItem[] = [];
          if (scopedIds) {
            if (!scopedIds.length) {
              setStatus("No items found for selected trailer scope.");
              return;
            }
            scopedItems = await fetchBoardItemsByIds(boardId, scopedIds, (done, total) => {
              setStatus(`Fetching scoped items... ${done}/${total}`);
            });
          } else {
            scopedItems = await fetchBoardItemsForDeploy(boardId, (loaded) => setStatus(`Fetching board items... ${loaded} loaded`));
          }

          const items = scopedItems.filter((item) => {
            const existingUrl = getExistingLinkUrl(item);
            const existingText = getItemColumnText(item, COL_TRAILER_LINK);
            return !existingUrl && !existingText;
          });
          const runItems = trailerScope === "group" ? items.slice(0, TRAILER_TEST_GROUP_LIMIT) : items;
          if (trailerScope === "group" && items.length > TRAILER_TEST_GROUP_LIMIT) {
            log(`Testing cap enabled: processing first ${TRAILER_TEST_GROUP_LIMIT} of ${items.length} group item(s).`);
          }
          if (!runItems.length) {
            setStatus("No trailer updates needed. All scoped items already have trailer links.");
            return;
          }

          let done = 0;
          let updated = 0;
          let failed = 0;
          const reviewRows: TrailerReviewRow[] = [];
          setProgress({ done: 0, total: runItems.length, ok: 0, failed: 0 });

          for (const item of runItems) {
            const itemId = String(item.id);
            try {
              const foreignTitle = getItemColumnText(item, COL_FOREIGN_TITLE);
              const searchTitle = (foreignTitle || item.name || "").trim();
              if (!searchTitle) throw new Error("No title");
              const yearText = getYearText(item);

              const year = getYear(item);
              const [movies, tv] = await Promise.all([tmdbSearch(searchTitle, "movie", year), tmdbSearch(searchTitle, "tv")]);
              const scored = [...movies.map((r: any) => ({ ...r, _media_type: "movie" as const })), ...tv.map((r: any) => ({ ...r, _media_type: "tv" as const }))]
                .map((r: any) => {
                  const candidate = String(r?.title ?? r?.name ?? "");
                  return { ...r, _score: movieTokenSetScore(searchTitle, candidate) };
                })
                .sort((a: any, b: any) => Number(b?._score ?? 0) - Number(a?._score ?? 0));

              const best = scored[0];
              const alt1 = scored[1];
              const alt2 = scored[2];
              const bestTmdbUrl = best ? await tmdbFindTrailer(Number(best.id), best._media_type) : "";
              const alt1Url = alt1 ? await tmdbFindTrailer(Number(alt1.id), alt1._media_type) : "";
              const alt2Url = alt2 ? await tmdbFindTrailer(Number(alt2.id), alt2._media_type) : "";
              const youtube = await youtubeFallback(searchTitle);
              const youtubeUrl = youtube.url;
              const youtubeLabel = youtube.label;
              const bestTitle = String(best?.title ?? best?.name ?? "").trim();
              const bestOriginal = String(best?.original_title ?? best?.original_name ?? "").trim();
              const bestLabel = best ? `${bestTitle || "-"} (${best?._media_type})` : "";
              const alt1Label = alt1 ? `${String(alt1?.title ?? alt1?.name ?? "")} (${alt1?._media_type})` : "";
              const alt2Label = alt2 ? `${String(alt2?.title ?? alt2?.name ?? "")} (${alt2?._media_type})` : "";
              const matchTitle = best ? `${String(best?.title ?? best?.name ?? "")} (${best?._media_type})` : "No TMDB match";
              const matchScore = Number(best?._score ?? 0);
              const extMeta = best ? await tmdbExternalMeta(Number(best.id), best._media_type) : null;
              const imdbUrl = extMeta?.imdbUrl || `https://www.imdb.com/find/?q=${encodeURIComponent(`${searchTitle} ${yearText || ""}`.trim())}`;
              const imdbLabel = extMeta?.imdbLabel || (bestOriginal && bestOriginal !== bestTitle ? `IMDb (${bestOriginal})` : "IMDb search");
              const posterUrl = extMeta?.posterUrl || (best?.poster_path ? `https://image.tmdb.org/t/p/w154${String(best.poster_path)}` : "");
              const translatedTitle = extMeta?.translatedTitle || bestTitle || foreignTitle || "-";

              reviewRows.push({
                itemId,
                itemName: String(item.name ?? ""),
                searchTitle,
                translatedTitle,
                yearText: yearText || "-",
                matchedOn: matchTitle,
                matchScore,
                bestTmdbUrl,
                bestLabel,
                alt1Url,
                alt1Label,
                alt2Url,
                alt2Label,
                youtubeUrl,
                youtubeLabel,
                imdbUrl,
                imdbLabel,
                posterUrl,
                confirmImdb: true,
                confirmImage: false,
                selectedChoice: "no_trailer",
                status: "pending_review",
                note: "Review IMDb + image and apply.",
              });
              log(`Item ${itemId}: added to IMDb/image review table (score ${matchScore}).`);
            } catch (error: any) {
              failed += 1;
              reviewRows.push({
                itemId,
                itemName: String(item.name ?? ""),
                searchTitle: String(item.name ?? ""),
                translatedTitle: "-",
                yearText: "-",
                matchedOn: "-",
                matchScore: 0,
                bestTmdbUrl: "",
                bestLabel: "",
                alt1Url: "",
                alt1Label: "",
                alt2Url: "",
                alt2Label: "",
                youtubeUrl: "",
                youtubeLabel: "",
                imdbUrl: `https://www.imdb.com/find/?q=${encodeURIComponent(String(item.name ?? ""))}`,
                imdbLabel: "IMDb search",
                posterUrl: "",
                confirmImdb: true,
                confirmImage: false,
                selectedChoice: "no_trailer",
                status: "failed",
                note: error?.message ?? "Failed while searching",
              });
              log(`Item ${itemId}: failed (${error?.message ?? "unknown"}).`);
            } finally {
              done += 1;
              setProgress({ done, total: runItems.length, ok: updated, failed });
              setStatus(`Trailer links ${done}/${runItems.length} • Reviewed: ${reviewRows.length} • Failed: ${failed}`);
            }
          }

          setTrailerReviewRows(reviewRows);
          setStatus(`IMDb/image scan complete. Review rows: ${reviewRows.length}.`);
          log(`Run complete. review=${reviewRows.length}, failed=${failed}.`);
        })(),
        TRAILER_RUN_TIMEOUT_MS,
        "Trailer links timed out. Please narrow scope and try again."
      );
    } catch (error: any) {
      setStatus(`Trailer links failed: ${error?.message ?? String(error)}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ maxWidth: 1100, padding: 16, fontFamily: "sans-serif" }}>
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
      <h2 style={{ marginTop: 0 }}>Marcomms Controls V {APP_VERSION}</h2>
      <p style={{ marginTop: 0, opacity: 0.7 }}>Board operations workflow</p>

      {workflow !== "home" && (
        <div style={{ marginBottom: 12 }}>
          <button
            disabled={busy}
            onClick={() => {
              setWorkflow("home");
            }}
          >
            Back to options
          </button>
        </div>
      )}

      {workflow === "home" && (
        <div>
          <div style={{ display: "grid", gap: 10, gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))" }}>
            <button
              onClick={() => {
                setWorkflow("align");
                setAlignStep(1);
              }}
              style={{ textAlign: "left", padding: 12 }}
            >
              <strong>Align 'In Marcomms'</strong>
              <div style={{ opacity: 0.7, marginTop: 6 }}>Reset and align Marcomms values against current selection.</div>
            </button>
            <button
              onClick={() => {
                setWorkflow("pg");
                setPgStep(1);
              }}
              style={{ textAlign: "left", padding: 12 }}
            >
              <strong>Updates Items to latest Programme Grid</strong>
              <div style={{ opacity: 0.7, marginTop: 6 }}>Load latest grid data, validate mappings, then deploy updates.</div>
            </button>
            <button onClick={() => setWorkflow("archive")} style={{ textAlign: "left", padding: 12 }}>
              <strong>Archive old items</strong>
              <div style={{ opacity: 0.7, marginTop: 6 }}>Identify and archive stale content items from previous cycles.</div>
            </button>
          </div>

          <div style={{ marginTop: 14, padding: 12, border: "1px solid #ddd" }}>
            <h3 style={{ marginTop: 0, marginBottom: 8 }}>Trailer links</h3>
            <p style={{ marginTop: 0, opacity: 0.8 }}>Run the Trailer links Monday API operation.</p>
            <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", marginBottom: 10 }}>
              <label>
                Mode{" "}
                <select disabled={busy} value={trailerMode} onChange={(e) => setTrailerMode(e.target.value as TrailerMode)}>
                  <option value="auto_mark_na">Automatic</option>
                  <option value="auto_only">Automatic (no NOT AVAILABLE writes)</option>
                </select>
              </label>
              <label>
                Scope{" "}
                <select disabled={busy} value={trailerScope} onChange={(e) => setTrailerScope(e.target.value as Scope)}>
                  <option value="selected">Selected</option>
                  <option value="group">Group</option>
                  <option value="board">Board</option>
                </select>
              </label>
              {trailerScope === "group" && (
                <label>
                  Group{" "}
                  <select disabled={busy} value={trailerGroupId} onChange={(e) => setTrailerGroupId(e.target.value)}>
                    {groups.length === 0 ? (
                      <option value="">No groups</option>
                    ) : (
                      groups.map((g) => (
                        <option key={g.id} value={g.id}>
                          {g.title}
                        </option>
                      ))
                    )}
                  </select>
                </label>
              )}
              <span style={{ opacity: 0.7 }}>{trailerScopeHint}</span>
            </div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
              <button disabled={busy} onClick={runTrailerLinks}>
                Run Trailer links
              </button>
              <button disabled={busy || trailerScope !== "group" || !trailerGroupId} onClick={removeTrailersForSelectedGroup}>
                Remove trailers for selected group
              </button>
              <button disabled={busy || trailerReviewRows.length === 0} onClick={applyTrailerReviewSelections}>
                Apply selected choices
              </button>
            </div>
            <div style={{ marginTop: 10, fontSize: 13 }}>
              <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                <span>Status: {busy && status.toLowerCase().includes("trailer links") ? "Working" : "Idle"}</span>
                {busy && status.toLowerCase().includes("trailer links") && (
                  <span
                    style={{
                      width: 14,
                      height: 14,
                      borderRadius: "50%",
                      border: "2px solid #cbd5e1",
                      borderTopColor: "#475569",
                      display: "inline-block",
                      animation: "spin 0.8s linear infinite",
                    }}
                  />
                )}
              </div>
              <div style={{ marginTop: 4 }}>{status}</div>
              <progress value={progress.done} max={Math.max(progress.total, 1)} style={{ width: 360, height: 14, marginTop: 6 }} />
              <div style={{ marginTop: 4, opacity: 0.8 }}>
                Progress: {progress.done}/{progress.total} | Success: {progress.ok} | Failed: {progress.failed}
              </div>
              <div style={{ marginTop: 8 }}>
                <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 4 }}>
                  <strong>Output</strong>
                  <button
                    disabled={busy || trailerLogs.length === 0}
                    onClick={() => setTrailerLogs([])}
                    style={{ fontSize: 12, padding: "2px 6px" }}
                  >
                    Clear
                  </button>
                </div>
                <div
                  style={{
                    border: "1px solid #ddd",
                    background: "#fafafa",
                    padding: 8,
                    maxHeight: 180,
                    overflow: "auto",
                    fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
                    fontSize: 12,
                    whiteSpace: "pre-wrap",
                  }}
                >
                  {trailerLogs.length ? trailerLogs.join("\n") : "No output yet."}
                </div>
              </div>
            </div>
            {trailerReviewRows.length > 0 && (
              <div style={{ marginTop: 12 }}>
                <h4 style={{ margin: "0 0 8px 0" }}>Trailer Review Table</h4>
                <div style={{ maxHeight: 360, overflow: "auto", border: "1px solid #ddd" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff" }}>
                          Item
                        </th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff" }}>
                          Searched title
                        </th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff" }}>
                          Translation
                        </th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff" }}>
                          Year/Season
                        </th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff" }}>
                          IMDb image
                        </th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff" }}>
                          Confirm image
                        </th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff" }}>
                          IMDb link
                        </th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff" }}>
                          Confirm IMDb
                        </th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff" }}>
                          Matched on
                        </th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff" }}>
                          Score
                        </th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff" }}>
                          Status
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {trailerReviewRows.map((row) => (
                        <tr key={`trailer-review-${row.itemId}`}>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>
                            {row.itemName} ({row.itemId})
                          </td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.searchTitle}</td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.translatedTitle}</td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.yearText}</td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>
                            {row.posterUrl ? (
                              <img src={row.posterUrl} alt={row.itemName} style={{ width: 46, height: 69, objectFit: "cover", borderRadius: 2 }} />
                            ) : (
                              "-"
                            )}
                          </td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>
                            <input
                              type="checkbox"
                              disabled={busy || !row.posterUrl}
                              checked={row.confirmImage}
                              onChange={(e) =>
                                setTrailerReviewRows((prev) =>
                                  prev.map((x) => (x.itemId === row.itemId ? { ...x, confirmImage: e.target.checked } : x))
                                )
                              }
                            />
                          </td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>
                            {row.imdbUrl ? (
                              <a href={row.imdbUrl} target="_blank" rel="noreferrer">
                                {row.imdbLabel || "IMDb"}
                              </a>
                            ) : (
                              "-"
                            )}
                          </td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>
                            <input
                              type="checkbox"
                              disabled={busy || !row.imdbUrl}
                              checked={row.confirmImdb}
                              onChange={(e) =>
                                setTrailerReviewRows((prev) =>
                                  prev.map((x) => (x.itemId === row.itemId ? { ...x, confirmImdb: e.target.checked } : x))
                                )
                              }
                            />
                          </td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.matchedOn}</td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.matchScore}</td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>
                            {row.status}
                            {row.note ? ` - ${row.note}` : ""}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {workflow === "align" && (
        <div>
          <h3 style={{ marginBottom: 8 }}>Align 'In Marcomms'</h3>
          <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
            <button disabled={busy} onClick={() => setAlignStep(1)} style={{ fontWeight: alignStep === 1 ? 700 : 400 }}>
              Step 1. Reset Marcomms
            </button>
            <button disabled={busy} onClick={() => setAlignStep(2)} style={{ fontWeight: alignStep === 2 ? 700 : 400 }}>
              Step 2. Align Marcomms
            </button>
          </div>

          {alignStep === 1 && renderResetMarcommsStep()}
          {alignStep === 2 && (
            <div style={{ padding: 12, border: "1px solid #ddd" }}>
              <h3 style={{ marginTop: 0 }}>Step 2. Align Marcomms</h3>
              <p style={{ marginTop: 0 }}>
                Scan subitems on a source board, collect linked item IDs, and set <strong>In Marcomms</strong> on board{" "}
                <strong>{MARCOMMS_BOARD_ID}</strong>.
              </p>

              <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", marginBottom: 10 }}>
                <label>
                  Source board{" "}
                  <select
                    value={alignSourceBoardId}
                    onChange={(e) => setAlignSourceBoardId(e.target.value)}
                    disabled={busy || boardsLoading}
                    style={{ width: 340 }}
                  >
                    <option value="">{boardsLoading ? "Loading boards..." : "Select board..."}</option>
                    {selectableBoards.map((board) => (
                      <option key={board.id} value={board.id}>
                        {board.name} ({board.id})
                      </option>
                    ))}
                  </select>
                </label>

                <label>
                  Source board ID{" "}
                  <input
                    value={alignSourceBoardId}
                    onChange={(e) => setAlignSourceBoardId(e.target.value)}
                    disabled={busy}
                    style={{ width: 160 }}
                  />
                </label>
                <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  <input
                    type="checkbox"
                    checked={onlyMarcommsBoards}
                    onChange={(e) => setOnlyMarcommsBoards(e.target.checked)}
                    disabled={busy || boardsLoading}
                  />
                  Name contains "Marcomms" and not "Timeline"
                </label>
                <button disabled={busy} onClick={runAlignMarcommsFromSubitemLinks}>
                  Run Align (Set In Marcomms)
                </button>
              </div>

              {alignScanStats && (
                <div style={{ fontSize: 13, marginBottom: 8 }}>
                  <div>Parents scanned: {alignScanStats.parentItems}</div>
                  <div>Subitems scanned: {alignScanStats.subitems}</div>
                  <div>Linked relations found: {alignScanStats.linkedRelations}</div>
                  <div>Unique linked items: {alignScanStats.uniqueLinkedItems}</div>
                </div>
              )}

              <div style={{ marginTop: 12, fontSize: 13 }}>
                <div>Status: {busy ? "Working" : "Idle"}</div>
                <div>{status}</div>
                <progress value={progress.done} max={Math.max(progress.total, 1)} style={{ width: 360, height: 14 }} />
                <div>
                  Progress: {progress.done}/{progress.total} | Success: {progress.ok} | Failed: {progress.failed}
                </div>
                {!busy && failedUpdates.length > 0 && (
                  <details open style={{ marginTop: 8 }}>
                    <summary>Failed items ({failedUpdates.length})</summary>
                    <div style={{ marginTop: 6, maxHeight: 180, overflowY: "auto", border: "1px solid #ddd", padding: 8 }}>
                      {failedUpdates.map((f) => (
                        <div key={f.itemId}>
                          Item {f.itemId}: {f.message}
                        </div>
                      ))}
                    </div>
                    <div style={{ marginTop: 6 }}>Failed IDs: {failedUpdates.map((f) => f.itemId).join(", ")}</div>
                  </details>
                )}
              </div>
            </div>
          )}
        </div>
      )}

      {workflow === "pg" && (
        <div>
          <h3 style={{ marginBottom: 8 }}>Updates Items to latest Programme Grid</h3>
          <div style={{ display: "flex", gap: 8, marginBottom: 12, flexWrap: "wrap" }}>
            <button disabled={busy} onClick={() => setPgStep(1)} style={{ fontWeight: pgStep === 1 ? 700 : 400 }}>
              Step 1. Load Programme Grid
            </button>
            <button disabled={busy} onClick={() => setPgStep(2)} style={{ fontWeight: pgStep === 2 ? 700 : 400 }}>
              Step 2. Check Mappings and Errors
            </button>
            <button disabled={busy} onClick={() => setPgStep(3)} style={{ fontWeight: pgStep === 3 ? 700 : 400 }}>
              Step 3. Deploy
            </button>
          </div>
          {pgStep === 1 && (
            <div style={{ padding: 12, border: "1px solid #ddd" }}>
              <h3 style={{ marginTop: 0 }}>Step 1. Load Programme Grid</h3>
              {busy && (
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                  <span
                    style={{
                      width: 14,
                      height: 14,
                      borderRadius: "50%",
                      border: "2px solid #cbd5e1",
                      borderTopColor: "#475569",
                      display: "inline-block",
                      animation: "spin 0.8s linear infinite",
                    }}
                  />
                  <span>Loading workbook...</span>
                </div>
              )}
              <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                <input
                  type="file"
                  accept=".xlsx,.xls"
                  disabled={busy}
                  onChange={(e) => {
                    const file = e.target.files?.[0];
                    if (file) void handlePgFileChange(file);
                  }}
                />
                {pgFileName && <span>Loaded: {pgFileName}</span>}
                {pgActiveSheet && <span>Active sheet: {pgActiveSheet}</span>}
              </div>
              {pgParseError && <div style={{ marginTop: 8, color: "#b91c1c" }}>Error: {pgParseError}</div>}
              {pgRows.length > 0 && (
                <div style={{ marginTop: 8 }}>
                  Rows: {Math.max(pgRows.length - 1, 0)} | Columns: {pgHeaders.length} | Workbook sheets:{" "}
                  {Object.keys(pgWorkbookSheets).length}
                </div>
              )}
            </div>
          )}
          {pgStep === 2 && (
            <div style={{ padding: 12, border: "1px solid #ddd" }}>
              <h3 style={{ marginTop: 0 }}>Step 2. Check Mappings and Errors</h3>
              <p style={{ marginTop: 0 }}>
                If sheet contains <code>Item ID</code>, validates headers against board columns. Otherwise validates required Programme
                Grid sheet mapping.
              </p>
              <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", marginBottom: 10 }}>
                <button disabled={busy || !pgRows.length} onClick={runPgMappingCheck}>
                  Run Mapping Check
                </button>
                <button disabled={busy || !pgSheetChecks.length} onClick={runPgHeaderVerification}>
                  Verify Headers
                </button>
                <span>Board ID: {boardId ?? "Loading..."}</span>
                <span>Sheet: {pgFileName || "Not loaded"}</span>
              </div>
              <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", marginBottom: 10, fontSize: 12 }}>
                <span>Manual mapping override:</span>
                <select
                  value={pgOverrideSheet}
                  onChange={(e) => setPgOverrideSheet(e.target.value as RequiredPgSheet)}
                  disabled={busy}
                >
                  {REQUIRED_PG_SHEETS.map((sheet) => (
                    <option key={sheet} value={sheet}>
                      {sheet}
                    </option>
                  ))}
                </select>
                <select
                  value={pgOverrideMapping}
                  onChange={(e) => setPgOverrideMapping(e.target.value as PgOverrideMappingKey)}
                  disabled={busy}
                >
                  <option value="header_row">Header row</option>
                  <option value="title_1">{selectedTitleLabels[0]}</option>
                  <option value="title_2">{selectedTitleLabels[1]}</option>
                  <option value="system_ex3">System EX3</option>
                  <option value="system_ex2">System EX2</option>
                  <option value="system_l3">System L3/3Ki</option>
                  <option value="system_thales">System Thales</option>
                  <option value="cycle">Cycle</option>
                  <option value="cycle_expired">Cycle Expired</option>
                  <option value="cat_start">Category start</option>
                  <option value="cat_end">Category end</option>
                </select>
                <label>
                  Value{" "}
                  <input
                    value={pgOverrideValue}
                    onChange={(e) => setPgOverrideValue(e.target.value.toUpperCase())}
                    disabled={busy}
                    style={{ width: 90 }}
                    placeholder={pgOverrideMapping === "header_row" ? "4" : "AA"}
                  />
                </label>
                <button
                  disabled={busy || !pgOverrideValue.trim()}
                  onClick={() => {
                    const value = pgOverrideValue.trim();
                    let validationError = "";
                    setPgOverrides((prev) => {
                      const current = { ...(prev[pgOverrideSheet] ?? {}) };
                      const next = { ...prev };

                      if (pgOverrideMapping === "header_row") {
                        const n = Number(value);
                        if (!Number.isFinite(n) || n < 1) {
                          validationError = "Header row override must be a positive number.";
                          return prev;
                        }
                        current.headerRow = Math.floor(n);
                      } else if (pgOverrideMapping === "title_1" || pgOverrideMapping === "title_2") {
                        if (!/^[A-Z]+$/.test(value)) {
                          validationError = "Column override must be letters only (e.g. AA).";
                          return prev;
                        }
                        const cols = current.titleCols ?? ["", ""];
                        if (pgOverrideMapping === "title_1") cols[0] = value;
                        if (pgOverrideMapping === "title_2") cols[1] = value;
                        current.titleCols = [cols[0], cols[1]] as [string, string];
                      } else if (pgOverrideMapping === "cycle" || pgOverrideMapping === "cycle_expired") {
                        if (!/^[A-Z]+$/.test(value)) {
                          validationError = "Column override must be letters only (e.g. AA).";
                          return prev;
                        }
                        if (pgOverrideMapping === "cycle") current.cycleCol = value;
                        if (pgOverrideMapping === "cycle_expired") current.cycleExpiredCol = value;
                      } else if (pgOverrideMapping === "cat_start" || pgOverrideMapping === "cat_end") {
                        if (!/^[A-Z]+$/.test(value)) {
                          validationError = "Column override must be letters only (e.g. AA).";
                          return prev;
                        }
                        const range = current.categoryRange ?? ["", ""];
                        if (pgOverrideMapping === "cat_start") range[0] = value;
                        if (pgOverrideMapping === "cat_end") range[1] = value;
                        current.categoryRange = [range[0], range[1]] as [string, string];
                      } else {
                        if (!/^[A-Z]+$/.test(value)) {
                          validationError = "Column override must be letters only (e.g. AA).";
                          return prev;
                        }
                        current.systemCols = { ...(current.systemCols ?? {}) };
                        if (pgOverrideMapping === "system_ex3") current.systemCols.EX3 = value;
                        if (pgOverrideMapping === "system_ex2") current.systemCols.EX2 = value;
                        if (pgOverrideMapping === "system_l3") current.systemCols.L3 = value;
                        if (pgOverrideMapping === "system_thales") current.systemCols.Thales = value;
                      }

                      next[pgOverrideSheet] = current;
                      return next;
                    });
                    if (validationError) {
                      setStatus(validationError);
                    } else {
                      setStatus(`Override set for ${pgOverrideSheet}: ${pgOverrideMapping} = ${value}`);
                    }
                    setPgOverrideValue("");
                  }}
                >
                  Apply Override
                </button>
                <button
                  disabled={busy}
                  onClick={() => {
                    setPgOverrides((prev) => {
                      const next = { ...prev };
                      delete next[pgOverrideSheet];
                      return next;
                    });
                    setStatus(`Cleared overrides for ${pgOverrideSheet}.`);
                  }}
                >
                  Clear Sheet Overrides
                </button>
                <button
                  disabled={busy}
                  onClick={() => {
                    setPgOverrides({});
                    setStatus("Cleared all overrides.");
                  }}
                >
                  Clear All
                </button>
                <span style={{ opacity: 0.7 }}>
                  Active sheets with overrides: {Object.keys(pgOverrides).length}
                </span>
                <button
                  disabled={busy}
                  onClick={() => {
                    runPgHeaderVerification();
                  }}
                >
                  Re-run Verify
                </button>
              </div>
              {selectedSheetOverrideRows.length > 0 && (
                <div style={{ marginBottom: 10, border: "1px solid #ddd", padding: 8 }}>
                  <div style={{ fontSize: 12, fontWeight: 700, marginBottom: 6 }}>Current overrides for {pgOverrideSheet}</div>
                  <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                    {selectedSheetOverrideRows.map((row) => (
                      <span
                        key={`${row.key}-${row.value}`}
                        style={{
                          display: "inline-flex",
                          alignItems: "center",
                          gap: 6,
                          border: "1px solid #cbd5e1",
                          padding: "2px 6px",
                          borderRadius: 3,
                          fontSize: 12,
                        }}
                      >
                        {row.label}: <strong>{row.value}</strong>
                        <button
                          disabled={busy}
                          onClick={() => {
                            clearSingleOverride(pgOverrideSheet, row.key);
                            setStatus(`Cleared override for ${pgOverrideSheet}: ${row.label}`);
                          }}
                          style={{ fontSize: 11 }}
                        >
                          x
                        </button>
                      </span>
                    ))}
                  </div>
                </div>
              )}
              {/* keep this tiny spacer for readability */}
              <div style={{ height: 2 }} />
              <div style={{ display: "none" }}>
                <button
                  disabled
                  onClick={() => {
                    // no-op placeholder
                    return;
                  }}
                >
                  noop
                </button>
              </div>
              <progress value={progress.done} max={Math.max(progress.total, 1)} style={{ width: 360, height: 14 }} />
              <div style={{ marginTop: 6 }}>
                Checked: {progress.done}/{progress.total} | Mapped: {pgMappingSummary.mapped} | Unmapped: {pgMappingSummary.unmapped}
              </div>
              {pgMappings.length > 0 && (
                <div style={{ marginTop: 10, maxHeight: 320, overflow: "auto", border: "1px solid #ddd" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd" }}>Header</th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd" }}>Result</th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd" }}>Target</th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd" }}>Detail</th>
                      </tr>
                    </thead>
                    <tbody>
                      {pgMappings.map((m, idx) => (
                        <tr key={`${m.header}-${idx}`}>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{m.header}</td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9", color: m.mapped ? "#166534" : "#b91c1c" }}>
                            {m.mapped ? "Mapped" : "Unmapped"}
                          </td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>
                            {m.columnTitle ? `${m.columnTitle} [${m.columnId}]` : "-"}
                          </td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{m.reason || m.columnType || "-"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {pgSheetChecks.length > 0 && (
                <div style={{ marginTop: 10, maxHeight: 320, overflow: "auto", border: "1px solid #ddd" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd" }}>Required Sheet</th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd" }}>Matched Sheet</th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd" }}>Result</th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd" }}>Header Row</th>
                        <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd" }}>Note</th>
                      </tr>
                    </thead>
                    <tbody>
                      {pgSheetChecks.map((m) => (
                        <tr key={m.requiredSheet}>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{m.requiredSheet}</td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{m.matchedSheet}</td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9", color: m.found ? "#166534" : "#b91c1c" }}>
                            {m.found ? "Matched" : "Needs review"}
                          </td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{m.headerRow || "-"}</td>
                          <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{m.note}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {pgHeaderMappingDetails.length > 0 && (
                <div style={{ marginTop: 12, border: "1px solid #ddd", padding: 8 }}>
                  <h4 style={{ margin: "0 0 8px 0" }}>Verify Headers</h4>
                  <div style={{ marginBottom: 8, fontSize: 12, opacity: 0.8 }}>
                    Verified sheets: {pgHeaderVerifyRows.length} | Mapping rows: {pgHeaderMappingDetails.length}
                  </div>
                  <div style={{ maxHeight: 360, overflow: "auto", border: "1px solid #ddd" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                      <thead>
                        <tr>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Matched Sheet</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Mapping</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Column Found</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Value Found</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Note</th>
                        </tr>
                      </thead>
                      <tbody>
                        {pgHeaderMappingDetails.map((row, idx) => (
                          <tr key={`${row.requiredSheet}-${row.matchedSheet}-${row.mapping}-${row.columnFound}-${idx}`}>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.matchedSheet}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.mapping}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.columnFound}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.valueFound}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.note}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}
          {pgStep === 3 && (
            <div style={{ padding: 12, border: "1px solid #ddd" }}>
              <h3 style={{ marginTop: 0 }}>Step 3. Deploy</h3>
              <p style={{ marginTop: 0 }}>
                Preview scans only items set to <strong>In Marcomms</strong>, routes each item to the relevant Programme Grid sheets by
                content type, and lists exact column updates before deploy.
              </p>
              <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", marginBottom: 10 }}>
                <button disabled={busy || !pgHeaderVerifyRows.length} onClick={buildPgDeployPreview}>
                  Build Preview
                </button>
                <button disabled={busy || !pgDeployPlan.some((r) => r.status === "ready" && r.updates.length > 0)} onClick={runPgDeploy}>
                  Deploy
                </button>
                {busy && status.toLowerCase().includes("building preview") && (
                  <span style={{ display: "inline-flex", alignItems: "center" }}>
                    <span
                      style={{
                        width: 14,
                        height: 14,
                        borderRadius: "50%",
                        border: "2px solid #cbd5e1",
                        borderTopColor: "#475569",
                        display: "inline-block",
                        animation: "spin 0.8s linear infinite",
                      }}
                    />
                  </span>
                )}
              </div>
              <progress value={progress.done} max={Math.max(progress.total, 1)} style={{ width: 360, height: 14 }} />
              <div style={{ marginTop: 8, fontSize: 13 }}>
                In Marcomms: {pgDeploySummary.inMarcomms} | Matched: {pgDeploySummary.matched} | Ready: {pgDeploySummary.ready} | No
                match: {pgDeploySummary.noMatch} | Ambiguous: {pgDeploySummary.ambiguous} | No changes: {pgDeploySummary.noChanges}
              </div>
              <div style={{ marginTop: 4, fontSize: 13 }}>
                {busy && status.toLowerCase().includes("building preview") ? "" : status}
              </div>

              {pgDeployPlan.length > 0 && (
                <>
                  <div style={{ marginTop: 10, border: "1px solid #ddd", padding: 8, display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                    <label>
                      Row Status{" "}
                      <select value={previewFilterRowStatus} onChange={(e) => setPreviewFilterRowStatus(e.target.value)} disabled={busy}>
                        <option value="all">All</option>
                        {previewRowStatusOptions.map((value) => (
                          <option key={value} value={value}>
                            {value}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      Content Type{" "}
                      <select value={previewFilterContentType} onChange={(e) => setPreviewFilterContentType(e.target.value)} disabled={busy}>
                        <option value="all">All</option>
                        {previewContentTypeOptions.map((value) => (
                          <option key={value} value={value}>
                            {value}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      Cycle Added{" "}
                      <select value={previewFilterCycleAdded} onChange={(e) => setPreviewFilterCycleAdded(e.target.value)} disabled={busy}>
                        <option value="all">All</option>
                        {previewCycleAddedOptions.map((value) => (
                          <option key={value} value={value}>
                            {value}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      Cycle Expiring{" "}
                      <select value={previewFilterCycleExpiring} onChange={(e) => setPreviewFilterCycleExpiring(e.target.value)} disabled={busy}>
                        <option value="all">All</option>
                        {previewCycleExpiringOptions.map((value) => (
                          <option key={value} value={value}>
                            {value}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      Search{" "}
                      <input
                        value={previewSearch}
                        onChange={(e) => setPreviewSearch(e.target.value)}
                        placeholder="Item name, ID, meta, row..."
                        disabled={busy}
                        style={{ minWidth: 220 }}
                      />
                    </label>
                    <span style={{ fontSize: 12, opacity: 0.75 }}>
                      Showing {filteredPreviewRows.length} / {pgDeployPlan.length}
                    </span>
                  </div>
                  {pgDeployPlan.some((r) => r.status === "failed") && (
                    <details open style={{ marginTop: 8, border: "1px solid #ddd", padding: 8 }}>
                      <summary>Failed deploy rows ({pgDeployPlan.filter((r) => r.status === "failed").length})</summary>
                      <div style={{ marginTop: 6, maxHeight: 180, overflow: "auto", fontSize: 12 }}>
                        {pgDeployPlan
                          .filter((r) => r.status === "failed")
                          .map((r) => (
                            <div key={`failed-${r.itemId}`}>
                              {r.itemName} ({r.itemId}): {r.reason}
                            </div>
                          ))}
                      </div>
                    </details>
                  )}
                  <div style={{ marginTop: 8, border: "1px solid #ddd", padding: 8, display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                    <label>
                      Item ID Debug{" "}
                      <input
                        value={previewDebugItemId}
                        onChange={(e) => setPreviewDebugItemId(e.target.value)}
                        placeholder="e.g. 10700426582"
                        disabled={busy}
                        style={{ minWidth: 180 }}
                      />
                    </label>
                    <button
                      disabled={busy || !previewDebugItemId.trim()}
                      onClick={() => {
                        const raw = previewDebugItemId.trim();
                        const key = raw.replace(/[^\d]/g, "");
                        setPreviewDebugText(
                          previewDebugMap[key] ||
                            previewDebugMap[raw] ||
                            `No debug entry found for item ${raw}. Build Preview first.`
                        );
                      }}
                    >
                      Show Debug
                    </button>
                    {previewDebugText && (
                      <pre style={{ margin: 0, whiteSpace: "pre-wrap", fontSize: 12, width: "100%", maxHeight: 180, overflow: "auto" }}>{previewDebugText}</pre>
                    )}
                  </div>

                  <div style={{ marginTop: 12, border: "1px solid #ddd", padding: 8 }}>
                    <h4 style={{ margin: "0 0 6px 0" }}>Columns Planned For Update</h4>
                    <div style={{ fontSize: 12 }}>
                      {Array.from(
                        new Set(
                          pgDeployPlan
                            .flatMap((row) => row.updates)
                            .map((update) => `${update.columnTitle} [${update.columnId}]`)
                        )
                      ).join(" | ") || "None"}
                    </div>
                  </div>

                  <div style={{ marginTop: 10, maxHeight: 420, overflow: "auto", border: "1px solid #ddd" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                      <thead>
                        <tr>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Item</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Year/Season/Album</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Content Type</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Cycle Added</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Cycle Expiring</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>PAC Categories</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Thales Categories</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>EX3</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>EX2</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>L3</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Thales</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>PAC Row</th>
                          <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #ddd", position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>Thales Row</th>
                        </tr>
                      </thead>
                      <tbody>
                        {filteredPreviewRows.map(({ row }) => {
                          return (
                          <tr key={row.itemId}>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>
                              {row.itemName} ({row.itemId})
                            </td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.itemMeta || "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.system || "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.computed?.cycleAdded || "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.computed?.cycleExpiring || "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.computed?.pacCategories || "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.computed?.thalesCategories || "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.computed?.ex3 || "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.computed?.ex2 || "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.computed?.l3 || "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.computed?.thales || "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.pacMatchedRow || "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f1f5f9" }}>{row.thalesMatchedRow || "-"}</td>
                          </tr>
                        )})}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </div>
          )}
        </div>
      )}

      {workflow === "archive" && (
        <div style={{ padding: 12, border: "1px solid #ddd" }}>
          <h3 style={{ marginTop: 0 }}>Archive old items</h3>
          <p style={{ marginBottom: 0 }}>Archive workflow placeholder ready for the next implementation step.</p>
        </div>
      )}
    </div>
  );
}
