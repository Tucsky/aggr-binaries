import { formatStartInputUtc, parseStartInputUtcMs } from "../../../src/shared/startInput.js";
import type { Prefs } from "./types.js";

export interface ChartMarketRoute {
  collector: string;
  exchange: string;
  symbol: string;
}

export interface ChartRoute {
  kind: "chart";
  market?: ChartMarketRoute;
  timeframe?: string;
  startTs?: number;
}

export interface TimelineRoute {
  kind: "timeline";
}

export type AppRoute = TimelineRoute | ChartRoute;

export function parseAppRoute(pathname: string, search = ""): AppRoute {
  const normalizedPath = normalizePath(pathname);
  if (normalizedPath === "/" || normalizedPath === "/timeline") {
    return { kind: "timeline" };
  }

  const query = new URLSearchParams(search);
  const timeframe = readTimeframe(query);
  const startTs = readStartTs(query);

  if (normalizedPath === "/chart") {
    return { kind: "chart", timeframe, startTs };
  }

  const segments = normalizedPath.split("/").filter(Boolean);
  if (segments.length === 4 && segments[0] === "chart") {
    const collector = decodeURIComponent(segments[1] ?? "").trim().toUpperCase();
    const exchange = decodeURIComponent(segments[2] ?? "").trim().toUpperCase();
    const symbol = decodeURIComponent(segments[3] ?? "").trim();
    if (collector && exchange && symbol) {
      return {
        kind: "chart",
        market: { collector, exchange, symbol },
        timeframe,
        startTs,
      };
    }
  }

  return { kind: "timeline" };
}

export function buildAppRouteUrl(route: AppRoute): string {
  if (route.kind === "timeline") {
    return "/timeline";
  }
  const query = new URLSearchParams();
  if (route.timeframe) {
    query.set("timeframe", route.timeframe);
  }
  if (Number.isFinite(route.startTs)) {
    query.set("startTs", String(Math.floor(route.startTs as number)));
  }
  const qs = query.toString();
  if (!route.market) {
    return qs ? `/chart?${qs}` : "/chart";
  }
  const path =
    "/chart/" +
    encodeURIComponent(route.market.collector.toUpperCase()) +
    "/" +
    encodeURIComponent(route.market.exchange.toUpperCase()) +
    "/" +
    encodeURIComponent(route.market.symbol);
  return qs ? `${path}?${qs}` : path;
}

export function applyChartRouteToPrefs(base: Prefs, route: ChartRoute): Prefs {
  if (route.kind !== "chart") {
    return base;
  }

  let next: Prefs = { ...base };
  if (route.market) {
    next = {
      ...next,
      collector: route.market.collector.toUpperCase(),
      exchange: route.market.exchange.toUpperCase(),
      symbol: route.market.symbol,
    };
  }
  if (route.timeframe) {
    next = { ...next, timeframe: route.timeframe };
  }
  if (Number.isFinite(route.startTs)) {
    next = { ...next, start: formatStartInputUtc(route.startTs as number) };
  }
  return next;
}

export function chartRouteFromPrefs(prefs: Prefs): ChartRoute {
  const market =
    prefs.collector.trim() && prefs.exchange.trim() && prefs.symbol.trim()
      ? {
          collector: prefs.collector.trim().toUpperCase(),
          exchange: prefs.exchange.trim().toUpperCase(),
          symbol: prefs.symbol.trim(),
        }
      : undefined;
  const startTs = readStartMsFromPrefs(prefs);
  return {
    kind: "chart",
    market,
    timeframe: prefs.timeframe.trim() || undefined,
    startTs,
  };
}

export function isChartRoute(route: AppRoute): route is ChartRoute {
  return route.kind === "chart";
}

export function resolveRouteMarket<T extends { collector: string; exchange: string; symbol: string }>(
  markets: T[],
  routeMarket: ChartMarketRoute,
): T | null {
  const collector = routeMarket.collector.trim().toUpperCase();
  const exchange = routeMarket.exchange.trim().toUpperCase();
  const symbol = routeMarket.symbol.trim().toLowerCase();
  for (const market of markets) {
    if (market.collector.toUpperCase() !== collector) continue;
    if (market.exchange.toUpperCase() !== exchange) continue;
    if (market.symbol.toLowerCase() !== symbol) continue;
    return market;
  }
  return null;
}

function normalizePath(pathname: string): string {
  const clean = pathname.trim();
  if (!clean) return "/";
  if (clean === "/") return "/";
  const withoutTrailing = clean.endsWith("/") ? clean.slice(0, -1) : clean;
  return withoutTrailing.startsWith("/") ? withoutTrailing : `/${withoutTrailing}`;
}

function readTimeframe(query: URLSearchParams): string | undefined {
  const raw = query.get("timeframe");
  if (!raw) return undefined;
  const trimmed = raw.trim();
  return trimmed || undefined;
}

function readStartTs(query: URLSearchParams): number | undefined {
  const raw = query.get("startTs");
  if (!raw) return undefined;
  const value = Number(raw);
  if (!Number.isFinite(value)) return undefined;
  return Math.floor(value);
}

function readStartMsFromPrefs(prefs: Prefs): number | undefined {
  if (!prefs.start.trim()) return undefined;
  const parsed = parseStartInputUtcMs(prefs.start);
  return parsed === null ? undefined : parsed;
}
