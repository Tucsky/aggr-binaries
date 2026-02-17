import { writable, type Writable } from "svelte/store";
import { buildAppRouteUrl, parseAppRoute, type AppRoute } from "./routes.js";

const fallbackRoute: AppRoute = { kind: "timeline" };
const initialRoute =
  typeof window === "undefined"
    ? fallbackRoute
    : parseAppRoute(window.location.pathname, window.location.search);
const routeStore: Writable<AppRoute> = writable(initialRoute);

let initialized = false;

export const currentRoute = {
  subscribe: routeStore.subscribe,
};

export function initRouteStore(): void {
  if (initialized || typeof window === "undefined") return;
  initialized = true;
  syncFromLocation(true);
  window.addEventListener("popstate", handlePopState);
}

export function navigate(route: AppRoute, opts: { replace?: boolean } = {}): void {
  if (typeof window === "undefined") return;
  const url = buildAppRouteUrl(route);
  const current = `${window.location.pathname}${window.location.search}`;
  if (url !== current) {
    if (opts.replace) {
      window.history.replaceState({}, "", url);
    } else {
      window.history.pushState({}, "", url);
    }
  }
  routeStore.set(parseAppRoute(window.location.pathname, window.location.search));
}

function handlePopState(): void {
  syncFromLocation(true);
}

function syncFromLocation(rewriteCanonical: boolean): void {
  if (typeof window === "undefined") return;
  const route = parseAppRoute(window.location.pathname, window.location.search);
  const canonical = buildAppRouteUrl(route);
  const current = `${window.location.pathname}${window.location.search}`;
  if (rewriteCanonical && canonical !== current) {
    window.history.replaceState({}, "", canonical);
  }
  routeStore.set(route);
}
