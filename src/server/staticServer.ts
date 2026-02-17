import fs from "node:fs/promises";
import http from "node:http";
import path from "node:path";
import type { HttpApiHandler } from "./timelineApi.js";

export function createStaticServer(publicDir: string, apiHandler?: HttpApiHandler): http.Server {
  return http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? "/", "http://localhost");
      if (apiHandler && (await apiHandler(req, res, url))) {
        return;
      }

      const requested = url.pathname === "/" ? "/index.html" : url.pathname;
      const staticResult = await tryServeFile(publicDir, requested, res);
      if (staticResult) return;

      if (url.pathname.startsWith("/api/")) {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Not found" }));
        return;
      }

      if (!url.pathname.startsWith("/api/")) {
        const spaFallback = await tryServeFile(publicDir, "/index.html", res);
        if (spaFallback) return;
      }
    } catch {
      res.writeHead(404);
      res.end("Not found");
    }
  });
}

async function tryServeFile(publicDir: string, requestPath: string, res: http.ServerResponse): Promise<boolean> {
  const safePath = sanitizePath(requestPath);
  if (!safePath) return false;
  const abs = path.join(publicDir, safePath);
  try {
    const data = await fs.readFile(abs);
    const ext = path.extname(abs);
    const mime =
      ext === ".html"
        ? "text/html"
        : ext === ".js"
          ? "application/javascript"
          : ext === ".css"
            ? "text/css"
            : ext === ".json"
              ? "application/json"
              : "application/octet-stream";
    res.writeHead(200, { "Content-Type": mime });
    res.end(data);
    return true;
  } catch {
    return false;
  }
}

function sanitizePath(inputPath: string): string | null {
  const normalized = path.posix.normalize(inputPath);
  if (!normalized.startsWith("/")) return null;
  if (normalized.includes("..")) return null;
  return normalized;
}
