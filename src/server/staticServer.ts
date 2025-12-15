import fs from "node:fs/promises";
import http from "node:http";
import path from "node:path";

export function createStaticServer(publicDir: string): http.Server {
  return http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? "/", "http://localhost");
      const filePath = url.pathname === "/" ? "/index.html" : url.pathname;
      const abs = path.join(publicDir, filePath);
      const data = await fs.readFile(abs);
      const ext = path.extname(abs);
      const mime =
        ext === ".html"
          ? "text/html"
          : ext === ".js"
            ? "application/javascript"
            : ext === ".css"
              ? "text/css"
              : "application/octet-stream";
      res.writeHead(200, { "Content-Type": mime });
      res.end(data);
    } catch {
      res.writeHead(404);
      res.end("Not found");
    }
  });
}
