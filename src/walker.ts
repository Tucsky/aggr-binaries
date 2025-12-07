import fs from "node:fs/promises";
import path from "node:path";
import type { FileSystemEntry } from "./model.js";

interface WalkOptions {
  statConcurrency?: number; // unused for now, kept for API compatibility
  includePaths?: string[];
}

export async function* walkFiles(
  rootPath: string,
  rootId: number,
  options: WalkOptions = {},
): AsyncGenerator<FileSystemEntry> {
  const dirStack: string[] = seedDirs(rootPath, options.includePaths);

  while (dirStack.length) {
    const dir = dirStack.pop()!;
    let entries;
    try {
      entries = await fs.readdir(dir, { withFileTypes: true });
    } catch {
      continue;
    }

    for (const dirent of entries) {
      if (dirent.name === ".DS_Store") continue;
      const fullPath = path.join(dir, dirent.name);
      if (dirent.isDirectory()) {
        dirStack.push(fullPath);
      } else if (dirent.isFile()) {
        const rel = path.relative(rootPath, fullPath);
        try {
          const stats = await fs.stat(fullPath);
          yield {
            rootId,
            rootPath,
            relativePath: toPosix(rel),
            fullPath,
            size: stats.size,
            mtimeMs: Math.round(stats.mtimeMs),
            ext: path.extname(fullPath) || undefined,
          };
        } catch {
          // ignore stat failures
        }
      }
    }
  }
}

function seedDirs(rootPath: string, includePaths?: string[]): string[] {
  if (!includePaths || !includePaths.length) return [rootPath];
  const dirs: string[] = [];
  for (const rel of includePaths) {
    const abs = path.resolve(rootPath, rel);
    const relCheck = path.relative(rootPath, abs);
    if (relCheck.startsWith("..")) continue;
    dirs.push(abs);
  }
  return dirs.length ? dirs : [rootPath];
}

const toPosix = (p: string): string => (path.sep === "/" ? p : p.split(path.sep).join("/"));
