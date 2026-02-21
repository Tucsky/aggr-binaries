import fs from "node:fs/promises";
import path from "node:path";
import type { FileSystemEntry } from "./model.js";

interface WalkOptions {
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
    let entries: import("node:fs").Dirent[];
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
        continue;
      }
      if (!dirent.isFile()) continue;

      const rel = path.relative(rootPath, fullPath);
      yield {
        rootId,
        rootPath,
        relativePath: toPosix(rel),
        fullPath,
        ext: path.extname(fullPath) || undefined,
      };
    }
  }
}

function seedDirs(rootPath: string, includePaths?: string[]): string[] {
  if (!includePaths || !includePaths.length) {
    // console.log('using default rootPath')
    return [rootPath];
  }

  const dirs: string[] = [];
  for (const rel of includePaths) {
    const abs = path.resolve(rootPath, rel);
    const relCheck = path.relative(rootPath, abs);
    if (relCheck.startsWith("..")) continue;
    dirs.push(abs);
  }
  // console.log('seed includePath', dirs.length ? dirs : [rootPath])

  return dirs.length ? dirs : [rootPath];
}

const toPosix = (p: string): string => (path.sep === "/" ? p : p.split(path.sep).join("/"));
