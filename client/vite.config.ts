// client/vite.config.ts
import { svelte } from "@sveltejs/vite-plugin-svelte";
import path from "node:path";
import { defineConfig } from "vite";
import svelteConfig from "../svelte.config.js"; // <– reuse shared config

export default defineConfig({
  root: path.resolve(__dirname),
  plugins: [
    svelte({
      ...svelteConfig, // gives plugin your preprocess from svelte.config.js
    }),
  ],
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src"),
    },
  },
});
