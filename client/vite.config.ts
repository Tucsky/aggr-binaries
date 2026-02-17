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
  server: {
    proxy: {
      "/api": {
        target: process.env.DEV_API_TARGET || "http://localhost:3000",
        changeOrigin: true,
      },
      "/ws": {
        target: process.env.DEV_API_TARGET || "http://localhost:3000",
        changeOrigin: true,
        ws: true,
      },
    },
  },
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
