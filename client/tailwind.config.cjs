const path = require("node:path");

/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    // Index HTML used by Vite
    path.join(__dirname, "index.html"),

    // All Svelte + TS files in client/src
    path.join(__dirname, "src/**/*.{svelte,ts}"),
  ],
  theme: {
    extend: {},
  },
  plugins: [],
};
