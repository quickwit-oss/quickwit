import { UserConfig } from "vite";

export default {
  base: "/ui",
  server: {
    proxy: {
      "/api": "http://127.0.0.1:7280",
      "/openapi.json": "http://127.0.0.1:7280",
    },
    port: 3000,
  },
  build: {
    rollupOptions: {
      onwarn(warning, warn) {
        // Suppress "use client" directive warnings from material-ui
        if (
          warning.code === "MODULE_LEVEL_DIRECTIVE" &&
          warning.message.includes('"use client"')
        ) {
          return;
        }
        warn(warning);
      },
    },
  },
} satisfies UserConfig;
