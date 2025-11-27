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
} satisfies UserConfig;
