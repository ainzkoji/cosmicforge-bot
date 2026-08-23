import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    port: 4173,  // Admin frontend on port 4173
    proxy: {
      "/monitoring": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      }
    },
  },
})
