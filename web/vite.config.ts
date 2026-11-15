import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => ({
  base: loadEnv(mode, '.', 'VITE_').VITE_BASE_PATH || '/',
  plugins: [react()],
  server: {
    port: 3001,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:9001',
        changeOrigin: true,
      },
      '/ring': {
        target: 'http://127.0.0.1:9001',
        changeOrigin: true,
      },
      '/internal': {
        target: 'http://127.0.0.1:9001',
        changeOrigin: true,
      },
      '/health': {
        target: 'http://127.0.0.1:9001',
        changeOrigin: true,
      },
      '/metrics': {
        target: 'http://127.0.0.1:9001',
        changeOrigin: true,
      },
    },
  },
}))
