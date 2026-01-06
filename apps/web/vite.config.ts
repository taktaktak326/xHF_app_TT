import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      // API リクエストのみを FastAPI (8080) へ
      '^/api(/.*)?$': {
        target: 'http://127.0.0.1:8080',
        changeOrigin: true,
      },
    },
  },
})
