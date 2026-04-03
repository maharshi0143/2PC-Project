# Frontend (Transaction Monitor)

This frontend is a React app served by Nginx in Docker.

## Runtime URL

- http://localhost:8081

## Local Development (optional)

```bash
npm install
npm start
```

By default, the app calls the coordinator at `http://localhost:3000` and opens a WebSocket connection to the same host with `ws://`.

## Build

```bash
npm run build
```

The Docker image builds static assets and serves them from Nginx on port 80 (mapped to host 8081 in `docker-compose.yml`).
