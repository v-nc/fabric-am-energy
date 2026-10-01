# One image for the OPC UA simulator and the edge gateway; docker-compose.yml picks the entry point.
FROM node:22-alpine
WORKDIR /app/edge
COPY edge/package.json edge/package-lock.json edge/tsconfig.json ./
COPY edge/shared/package.json shared/
COPY edge/opcua-sim/package.json opcua-sim/
COPY edge/gateway/package.json gateway/
RUN npm ci --no-audit --no-fund
COPY config /app/config
COPY edge/shared shared
COPY edge/opcua-sim opcua-sim
COPY edge/gateway gateway
RUN mkdir -p /data && chown node:node /data
USER node
ENV NODE_OPTIONS=--disable-warning=ExperimentalWarning
