# Static portal images (Section H, Step 5.6).
#   docker build -f deploy/containers/frontend.Dockerfile --build-arg APP=user-frontend -t cosmicforge/user-frontend:<rev> .
FROM node:22-slim AS build
ARG APP
WORKDIR /src
COPY frontends/${APP}/package.json frontends/${APP}/package-lock.json ./
RUN npm ci
COPY frontends/${APP}/ ./
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /src/dist /usr/share/nginx/html
COPY deploy/containers/frontend-nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://127.0.0.1:8080/ >/dev/null || exit 1
