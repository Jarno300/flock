FROM node:20-bookworm-slim AS build

WORKDIR /frontend

ARG VITE_API_BASE_URL=http://localhost:3001
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}

COPY src/frontend/package.json src/frontend/package-lock.json /frontend/
RUN npm ci

COPY src/frontend /frontend
RUN npm run build

FROM nginx:1.27-alpine

COPY src/frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /frontend/dist /usr/share/nginx/html

EXPOSE 80
