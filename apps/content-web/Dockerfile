FROM node:20-alpine AS builder
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build:prod

FROM nginx:1.27-alpine
COPY --from=builder /app/dist/app/browser /usr/share/nginx/html
EXPOSE 80
