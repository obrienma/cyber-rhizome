FROM node:22-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:22-alpine
WORKDIR /app
COPY --from=builder /app/dist ./dist
RUN npm install -g http-server
EXPOSE 3000
CMD ["http-server", "dist", "-p", "3000", "--proxy", "http://localhost:3000?"]
