# ---- Build ----
FROM node:20-alpine AS builder

WORKDIR /app

# Install dependencies
COPY package.json ./
RUN npm install

# Backend URL at build time
ARG REACT_APP_BACKEND_URL=https://sales.demo.agrayianailabs.com
ENV REACT_APP_BACKEND_URL=$REACT_APP_BACKEND_URL

COPY . .
RUN npm run build

# ---- Serve ----
FROM nginx:alpine

# SPA routing
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Production build
COPY --from=builder /app/build /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
