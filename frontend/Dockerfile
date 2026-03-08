# Sales Analytics Dashboard - Frontend (React)
# For EasyPanel: build with REACT_APP_BACKEND_URL pointing to your backend; port 80

# ---- Build ----
FROM node:20-alpine AS builder

WORKDIR /app

# Install dependencies
COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile

# Backend URL at build time (set in EasyPanel build args or leave default)
ARG REACT_APP_BACKEND_URL=https://sales.demo.agrayianailabs.com
ENV REACT_APP_BACKEND_URL=$REACT_APP_BACKEND_URL

COPY . .
RUN yarn build

# ---- Serve ----
FROM nginx:alpine

# SPA routing
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Production build from previous stage
COPY --from=builder /app/build /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
