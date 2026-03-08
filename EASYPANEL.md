# Deploy on EasyPanel

Use two separate apps in EasyPanel: **backend** and **frontend**.

---

## 1. Backend

- **Build context:** `backend/` (or repo root with Dockerfile path `backend/Dockerfile`)
- **Dockerfile path:** `backend/Dockerfile` (if context is repo root) or `Dockerfile` (if context is `backend/`)
- **Port:** `10000`
- **Environment variables (required):**
  - `MONGO_URL` – MongoDB connection string (e.g. `mongodb://host:27017`)
  - `DB_NAME` – Database name (e.g. `sales_dashboard`)
- **Optional:** `CORS_ORIGINS` – Comma-separated origins (default `*`)

---

## 2. Frontend

- **Build context:** `frontend/`
- **Dockerfile path:** `Dockerfile`
- **Port:** `80`
- **Build argument:** Set `REACT_APP_BACKEND_URL` to your backend URL (e.g. `https://your-backend.easypanel.host` or the internal URL EasyPanel gives you).  
  The frontend calls this URL for `/api/*` requests.

After both are deployed, open the frontend URL in the browser.
