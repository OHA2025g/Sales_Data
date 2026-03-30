/**
 * API prefix for axios calls.
 * - Empty REACT_APP_BACKEND_URL → "/api" (same origin: nginx proxy in Docker, or webpack devServer proxy).
 * - Set REACT_APP_BACKEND_URL → full backend origin, e.g. http://localhost:10000
 */
const raw = process.env.REACT_APP_BACKEND_URL;
const trimmed = raw != null ? String(raw).trim() : "";
const base = trimmed.replace(/\/$/, "");
export const API = base ? `${base}/api` : "/api";
