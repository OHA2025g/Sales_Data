# Sales Analytics Dashboard - Backend (FastAPI)
# For EasyPanel: set env MONGO_URL, DB_NAME, CORS_ORIGINS; port 10000

FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Application code (do not COPY .env; use EasyPanel env vars for MONGO_URL, DB_NAME, etc.)
COPY backend/server.py ./server.py
COPY backend/copilot_responses.py ./copilot_responses.py
COPY backend/sales_insight_engine.py ./sales_insight_engine.py

# Bundle dataset inside image to avoid Docker-for-Mac bind-mount read deadlocks.
COPY Sales_Data.xlsx ./Sales_Data.xlsx

EXPOSE 10000

# Production ASGI server
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "10000"]
