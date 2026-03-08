# Sales Analytics Dashboard

A comprehensive pharma sales analytics portal with drill-down capabilities, AI-powered insights, and 6 interactive dashboard pages.

## Features
- **Executive Summary** - KPIs, monthly trends, zone distribution
- **Product Intelligence** - Product performance with drill-down
- **Geography Intelligence** - Zone → State → City drill-down
- **Customer Analytics** - Pareto concentration analysis
- **Pricing & Discount** - Price realization metrics
- **Risk & Governance** - Risk indicators with alerts

## Tech Stack
- **Frontend**: React 19, Tailwind CSS, Shadcn/UI, Recharts
- **Backend**: FastAPI, MongoDB
- **AI**: OpenAI GPT-4o (optional)

## Prerequisites
- Python 3.10+
- Node.js 18+
- MongoDB (running locally or via Docker)
- Yarn package manager

## Quick Start

### 1. Start MongoDB
```bash
# Using Docker
docker run -d -p 27017:27017 --name mongodb mongo:latest

# Or install MongoDB locally and start the service
```

### 2. Setup Backend
```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Update .env file for local setup
# Edit backend/.env:
# MONGO_URL="mongodb://localhost:27017"
# DB_NAME="sales_dashboard"

# Run backend on port 10000
uvicorn server:app --host 0.0.0.0 --port 10000 --reload
```

### 3. Setup Frontend
```bash
cd frontend

# Install dependencies
yarn install

# Update .env for local backend
# Edit frontend/.env:
# REACT_APP_BACKEND_URL=http://localhost:10000

# Run frontend
yarn start
```

### 4. Load Data
Once both services are running, click "Refresh Data" button on the dashboard to load the sales data from the Excel files.

## Environment Variables

### Backend (.env)
```
MONGO_URL="mongodb://localhost:27017"
DB_NAME="sales_dashboard"
CORS_ORIGINS="*"
EMERGENT_LLM_KEY=your-openai-key-here  # Optional for AI insights
```

### Frontend (.env)
```
REACT_APP_BACKEND_URL=http://localhost:10000
```

## Running on Port 10000

To run everything on port 10000:

1. Run backend on port 10000:
```bash
cd backend
uvicorn server:app --host 0.0.0.0 --port 10000 --reload
```

2. Update frontend to connect to port 10000:
```bash
# Edit frontend/.env
REACT_APP_BACKEND_URL=http://localhost:10000
```

3. Run frontend (default port 3000):
```bash
cd frontend
yarn start
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| POST /api/data/load | Load Excel data into MongoDB |
| GET /api/dashboard/overview | Executive KPIs |
| GET /api/dashboard/trends | Monthly sales trends |
| GET /api/products/performance | Product metrics |
| GET /api/geography/zones | Zone performance |
| GET /api/customers/performance | Customer analytics |
| GET /api/pricing/analysis | Pricing metrics |
| GET /api/risk/indicators | Risk governance |

## Data Source
The dashboard loads data from two Excel files containing 60,506 pharma sales records:
- Net Sales: ₹520Cr
- Transactions: 5,964
- Customers: 354
- Products: 33
- Date Range: Oct 2022 - Mar 2023

## License
MIT
