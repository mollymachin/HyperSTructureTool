# Knowledge HyperSTructure Repo Startup Guide

## Quick Start

### First Time Setup
```bash
# Install all dependencies (both root and frontend)
npm run install:all
```

### Start the Application
```bash
# Starts both backend and frontend simultaneously
npm start
```

This will start:
- **Backend**: FastAPI server on `http://localhost:8000`
- **Frontend**: React development server on `http://localhost:3000`

## Available Commands

| Command | Description |
|---------|-------------|
| `npm run install:all` | Install all dependencies (root + frontend) |
| `npm run cleanup` | Kill any processes using ports 3000 and 8000 |
| `npm start` | Start both backend and frontend (includes cleanup) |
| `npm run dev` | Alias for `npm start` |
| `npm run start:backend` | Start only the backend |
| `npm run start:frontend` | Start only the frontend |
| `npm run build` | Build the frontend for production |
| `npm run test` | Run frontend tests |

## Manual Startup (if needed)

To start services manually in separate terminals:

**Terminal 1 - Backend:**
```bash
python3 backend/main.py
```

**Terminal 2 - Frontend:**
```bash
cd frontend
npm start
```