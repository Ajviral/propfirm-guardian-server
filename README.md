# PropFirm Guardian Server

Backend WebSocket server for PropFirm Guardian app.

## Setup
1. Run: npm install
2. Run: npm start
3. Server runs on port 3000 by default
4. Set PORT environment variable to override

## Endpoints
- POST /api/account-update — receives data from MT5 EA
- GET /api/account/:token — returns latest account data
- GET /health — server health check

## Deployment
Deploy to Railway by connecting this folder as a GitHub repository.
Set the start command to: npm start
