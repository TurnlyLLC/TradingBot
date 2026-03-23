# Kalshi Signal Collector - Railway Deployment Package

This package runs a non-trading Kalshi signal collector on Railway and exposes a phone-friendly dashboard.

## What is included

- `kalshi_signal_collector.py` - the collector bot that scans and labels signals
- `railway_app.py` - dashboard + supervisor + backup manager
- `config_signal_collector.json` - collector config using `/app/data` paths for Railway volumes
- `start.sh` - Railway start command
- `requirements.txt` - Python dependencies
- `.env.example` - environment variable template

## Railway setup

1. Create a new GitHub repo and upload these files.
2. In Railway, create a new project and deploy from that GitHub repo.
3. In the service Variables tab, add:
   - `KALSHI_API_KEY_ID`
   - `KALSHI_PRIVATE_KEY_PEM`
   - optional `ADMIN_TOKEN`
   - optional `BACKUP_INTERVAL_SECONDS`
   - optional `RETENTION_DAYS`
4. Set the Start Command to:
   - `bash start.sh`
5. Add a Volume to the service and mount it to:
   - `/app/data`
6. Deploy the service.
7. After the service is healthy, go to Settings -> Networking -> Public Networking and click Generate Domain.
8. Open the generated domain on your phone to view the dashboard.

## Optional cloud backup to a Railway Bucket

1. Create a Bucket in the same Railway project.
2. Add the Bucket variables to the service as reference variables:
   - `BUCKET`
   - `ACCESS_KEY_ID`
   - `SECRET_ACCESS_KEY`
   - `ENDPOINT`
   - `REGION`
3. Redeploy the service.
4. The app will begin uploading DB and log backups automatically.

## Notes

- This package does **not** place trades.
- The dashboard route is `/`.
- Health JSON is at `/health` and `/api/dashboard`.
- Database download is at `/download/db`.
- Local files are stored under `/app/data` so they persist on the Railway volume.
