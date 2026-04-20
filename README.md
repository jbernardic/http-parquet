# Deployment Guide

## Prerequisites

- Docker installed on the server
- A domain with an A record pointing to your server's IP
- (Optional) A GCS bucket for Parquet file uploads

## 1. Build the Docker Image

On your server, clone the repo and build:

```bash
git clone <repo-url>
cd http-parquet
docker build -t http-parquet .
```

## 2. Run the Container

### Minimal (no GCS)

```bash
docker run -d -p 8080:8080 \
  -v /data:/data \
  -e api.secret-key=your-secret-key \
  --restart unless-stopped \
  http-parquet
```

### With GCS Upload

```bash
docker run -d -p 8080:8080 \
  -v /data:/data \
  -v /path/to/credentials.json:/secrets/adc.json:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/adc.json \
  -e gcs.enabled=true \
  -e gcs.bucket-name=your-bucket \
  -e gcs.object-prefix=events/raw \
  -e api.secret-key=your-secret-key \
  --restart unless-stopped \
  http-parquet
```

For GCS credentials, copy your Application Default Credentials from your local machine:

```bash
# Windows
scp "%APPDATA%\gcloud\application_default_credentials.json" root@your-server-ip:~/credentials.json

# Mac/Linux
scp ~/.config/gcloud/application_default_credentials.json root@your-server-ip:~/credentials.json
```

Or use a service account key from GCP Console → IAM → Service Accounts.

## 3. Set Up Caddy (Reverse Proxy + TLS)

Install Caddy:

```bash
sudo apt update && sudo apt install -y caddy
```

Edit `/etc/caddy/Caddyfile`:

```
{
    cert_issuer acme
}

api.yourdomain.com {
    reverse_proxy localhost:8080
}
```

Reload:

```bash
sudo systemctl reload caddy
```

Caddy automatically provisions a Let's Encrypt TLS certificate. Make sure ports 80 and 443 are open and 8080 is blocked:

```bash
sudo ufw allow 80
sudo ufw allow 443
sudo ufw deny 8080
```

## 4. Configuration Reference

All settings can be passed as `-e property.name=value` to `docker run`.

| Property | Default | Description |
|---|---|---|
| `api.secret-key` | _(empty)_ | API key required in `X-Api-Key` header. Leave blank to disable auth. |
| `ingestion.output-dir` | `data` | Directory where JSONL and Parquet files are written. |
| `ingestion.flush.max-interval-ms` | `1000` | Max milliseconds between disk flushes. |
| `ingestion.flush.max-bytes` | `65536` | Flush when unflushed buffer exceeds this size (bytes). |
| `gcs.enabled` | `false` | Enable GCS uploads after each hourly file roll. |
| `gcs.bucket-name` | _(empty)_ | Destination GCS bucket name. |
| `gcs.object-prefix` | _(empty)_ | Optional folder prefix inside the bucket, e.g. `events/raw`. |
| `gcs.credentials-path` | _(empty)_ | Path to service account JSON. Leave blank to use Application Default Credentials. |

## 5. Sending Events

Single event:

```bash
curl -X POST https://api.yourdomain.com/ingest/my-tenant \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: your-secret-key" \
  -d '{"event": "click", "user": "123"}'
```

Batch:

```bash
curl -X POST https://api.yourdomain.com/ingest/my-tenant \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: your-secret-key" \
  -d '[{"event": "click"}, {"event": "view"}]'
```

Returns `202 Accepted` immediately — data is buffered and written asynchronously.

## 6. Viewing Logs

```bash
docker logs <container-id> -f
```

## 7. Output Files

Files are written to `/data/{tenantId}/` on the host:

```
/data/
  my-tenant/
    2026-04-20T14-00-00-000.jsonl       # completed hour
    2026-04-20T14-00-00-000.parquet     # converted Parquet
    2026-04-20T15-00-00-000.jsonl.tmp   # current hour, still writing
```

JSONL files are converted to Parquet at the end of each hour and optionally uploaded to GCS.

## 8. GCS Lifecycle (Auto-Delete Old Files)

To automatically delete old files from your GCS bucket after 30 days:

```bash
gcloud storage buckets update gs://your-bucket --lifecycle-file=lifecycle.json
```

`lifecycle.json`:

```json
{
  "lifecycle": {
    "rule": [{
      "action": { "type": "Delete" },
      "condition": { "age": 30 }
    }]
  }
}
```
