# pdf-worker example

A real end-to-end demo of lb-upload-infra:

1. Browser uploads a PDF straight to S3 (presigned URL).
2. POST /complete saves the file metadata in MongoDB (collection: files).
3. lb-upload-infra publishes a Kafka event.
4. A separate worker consumes it, downloads the PDF, extracts the first 3 words,
 and saves them in MongoDB (collection: pdfsummaries) with a ref to the file.

This folder is a standalone project: it depends on lb-upload-infra like any npm
package (here via a local file: link) and imports it as `import { createUploader }
from "lb-upload-infra"`.

## Layout

```text
pdf-worker/
 server.js Express app + Mongoose metadata store (publishes to Kafka)
 worker.js Kafka consumer: PDF -> first 3 words -> MongoDB
 models/
 File.js files collection (uses the lb-upload-infra id as _id)
 PdfSummary.js pdfsummaries collection (ref -> File)
 docker-compose.yml local Kafka + MongoDB
 frontend.html the upload page
```

## Prerequisites

- A real S3 bucket and AWS credentials.
- Docker (for local Kafka + MongoDB).
- The parent package built: from the repo root run `npm run build` once, so the
 file: dependency resolves to dist.

## S3 bucket CORS

The browser PUTs directly to S3, so the bucket needs a CORS rule that allows PUT
from your origin and exposes ETag:

```json
[
 {
 "AllowedOrigins": ["http://localhost:3000"],
 "AllowedMethods": ["PUT", "GET"],
 "AllowedHeaders": ["*"],
 "ExposeHeaders": ["ETag"]
 }
]
```

## Run

```bash
cp .env.example .env # then fill in AWS_REGION, S3_BUCKET, and credentials
npm install
npm run infra:up # start Kafka + MongoDB
npm run server # terminal 1
npm run worker # terminal 2 (mongo may take some time to run the image)
```

Open http://localhost:3000, choose a PDF, and click Upload. The page shows the
file id, waits for the worker, and then displays the first three words.

Inspect MongoDB to see both collections:

```bash
docker exec -it pdf-worker-mongo mongosh pdf-worker-demo --eval "db.files.find().pretty(); db.pdfsummaries.find().pretty()"
```

When you are done: `npm run infra:down`.
