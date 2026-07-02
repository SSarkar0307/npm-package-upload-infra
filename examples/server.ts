import express from "express";
import { createUploader, bearerTokenAuth } from "../src";
import "dotenv/config";
import cors from "cors";

// demo in-memory metadata store
const files = new Map<string, unknown>();

const app = express();

const uploader = createUploader({
  s3: {
    region: process.env.AWS_REGION ?? "us-east-1",
    bucket: process.env.S3_BUCKET ?? "my-bucket",
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? "",
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? "",
    },
  },
  metadata: {
    save: async (file) => {
      files.set(file.id, file);
    },
    find: async (id) => (files.get(id) as any) ?? null,
    delete: async (id) => {
      files.delete(id);
    },
  },
  // send "Authorization: Bearer <token>" from clients
  // swap in your own JWT or session middleware if you prefer.
  auth: bearerTokenAuth(process.env.UPLOAD_TOKEN ?? "dev-token"),
  // Optional (checked after upload)
  limits: {
    maxSizeBytes: 10 * 1024 * 1024,
    allowedMimeTypes: ["image/png", "image/jpeg", "application/pdf"],
  },
  // Optional: enable Kafka to kick-off afterUpload to a separate worker process, otherwise runs inline during POST /complete.

  kafka: { brokers: ["localhost:9092"], topic: "file-processing" },
  afterUpload: async (file) => {
    console.log("post-upload work for", file.id, file.key);
  },
});

// for frontend running in separate port
app.use(
    cors({
        origin: [
            "http://127.0.0.1:5500",
            "http://localhost:5500"
        ]
    })
);

// upload routes mounted
app.use("/files", uploader.router());

app.get("/debug/files", (req, res) => {
  res.json(Array.from(files.values()));
});

app.get("/debug/file/:id", (req, res) => {
  res.json(files.get(req.params.id));
});

const port = Number(process.env.PORT ?? 3000);

async function start() {
  // connect the Kafka producer
  // this web server only publishes.
  await uploader.ready();
  app.listen(port, () => {
    console.log("lb-upload-infra example on http://localhost:" + port + "/files");
  });
}

// connection closure
async function shutdown() {
  console.log("shutting down...");
  await uploader.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

start().catch((err) => {
  console.error("failed to start", err);
  process.exit(1);
});
