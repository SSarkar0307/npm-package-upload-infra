import { createUploader } from "../src";
import "dotenv/config";

// Dedicated Kafka worker process
// Consumer

const files = new Map<string, unknown>();

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
  kafka: {
    brokers: ["localhost:9092"],
    topic: "file-processing",
    // optional but recommended
    // deadLetterTopic: "file-processing-dlq",
  },
  afterUpload: async (file) => {
    console.log("worker processing", file.id, file.originalName);
  },
});

async function shutdown() {
  console.log("shutting down worker...");
  await uploader.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

uploader
  .startWorker()
  .then(() => console.log("upload-kit worker started"))
  .catch((err) => {
    console.error("worker failed to start", err);
    process.exit(1);
  });
