require("dotenv/config");

const mongoose = require("mongoose");
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const pdf = require("pdf-parse");
const { createUploader } = require("lb-upload-infra");
const { PdfSummary } = require("./models/PdfSummary");

const AWS_REGION = process.env.AWS_REGION;
const S3_BUCKET = process.env.S3_BUCKET;
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;
const KAFKA_BROKERS = process.env.KAFKA_BROKERS;
const KAFKA_TOPIC = process.env.KAFKA_TOPIC;
const KAFKA_DLQ = process.env.KAFKA_DLQ;
const MONGO_URL = process.env.MONGO_URL;

// used to download the uploaded PDF from s3, not using preSigned URL obviously as its needed in the backend-worker not client.
const s3 = new S3Client({
  region: AWS_REGION,
  credentials: { accessKeyId: AWS_ACCESS_KEY_ID, secretAccessKey: AWS_SECRET_ACCESS_KEY },
});

async function downloadObject(bucket, key) {
  const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const bytes = await res.Body.transformToByteArray();
  return Buffer.from(bytes);
}

function words(text) {
  return text.trim().split(/\s+/).filter((word) => word.length > 0);
}

async function main() {
  await mongoose.connect(MONGO_URL);
  console.log("worker connected to MongoDB");

  const uploader = createUploader({
    s3: {
      region: AWS_REGION,
      bucket: S3_BUCKET,
      credentials: { accessKeyId: AWS_ACCESS_KEY_ID, secretAccessKey: AWS_SECRET_ACCESS_KEY },
    },
    kafka: {
      brokers: KAFKA_BROKERS.split(","),
      topic: KAFKA_TOPIC,
      deadLetterTopic: KAFKA_DLQ,
    },
    // worker doesnt need metadata handling
    metadata: {
      save: async () => {},
      find: async () => null,
      delete: async () => {},
    },
    // download the PDF --> read the first three words, and save in Mongo with a reference back to the file id.
    afterUpload: async (file) => {
      console.log("processing", file.id, file.originalName);
      const buffer = await downloadObject(file.bucket, file.key);
      const parsed = await pdf(buffer);
      const allWords = words(parsed.text);
      const firstWords = allWords.length > 0 ? allWords.slice(0, 3).join(" ") : "(no extractable text)";
      await PdfSummary.updateOne(
        { file: file.id },
        { $set: { firstWords, wordCount: allWords.length } },
        { upsert: true }
      );
      console.log("saved summary for", file.id, "->", firstWords);
    },
  });

  await uploader.startWorker();
  console.log("worker started, waiting for uploads...");

  const shutdown = async () => {
    await uploader.close();
    await mongoose.disconnect();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("worker failed to start", err);
  process.exit(1);
});
