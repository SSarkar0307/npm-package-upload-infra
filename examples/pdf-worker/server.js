require("dotenv/config");

const path = require("node:path");
const express = require("express");
const mongoose = require("mongoose");
const { createUploader, bearerTokenAuth } = require("lb-upload-infra");
const { File } = require("./models/File");
const { PdfSummary } = require("./models/PdfSummary");
const cors = require("cors");

const AWS_REGION = process.env.AWS_REGION;
const S3_BUCKET = process.env.S3_BUCKET;
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;
const KAFKA_BROKERS = process.env.KAFKA_BROKERS;
const KAFKA_TOPIC = process.env.KAFKA_TOPIC;
const KAFKA_DLQ = process.env.KAFKA_DLQ;
const UPLOAD_TOKEN = process.env.UPLOAD_TOKEN;
const MONGO_URL = process.env.MONGO_URL;
const PORT = process.env.PORT;

async function main() {
  await mongoose.connect(MONGO_URL);
  console.log("connected to MongoDB");

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
    // Metadata in mongodb, use file id as the _id.
    metadata: {
      save: async (file) => {
        await File.updateOne(
          { _id: file.id },
          {
            $set: {
              key: file.key,
              bucket: file.bucket,
              originalName: file.originalName,
              mimeType: file.mimeType,
              size: file.size,
              etag: file.etag,
              uploadedAt: new Date(file.uploadedAt),
            },
          },
          { upsert: true }
        );
      },
      find: async (id) => {
        const doc = await File.findById(id).lean();
        if (!doc) return null;
        return {
          id: doc._id,
          key: doc.key,
          bucket: doc.bucket,
          originalName: doc.originalName,
          mimeType: doc.mimeType,
          size: doc.size,
          etag: doc.etag,
          uploadedAt: doc.uploadedAt.toISOString(),
        };
      },
      delete: async (id) => {
        await File.deleteOne({ _id: id });
      },
    },
    auth: bearerTokenAuth(UPLOAD_TOKEN),
  });

  const app = express();

  app.use(cors());

  app.get("/", (_req, res) => {
    res.sendFile(path.join(__dirname, "frontend.html"));
  });

  app.use("/files", uploader.router());

  // read the extracted summary the worker saved.
  app.get("/summary/:id", async (req, res) => {
    const fileId = String(req.params.id);
    const summary = await PdfSummary.findOne({ file: fileId }).lean();
    if (!summary) {
      res.status(202).json({ status: "pending" });
      return;
    }
    res.json({ status: "done", firstWords: summary.firstWords, wordCount: summary.wordCount });
  });

  await uploader.ready();
  app.listen(PORT, () => {
    console.log("server on http://localhost:" + PORT);
  });

  const shutdown = async () => {
    await uploader.close();
    await mongoose.disconnect();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("server failed to start", err);
  process.exit(1);
});
