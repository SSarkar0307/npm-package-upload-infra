const { UploaderImpl } = require("../dist/uploader.js");
const { KafkaService } = require("../dist/kafka-service.js");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const logger = {
  info: (...a) => console.log("[info]", ...a),
  warn: (...a) => console.warn("[warn]", ...a),
  error: (...a) => console.error("[error]", ...a),
};

const kafkaConfig = {
  brokers: ["localhost:9092"],
  topic: "smoke-file-processing",
  clientId: "upload-kit-smoke",
  groupId: "upload-kit-smoke-" + Date.now(),
};
const kafka = new KafkaService(kafkaConfig, logger);

const fakeS3 = {
  bucketName: "smoke-bucket",
  head: async () => ({ exists: true, size: 123, etag: "smoke-etag", contentType: "image/png" }),
  buildObjectUrl: (key) => "https://smoke-bucket.s3.amazonaws.com/" + key,
  createPresignedUploadUrl: async (key) => "https://upload.example/" + key,
  createPresignedDownloadUrl: async (key) => "https://download.example/" + key,
  deleteObject: async () => {},
};

const store = new Map();
const metadataStore = {
  save: async (f) => { store.set(f.id, f); },
  find: async (id) => store.get(id) ?? null,
  delete: async (id) => { store.delete(id); },
};

let receivedFile = null;
const afterUpload = async (file) => {
  console.log("[afterUpload] worker received", file.id, file.originalName);
  receivedFile = file;
};

const uploader = new UploaderImpl({
  s3: fakeS3,
  store: metadataStore,
  kafka,
  afterUpload,
  keyPrefix: "uploads/",
  uploadUrlExpiresIn: 900,
  logger,
});

(async () => {
  console.log("starting real Kafka worker...");
  await uploader.startWorker();
  await sleep(5000);
  console.log("running completeUpload (saves metadata + publishes to Kafka)...");
  const completed = await uploader.completeUpload({
    id: "smoke-1",
    key: "uploads/smoke-1/a.png",
  });
  console.log("completeUpload returned id=" + completed.id + " size=" + completed.size);
  const deadline = Date.now() + 40000;
  while (!receivedFile && Date.now() < deadline) {
    await sleep(2000);
    if (!receivedFile) { try { await kafka.publish(completed); } catch (e) {} }
  }
  if (!receivedFile) throw new Error("timeout: worker did not receive the published message");
  if (receivedFile.id !== "smoke-1" || receivedFile.etag !== "smoke-etag") {
    throw new Error("received metadata did not match what was published");
  }
  if (!store.has("smoke-1")) throw new Error("metadata was not saved");
  console.log("SMOKE OK: full upload->save->publish->worker->afterUpload round-trip through REAL Kafka succeeded");
  await uploader.close();
  process.exit(0);
})().catch(async (err) => {
  console.error("SMOKE FAILED:", err && err.message ? err.message : err);
  try { await uploader.close(); } catch (e) {}
  process.exit(1);
});
