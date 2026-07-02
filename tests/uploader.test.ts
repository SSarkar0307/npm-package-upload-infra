import { describe, it, expect, vi } from "vitest";
import { UploaderImpl } from "../src/uploader";
import {
  FileTooLargeError,
  InvalidFileTypeError,
  NotFoundError,
  UploadIncompleteError,
  ValidationError,
} from "../src/errors";
import type { FileMetadata, Logger, MetadataStore } from "../src/types";
import type { S3Service } from "../src/s3-service";
import type { KafkaService } from "../src/kafka-service";

const logger: Logger = { info: () => {}, warn: () => {}, error: () => {} };

function createStore() {
  const map = new Map<string, FileMetadata>();
  const store: MetadataStore = {
    save: vi.fn(async (f: FileMetadata) => {
      map.set(f.id, f);
    }),
    find: vi.fn(async (id: string) => map.get(id) ?? null),
    delete: vi.fn(async (id: string) => {
      map.delete(id);
    }),
  };
  return { map, store };
}

function createS3(head: { exists: boolean; size: number; etag: string; contentType?: string } = { exists: true, size: 42, etag: "etag123", contentType: "image/png" }) {
  return {
    bucketName: "test-bucket",
    createPresignedUploadUrl: vi.fn(async (key: string) => "https://upload.example/" + key),
    createPresignedDownloadUrl: vi.fn(async (key: string) => "https://download.example/" + key),
    head: vi.fn(async () => head),
    deleteObject: vi.fn(async () => {}),
    buildObjectUrl: vi.fn((key: string) => "https://bucket.example/" + key),
  } as unknown as S3Service;
}

function createKafka() {
  return {
    publish: vi.fn(async () => {}),
    startWorker: vi.fn(async () => {}),
    connectProducer: vi.fn(async () => {}),
    close: vi.fn(async () => {}),
  } as unknown as KafkaService;
}

const seedMeta = (): FileMetadata => ({
  id: "id1",
  key: "uploads/id1/a.png",
  bucket: "test-bucket",
  originalName: "a.png",
  mimeType: "image/png",
  size: 42,
  etag: "e",
  uploadedAt: new Date().toISOString(),
});

describe("UploaderImpl", () => {
  it("creates an upload URL with a prefixed, sanitized key", async () => {
    const { store } = createStore();
    const u = new UploaderImpl({ s3: createS3(), store, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger });
    const res = await u.createUploadUrl({ originalName: "my photo.png", mimeType: "image/png" });
    expect(res.id).toBeTruthy();
    expect(res.key).toMatch(/^uploads\/[^/]+\/my-photo\.png$/);
    expect(res.uploadUrl).toContain(res.key);
    expect(res.expiresIn).toBe(900);
  });

  it("rejects an upload URL request without originalName", async () => {
    const { store } = createStore();
    const u = new UploaderImpl({ s3: createS3(), store, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger });
    await expect(u.createUploadUrl({ mimeType: "image/png" } as any)).rejects.toBeInstanceOf(ValidationError);
  });

  it("completes an upload: derives name from key, mime from S3, no stored url", async () => {
    const { map, store } = createStore();
    const afterUpload = vi.fn(async () => {});
    const u = new UploaderImpl({ s3: createS3(), store, afterUpload, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger });
    const file = await u.completeUpload({ id: "id1", key: "uploads/id1/a.png" });
    expect(file.size).toBe(42);
    expect(file.etag).toBe("etag123");
    expect(file.bucket).toBe("test-bucket");
    expect(file.mimeType).toBe("image/png");
    expect(file.originalName).toBe("a.png");
    expect("url" in file).toBe(false);
    expect(map.get("id1")).toBeTruthy();
    expect(store.save).toHaveBeenCalledTimes(1);
    expect(afterUpload).toHaveBeenCalledTimes(1);
    expect(afterUpload).toHaveBeenCalledWith(expect.objectContaining({ id: "id1" }));
  });

  it("publishes to Kafka instead of running the callback inline when Kafka is configured", async () => {
    const { store } = createStore();
    const afterUpload = vi.fn(async () => {});
    const kafka = createKafka();
    const u = new UploaderImpl({ s3: createS3(), store, afterUpload, kafka, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger });
    await u.completeUpload({ id: "id1", key: "uploads/id1/a.png" });
    expect(kafka.publish).toHaveBeenCalledTimes(1);
    expect(afterUpload).not.toHaveBeenCalled();
  });

  it("rejects completion when the object is missing in S3", async () => {
    const { store } = createStore();
    const u = new UploaderImpl({ s3: createS3({ exists: false, size: 0, etag: "" }), store, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger });
    await expect(
      u.completeUpload({ id: "id1", key: "uploads/id1/a.png" })
    ).rejects.toBeInstanceOf(UploadIncompleteError);
    expect(store.save).not.toHaveBeenCalled();
  });

  it("rejects completion when the key does not match the id", async () => {
    const s3 = createS3();
    const { store } = createStore();
    const u = new UploaderImpl({ s3, store, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger });
    await expect(
      u.completeUpload({ id: "id1", key: "uploads/other/a.png" })
    ).rejects.toBeInstanceOf(ValidationError);
    expect(s3.head).not.toHaveBeenCalled();
  });

  it("rejects and deletes the object when it is larger than maxSizeBytes", async () => {
    const s3 = createS3({ exists: true, size: 42, etag: "e", contentType: "image/png" });
    const { store } = createStore();
    const u = new UploaderImpl({ s3, store, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger, limits: { maxSizeBytes: 10 } });
    await expect(
      u.completeUpload({ id: "id1", key: "uploads/id1/a.png" })
    ).rejects.toBeInstanceOf(FileTooLargeError);
    expect(s3.deleteObject).toHaveBeenCalledWith("uploads/id1/a.png");
    expect(store.save).not.toHaveBeenCalled();
  });

  it("rejects and deletes the object when the type is not allowed", async () => {
    const s3 = createS3({ exists: true, size: 42, etag: "e", contentType: "image/png" });
    const { store } = createStore();
    const u = new UploaderImpl({ s3, store, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger, limits: { allowedMimeTypes: ["image/jpeg"] } });
    await expect(
      u.completeUpload({ id: "id1", key: "uploads/id1/a.png" })
    ).rejects.toBeInstanceOf(InvalidFileTypeError);
    expect(s3.deleteObject).toHaveBeenCalledWith("uploads/id1/a.png");
    expect(store.save).not.toHaveBeenCalled();
  });

  it("returns metadata with a fresh download URL", async () => {
    const { map, store } = createStore();
    const u = new UploaderImpl({ s3: createS3(), store, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger });
    map.set("id1", seedMeta());
    const got = await u.getFile("id1");
    expect(got.id).toBe("id1");
    expect(got.downloadUrl).toBe("https://download.example/uploads/id1/a.png");
  });

  it("throws NotFound when getting a missing file", async () => {
    const { store } = createStore();
    const u = new UploaderImpl({ s3: createS3(), store, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger });
    await expect(u.getFile("missing")).rejects.toBeInstanceOf(NotFoundError);
  });

  it("deletes the S3 object and the metadata record", async () => {
    const { map, store } = createStore();
    const s3 = createS3();
    const u = new UploaderImpl({ s3, store, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger });
    map.set("id1", seedMeta());
    await u.deleteFile("id1");
    expect(s3.deleteObject).toHaveBeenCalledWith("uploads/id1/a.png");
    expect(map.has("id1")).toBe(false);
    await expect(u.deleteFile("id1")).rejects.toBeInstanceOf(NotFoundError);
  });

  it("starts a Kafka worker with the provided callback", async () => {
    const { store } = createStore();
    const afterUpload = vi.fn(async () => {});
    const kafka = createKafka();
    const u = new UploaderImpl({ s3: createS3(), store, afterUpload, kafka, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger });
    await u.startWorker();
    expect(kafka.startWorker).toHaveBeenCalledWith(afterUpload);
  });
});
