import { describe, it, expect, vi } from "vitest";
import express from "express";
import request from "supertest";
import { UploaderImpl } from "../src/uploader";
import type { FileMetadata, Logger, MetadataStore } from "../src/types";
import type { S3Service } from "../src/s3-service";

const logger: Logger = { info: () => {}, warn: () => {}, error: () => {} };

function createStore(seed?: FileMetadata) {
  const map = new Map<string, FileMetadata>();
  if (seed) map.set(seed.id, seed);
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

function createS3(head = { exists: true, size: 42, etag: "etag123", contentType: "image/png" }) {
  return {
    bucketName: "test-bucket",
    createPresignedUploadUrl: vi.fn(async (key: string) => "https://upload.example/" + key),
    createPresignedDownloadUrl: vi.fn(async (key: string) => "https://download.example/" + key),
    head: vi.fn(async () => head),
    deleteObject: vi.fn(async () => {}),
    buildObjectUrl: vi.fn((key: string) => "https://bucket.example/" + key),
  } as unknown as S3Service;
}

function buildApp(u: UploaderImpl) {
  const app = express();
  app.use("/files", u.router());
  return app;
}

const seed: FileMetadata = {
  id: "id1",
  key: "uploads/id1/a.png",
  bucket: "test-bucket",
  originalName: "a.png",
  mimeType: "image/png",
  size: 42,
  etag: "e",
  uploadedAt: new Date().toISOString(),
};

function makeUploader(store: MetadataStore, s3: S3Service) {
  return new UploaderImpl({ s3, store, keyPrefix: "uploads/", uploadUrlExpiresIn: 900, logger });
}

describe("router", () => {
  it("POST /upload-url returns 201 with id, key and uploadUrl", async () => {
    const { store } = createStore();
    const app = buildApp(makeUploader(store, createS3()));
    const res = await request(app).post("/files/upload-url").send({ originalName: "a.png", mimeType: "image/png" });
    expect(res.status).toBe(201);
    expect(res.body.id).toBeTruthy();
    expect(res.body.key).toContain("uploads/");
    expect(res.body.uploadUrl).toContain("https://upload.example/");
  });

  it("POST /upload-url returns 400 on invalid input", async () => {
    const { store } = createStore();
    const app = buildApp(makeUploader(store, createS3()));
    const res = await request(app).post("/files/upload-url").send({});
    expect(res.status).toBe(400);
    expect(res.body.error.code).toBe("validation_error");
  });

  it("POST /complete returns 200 and the stored metadata (only id + key needed)", async () => {
    const { store } = createStore();
    const app = buildApp(makeUploader(store, createS3()));
    const res = await request(app)
      .post("/files/complete")
      .send({ id: "id1", key: "uploads/id1/a.png" });
    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.file.id).toBe("id1");
    expect(res.body.file.size).toBe(42);
    expect(res.body.file.originalName).toBe("a.png");
    expect(res.body.file.mimeType).toBe("image/png");
  });

  it("POST /complete returns 409 when the object is missing", async () => {
    const { store } = createStore();
    const app = buildApp(makeUploader(store, createS3({ exists: false, size: 0, etag: "" } as any)));
    const res = await request(app)
      .post("/files/complete")
      .send({ id: "id1", key: "uploads/id1/a.png" });
    expect(res.status).toBe(409);
    expect(res.body.error.code).toBe("upload_incomplete");
  });

  it("GET /:id returns metadata with a download URL", async () => {
    const { store } = createStore(seed);
    const app = buildApp(makeUploader(store, createS3()));
    const res = await request(app).get("/files/id1");
    expect(res.status).toBe(200);
    expect(res.body.downloadUrl).toContain("https://download.example/");
    expect(res.body.id).toBe("id1");
  });

  it("GET /:id returns 404 for a missing file", async () => {
    const { store } = createStore();
    const app = buildApp(makeUploader(store, createS3()));
    const res = await request(app).get("/files/missing");
    expect(res.status).toBe(404);
    expect(res.body.error.code).toBe("not_found");
  });

  it("DELETE /:id removes the file and returns success", async () => {
    const { map, store } = createStore(seed);
    const app = buildApp(makeUploader(store, createS3()));
    const res = await request(app).delete("/files/id1");
    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(map.has("id1")).toBe(false);
  });
});
