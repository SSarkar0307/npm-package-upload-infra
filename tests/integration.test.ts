import { describe, it, expect, vi, beforeEach } from "vitest";
import express from "express";
import request from "supertest";
import { mockClient } from "aws-sdk-client-mock";
import { S3Client, HeadObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { createUploader } from "../src/index";
import type { FileMetadata, Logger, MetadataStore } from "../src/types";

const s3Mock = mockClient(S3Client);
const logger: Logger = { info: () => {}, warn: () => {}, error: () => {} };

function memoryStore() {
  const map = new Map<string, FileMetadata>();
  const store: MetadataStore = {
    save: async (f) => {
      map.set(f.id, f);
    },
    find: async (id) => map.get(id) ?? null,
    delete: async (id) => {
      map.delete(id);
    },
  };
  return { map, store };
}

beforeEach(() => {
  s3Mock.reset();
});

describe("end-to-end flow via createUploader", () => {
  it("runs upload-url -> complete -> get -> delete with the direct callback", async () => {
    const { map, store } = memoryStore();
    const afterUpload = vi.fn(async () => {});
    const uploader = createUploader({
      s3: {
        region: "us-east-1",
        bucket: "test-bucket",
        credentials: { accessKeyId: "AKIAEXAMPLE", secretAccessKey: "secretexample" },
      },
      metadata: store,
      afterUpload,
      logger,
    });

    const app = express();
    app.use("/files", uploader.router());

    const urlRes = await request(app)
      .post("/files/upload-url")
      .send({ originalName: "report.pdf", mimeType: "application/pdf" });
    expect(urlRes.status).toBe(201);
    const { id, key, uploadUrl } = urlRes.body;
    expect(uploadUrl).toContain("X-Amz-Signature");
    expect(key).toMatch(/^uploads\//);

    s3Mock.on(HeadObjectCommand).resolves({ ContentLength: 2048, ETag: "\"deadbeef\"", ContentType: "application/pdf" });

    // The client only needs to send back the id and key it received.
    const completeRes = await request(app)
      .post("/files/complete")
      .send({ id, key });
    expect(completeRes.status).toBe(200);
    expect(completeRes.body.file.size).toBe(2048);
    expect(completeRes.body.file.etag).toBe("deadbeef");
    expect(completeRes.body.file.mimeType).toBe("application/pdf");
    expect(completeRes.body.file.originalName).toBe("report.pdf");
    expect(afterUpload).toHaveBeenCalledTimes(1);
    expect(map.has(id)).toBe(true);

    const getRes = await request(app).get("/files/" + id);
    expect(getRes.status).toBe(200);
    expect(getRes.body.id).toBe(id);
    expect(getRes.body.downloadUrl).toContain("X-Amz-Signature");

    s3Mock.on(DeleteObjectCommand).resolves({});
    const delRes = await request(app).delete("/files/" + id);
    expect(delRes.status).toBe(200);
    expect(delRes.body.success).toBe(true);
    expect(map.has(id)).toBe(false);
    expect(s3Mock.commandCalls(DeleteObjectCommand)).toHaveLength(1);
  });
});
