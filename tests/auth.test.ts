import { describe, it, expect, vi } from "vitest";
import express from "express";
import request from "supertest";
import { bearerTokenAuth } from "../src/auth";
import { UploaderImpl } from "../src/uploader";
import type { FileMetadata, Logger, MetadataStore } from "../src/types";
import type { S3Service } from "../src/s3-service";

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

function createS3() {
 return {
 bucketName: "test-bucket",
 createPresignedUploadUrl: vi.fn(async (key: string) => "https://upload.example/" + key),
 createPresignedDownloadUrl: vi.fn(async (key: string) => "https://download.example/" + key),
 head: vi.fn(async () => ({ exists: true, size: 42, etag: "e", contentType: "image/png" })),
 deleteObject: vi.fn(async () => {}),
 buildObjectUrl: vi.fn((key: string) => "https://bucket.example/" + key),
 } as unknown as S3Service;
}

function buildApp(authTokens: string | string[]) {
 const { store } = createStore();
 const u = new UploaderImpl({
 s3: createS3(),
 store,
 auth: [bearerTokenAuth(authTokens)],
 keyPrefix: "uploads/",
 uploadUrlExpiresIn: 900,
 logger,
 });
 const app = express();
 app.use("/files", u.router());
 return app;
}

describe("bearerTokenAuth", () => {
 it("throws when constructed without a token", () => {
 expect(() => bearerTokenAuth("")).toThrow();
 expect(() => bearerTokenAuth([])).toThrow();
 });

 it("rejects requests with no Authorization header", async () => {
 const res = await request(buildApp("secret"))
 .post("/files/upload-url")
 .send({ originalName: "a.png", mimeType: "image/png" });
 expect(res.status).toBe(401);
 expect(res.body.error.code).toBe("unauthorized");
 });

 it("rejects requests with the wrong token", async () => {
 const res = await request(buildApp("secret"))
 .post("/files/upload-url")
 .set("Authorization", "Bearer nope")
 .send({ originalName: "a.png", mimeType: "image/png" });
 expect(res.status).toBe(401);
 });

 it("allows requests with a valid token", async () => {
 const res = await request(buildApp("secret"))
 .post("/files/upload-url")
 .set("Authorization", "Bearer secret")
 .send({ originalName: "a.png", mimeType: "image/png" });
 expect(res.status).toBe(201);
 expect(res.body.id).toBeTruthy();
 });

 it("supports multiple accepted tokens", async () => {
 const res = await request(buildApp(["t1", "t2"]))
 .post("/files/upload-url")
 .set("Authorization", "Bearer t2")
 .send({ originalName: "a.png", mimeType: "image/png" });
 expect(res.status).toBe(201);
 });

 it("protects every route including GET", async () => {
 const res = await request(buildApp("secret")).get("/files/anything");
 expect(res.status).toBe(401);
 });
});
