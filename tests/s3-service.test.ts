import { describe, it, expect, beforeEach } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import { S3Client, HeadObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { S3Service } from "../src/s3-service";
import type { NormalizedS3Config } from "../src/config";

const s3Mock = mockClient(S3Client);

const config: NormalizedS3Config = {
 region: "us-east-1",
 bucket: "test-bucket",
 credentials: { accessKeyId: "AKIAEXAMPLE", secretAccessKey: "secretexample" },
 forcePathStyle: false,
 keyPrefix: "uploads/",
 uploadUrlExpiresIn: 900,
 downloadUrlExpiresIn: 900,
};

beforeEach(() => {
 s3Mock.reset();
});

describe("S3Service", () => {
 it("creates a presigned upload URL", async () => {
 const s3 = new S3Service(config);
 const url = await s3.createPresignedUploadUrl("uploads/abc/file.png", "image/png");
 expect(url).toContain("test-bucket");
 expect(url).toContain("uploads/abc/file.png");
 expect(url).toContain("X-Amz-Signature");
 });

 it("creates a presigned download URL", async () => {
 const s3 = new S3Service(config);
 const url = await s3.createPresignedDownloadUrl("uploads/abc/file.png");
 expect(url).toContain("X-Amz-Signature");
 expect(url).toContain("uploads/abc/file.png");
 });

 it("reports an existing object via head and strips etag quotes", async () => {
 s3Mock.on(HeadObjectCommand).resolves({ ContentLength: 123, ETag: "\"abc123\"", ContentType: "image/png" });
 const s3 = new S3Service(config);
 const head = await s3.head("uploads/abc/file.png");
 expect(head.exists).toBe(true);
 expect(head.size).toBe(123);
 expect(head.etag).toBe("abc123");
 expect(head.contentType).toBe("image/png");
 });

 it("reports a missing object as not existing", async () => {
 const notFound = Object.assign(new Error("NotFound"), { name: "NotFound", $metadata: { httpStatusCode: 404 } });
 s3Mock.on(HeadObjectCommand).rejects(notFound);
 const s3 = new S3Service(config);
 const head = await s3.head("missing");
 expect(head.exists).toBe(false);
 });

 it("deletes an object", async () => {
 s3Mock.on(DeleteObjectCommand).resolves({});
 const s3 = new S3Service(config);
 await s3.deleteObject("uploads/abc/file.png");
 expect(s3Mock.commandCalls(DeleteObjectCommand)).toHaveLength(1);
 });

 it("builds a virtual-hosted object URL by default", () => {
 const s3 = new S3Service(config);
 expect(s3.buildObjectUrl("uploads/abc/file.png")).toBe(
 "https://test-bucket.s3.us-east-1.amazonaws.com/uploads/abc/file.png"
 );
 });

 it("builds a path-style object URL when configured", () => {
 const s3 = new S3Service({ ...config, forcePathStyle: true });
 expect(s3.buildObjectUrl("uploads/abc/file.png")).toBe(
 "https://s3.us-east-1.amazonaws.com/test-bucket/uploads/abc/file.png"
 );
 });
});
