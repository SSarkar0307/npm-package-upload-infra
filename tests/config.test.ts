import { describe, it, expect } from "vitest";
import { normalizeConfig } from "../src/config";
import { ValidationError } from "../src/errors";
import type { MetadataStore } from "../src/types";

const store: MetadataStore = {
  save: async () => {},
  find: async () => null,
  delete: async () => {},
};

const baseS3 = {
  region: "us-east-1",
  bucket: "bucket",
  credentials: { accessKeyId: "a", secretAccessKey: "b" },
};

describe("normalizeConfig", () => {
  it("applies defaults", () => {
    const c = normalizeConfig({ s3: { ...baseS3 }, metadata: store });
    expect(c.s3.keyPrefix).toBe("uploads/");
    expect(c.s3.uploadUrlExpiresIn).toBe(900);
    expect(c.s3.downloadUrlExpiresIn).toBe(900);
    expect(c.s3.forcePathStyle).toBe(false);
    expect(c.kafka).toBeUndefined();
    expect(typeof c.logger.info).toBe("function");
  });

  it("normalizes a key prefix without a trailing slash", () => {
    const c = normalizeConfig({ s3: { ...baseS3, keyPrefix: "media" }, metadata: store });
    expect(c.s3.keyPrefix).toBe("media/");
  });

  it("allows an empty key prefix", () => {
    const c = normalizeConfig({ s3: { ...baseS3, keyPrefix: "" }, metadata: store });
    expect(c.s3.keyPrefix).toBe("");
  });

  it("requires region and bucket", () => {
    expect(() => normalizeConfig({ s3: { ...baseS3, region: "" }, metadata: store })).toThrow(ValidationError);
    expect(() => normalizeConfig({ s3: { ...baseS3, bucket: "" }, metadata: store })).toThrow(ValidationError);
  });

  it("requires metadata functions", () => {
    expect(() => normalizeConfig({ s3: { ...baseS3 }, metadata: {} as MetadataStore })).toThrow(ValidationError);
  });

  it("validates and defaults kafka config", () => {
    expect(() =>
      normalizeConfig({ s3: { ...baseS3 }, metadata: store, kafka: { brokers: [], topic: "t" } })
    ).toThrow(ValidationError);
    const c = normalizeConfig({
      s3: { ...baseS3 },
      metadata: store,
      kafka: { brokers: ["localhost:9092"], topic: "files" },
    });
    expect(c.kafka?.clientId).toBe("lb-upload-infra");
    expect(c.kafka?.groupId).toBe("lb-upload-infra-workers");
    expect(c.kafka?.deadLetterTopic).toBeUndefined();
  });

  it("passes through a dead-letter topic when provided", () => {
    const c = normalizeConfig({
      s3: { ...baseS3 },
      metadata: store,
      kafka: { brokers: ["localhost:9092"], topic: "files", deadLetterTopic: "files-dlq" },
    });
    expect(c.kafka?.deadLetterTopic).toBe("files-dlq");
  });
});
