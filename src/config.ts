import { ValidationError } from "./errors";
import type { RequestHandler } from "express";
import type { AfterUploadCallback, AuthMiddleware, KafkaConfig, Logger, MetadataStore, UploadLimits, UploaderConfig } from "./types";
import type { SASLOptions } from "kafkajs";

// follow fixed configuration structures

// s3 interface config 
export interface NormalizedS3Config {
  region: string;
  bucket: string;
  credentials?: { accessKeyId: string; secretAccessKey: string; sessionToken?: string };
  endpoint?: string;
  forcePathStyle: boolean;
  keyPrefix: string;
  uploadUrlExpiresIn: number;
  downloadUrlExpiresIn: number;
}

// kafka interface config (although optional)
export interface NormalizedKafkaConfig {
  brokers: string[];
  topic: string;
  clientId: string;
  groupId: string;
  ssl?: boolean;
  sasl?: SASLOptions;
  deadLetterTopic?: string;
}

// main config
export interface NormalizedConfig {
  s3: NormalizedS3Config;
  metadata: MetadataStore;
  kafka?: NormalizedKafkaConfig;
  afterUpload?: AfterUploadCallback;
  auth?: RequestHandler[];
  limits?: UploadLimits;
  logger: Logger;
}

// check non empty string
function isNonEmptyString(value: unknown): boolean {
  if (typeof value !== "string") return false;
  return value.trim().length > 0;
}

// Logger writes to the console (can be used if dev doesnt pass its own logger)
function defaultLogger(): Logger {
  const prefix = "[lb-upload-infra]";
  return {
    info: (message, meta) => console.log(prefix, message, meta ?? ""),
    warn: (message, meta) => console.warn(prefix, message, meta ?? ""),
    error: (message, meta) => console.error(prefix, message, meta ?? ""),
  };
}

// normalize the prefix (must have / at end)
function normalizeKeyPrefix(prefix: string | undefined): string {
  if (prefix === undefined) return "uploads/";

  const trimmed = prefix.trim();
  if (trimmed === "") return "";

  if (trimmed.endsWith("/")) return trimmed;

  return trimmed + "/";
}

// auth can accept an array of middlewares (or undefined).
function normalizeAuth(auth: AuthMiddleware | AuthMiddleware[] | undefined): RequestHandler[] | undefined {
  if (auth === undefined) return undefined;
  
  let list: AuthMiddleware[];

  if (Array.isArray(auth)) list = auth;
  else list = [auth];
  

  if (list.length === 0 || !list.every((m) => typeof m === "function")) {
    throw new ValidationError(
      "config.auth must be a middleware function or an array of middleware functions"
    );
  }
  return list;
}

// checks the optional upload limits and returns them (or undefined).
function normalizeLimits(limits: UploadLimits | undefined): UploadLimits | undefined {
  if (limits === undefined) return undefined;
  
  if (limits.maxSizeBytes !== undefined) {
    if (typeof limits.maxSizeBytes !== "number" || !Number.isFinite(limits.maxSizeBytes) || limits.maxSizeBytes <= 0) {
      throw new ValidationError("config.limits.maxSizeBytes must be a positive number");
    }
  }

  if (limits.allowedMimeTypes !== undefined) {
    if (!Array.isArray(limits.allowedMimeTypes) || limits.allowedMimeTypes.length === 0) {
      throw new ValidationError("config.limits.allowedMimeTypes must be a non-empty array");
    }
    if (!limits.allowedMimeTypes.every((t) => isNonEmptyString(t))) {
      throw new ValidationError("config.limits.allowedMimeTypes must contain non-empty strings");
    }
  }

  return {
    maxSizeBytes: limits.maxSizeBytes,
    allowedMimeTypes: limits.allowedMimeTypes,
  };
}

// field validation - kafka
function normalizeKafka(kafka: KafkaConfig): NormalizedKafkaConfig {
  if (!Array.isArray(kafka.brokers) || kafka.brokers.length === 0) {
    throw new ValidationError("config.kafka.brokers must be a non-empty array");
  }
  if (!kafka.brokers.every((b) => isNonEmptyString(b))) {
    throw new ValidationError("config.kafka.brokers must contain non-empty strings");
  }
  if (!isNonEmptyString(kafka.topic)) {
    throw new ValidationError("config.kafka.topic is required");
  }
  if (kafka.deadLetterTopic !== undefined && !isNonEmptyString(kafka.deadLetterTopic)) {
    throw new ValidationError("config.kafka.deadLetterTopic must be a non-empty string");
  }

  return {
    brokers: kafka.brokers,
    topic: kafka.topic,
    clientId: kafka.clientId ?? "lb-upload-infra",
    groupId: kafka.groupId ?? "lb-upload-infra-workers",
    ssl: kafka.ssl,
    sasl: kafka.sasl,
    deadLetterTopic: kafka.deadLetterTopic,
  };
}

// field validation & fill in default values -s3
export function normalizeConfig(config: UploaderConfig): NormalizedConfig {
  if (!config || typeof config !== "object") {
    throw new ValidationError("createUploader requires a configuration object");
  }

  const s3 = config.s3;
  if (!s3 || typeof s3 !== "object") {
    throw new ValidationError("config.s3 is required");
  }
  if (!isNonEmptyString(s3.region)) {
    throw new ValidationError("config.s3.region is required");
  }
  if (!isNonEmptyString(s3.bucket)) {
    throw new ValidationError("config.s3.bucket is required");
  }
  if (s3.credentials !== undefined) {
    if (
      !isNonEmptyString(s3.credentials.accessKeyId) ||
      !isNonEmptyString(s3.credentials.secretAccessKey)
    ) {
      throw new ValidationError(
        "config.s3.credentials must include accessKeyId and secretAccessKey"
      );
    }
  }

  const metadata = config.metadata;
  if (!metadata || typeof metadata !== "object") {
    throw new ValidationError("config.metadata is required");
  }
  // metadata functions
  if (typeof metadata.save !== "function") {
    throw new ValidationError("config.metadata.save must be a function");
  }
  if (typeof metadata.find !== "function") {
    throw new ValidationError("config.metadata.find must be a function");
  }
  if (typeof metadata.delete !== "function") {
    throw new ValidationError("config.metadata.delete must be a function");
  }

  if (config.afterUpload !== undefined && typeof config.afterUpload !== "function") {
    throw new ValidationError("config.afterUpload must be a function");
  }

  const auth = normalizeAuth(config.auth);
  const limits = normalizeLimits(config.limits);

  // keep default of 15 minutes, or could be less depending on security measures
  const uploadUrlExpiresIn = s3.uploadUrlExpiresIn ?? 900;
  const downloadUrlExpiresIn = s3.downloadUrlExpiresIn ?? 900;
  if (!Number.isFinite(uploadUrlExpiresIn) || uploadUrlExpiresIn <= 0) {
    throw new ValidationError("config.s3.uploadUrlExpiresIn must be a positive number");
  }
  if (!Number.isFinite(downloadUrlExpiresIn) || downloadUrlExpiresIn <= 0) {
    throw new ValidationError("config.s3.downloadUrlExpiresIn must be a positive number");
  }

  let kafka: NormalizedKafkaConfig | undefined;
  if (config.kafka !== undefined) {
    kafka = normalizeKafka(config.kafka);
  }

  return {
    s3: {
      region: s3.region,
      bucket: s3.bucket,
      credentials: s3.credentials,
      endpoint: s3.endpoint,
      forcePathStyle: s3.forcePathStyle ?? false,
      keyPrefix: normalizeKeyPrefix(s3.keyPrefix),
      uploadUrlExpiresIn,
      downloadUrlExpiresIn,
    },
    metadata,
    kafka,
    afterUpload: config.afterUpload,
    auth,
    limits,
    logger: config.logger ?? defaultLogger(),
  };
}
