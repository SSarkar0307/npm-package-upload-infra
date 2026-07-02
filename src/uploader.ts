import type { RequestHandler, Router } from "express";
import { createRouter } from "./router";
import { buildFileMetadata } from "./metadata";
import { buildObjectKey, generateId, getFileNameFromKey } from "./keys";
import { FileTooLargeError, InvalidFileTypeError, NotFoundError, UploadIncompleteError, ValidationError } from "./errors";
import type { S3Service } from "./s3-service";
import type { KafkaService } from "./kafka-service";
import type { AfterUploadCallback, CompleteUploadInput, CreateUploadUrlInput, CreateUploadUrlResult, FileMetadata, FileWithDownloadUrl, Logger, MetadataStore, UploadLimits, Uploader } from "./types";

export interface UploaderDeps {
  s3: S3Service;
  store: MetadataStore;
  afterUpload?: AfterUploadCallback;
  kafka?: KafkaService;
  auth?: RequestHandler[];
  limits?: UploadLimits;
  keyPrefix: string;
  uploadUrlExpiresIn: number;
  logger: Logger;
}

// main class
export class UploaderImpl implements Uploader {
  private s3: S3Service;
  private store: MetadataStore;
  private afterUpload?: AfterUploadCallback;
  private kafka?: KafkaService;
  private auth?: RequestHandler[];
  private limits?: UploadLimits;
  private keyPrefix: string;
  private uploadUrlExpiresIn: number;
  logger: Logger;

  constructor(deps: UploaderDeps) {
    this.s3 = deps.s3;
    this.store = deps.store;
    this.afterUpload = deps.afterUpload;
    this.kafka = deps.kafka;
    this.auth = deps.auth;
    this.limits = deps.limits;
    this.keyPrefix = deps.keyPrefix;
    this.uploadUrlExpiresIn = deps.uploadUrlExpiresIn;
    this.logger = deps.logger;
  }

  // build all the upload/download routes
  router(): Router {
    return createRouter(this, this.logger, this.auth);
  }

  // generate the presigned URL
  async createUploadUrl(input: CreateUploadUrlInput): Promise<CreateUploadUrlResult> {
    const originalName = requireString(input?.originalName, "originalName");
    const mimeType = requireString(input?.mimeType, "mimeType");

    const size = input?.size;
    if (size !== undefined && (typeof size !== "number" || size < 0)) {
      throw new ValidationError("size must be a non-negative number");
    }

    const id = generateId();
    const key = buildObjectKey(this.keyPrefix, id, originalName);
    const uploadUrl = await this.s3.createPresignedUploadUrl(key, mimeType);
    this.logger.info("generated upload URL", { id, key });
    return { id, key, uploadUrl, expiresIn: this.uploadUrlExpiresIn };
  }

  // after upload, verify the object in S3, then save the metadata and either publish to Kafka or run the callback
  async completeUpload(input: CompleteUploadInput): Promise<FileMetadata> {
    const id = requireString(input?.id, "id");
    const key = requireString(input?.key, "key");

    // verify the key
    const expectedStart = this.keyPrefix + id + "/";
    if (!key.startsWith(expectedStart)) {
      throw new ValidationError("key does not match id");
    }

    // fetch head and check object existence in s3
    const head = await this.s3.head(key);
    if (!head.exists) {
      throw new UploadIncompleteError();
    }

    // check content-type from head
    let mimeType = head.contentType;
    if (!mimeType) {
      mimeType = "application/octet-stream";
    }

    await this.checkLimits(key, head.size, mimeType);
    const originalName = getFileNameFromKey(key);

    const file = buildFileMetadata({
      id,
      key,
      bucket: this.s3.bucketName,
      originalName,
      mimeType,
      size: head.size,
      etag: head.etag,
    });

    this.logger.info("verified upload", { id, key, size: file.size, mimeType: file.mimeType });

    await this.store.save(file);
    this.logger.info("saved metadata", { id });

    await this.dispatch(file);
    return file;
  }

  // returns metadata + presigned download URL
  async getFile(id: string): Promise<FileWithDownloadUrl> {
    const fileId = requireString(id, "id");
    const file = await this.store.find(fileId);
    if (!file) {
      throw new NotFoundError();
    }
    const downloadUrl = await this.s3.createPresignedDownloadUrl(file.key);
    return { ...file, downloadUrl };
  }

  // delete from s3, then delete the metadata
  async deleteFile(id: string): Promise<void> {
    const fileId = requireString(id, "id");
    const file = await this.store.find(fileId);
    if (!file) {
      throw new NotFoundError();
    }
    await this.s3.deleteObject(file.key);
    await this.store.delete(fileId);
    this.logger.info("deleted file", { id: fileId });
  }

  // only if kafka and workers are configured and setup
  async startWorker(): Promise<void> {
    if (!this.kafka) {
      this.logger.warn("startWorker called but Kafka is not configured");
      return;
    }
    if (!this.afterUpload) {
      this.logger.warn("startWorker called but no afterUpload callback was provided");
      return;
    }
    await this.kafka.startWorker(this.afterUpload);
  }

  // builds the connection
  async ready(): Promise<void> {
    if (this.kafka) {
      await this.kafka.connectProducer();
    }
  }

  // closure of connections
  async close(): Promise<void> {
    if (this.kafka) {
      await this.kafka.close();
    }
  }

  // check and handle optional size and type limits
  private async checkLimits(key: string, size: number, mimeType: string): Promise<void> {
    if (!this.limits) {
      return;
    }

    if (this.limits.maxSizeBytes !== undefined && size > this.limits.maxSizeBytes) {
      await this.safeDeleteObject(key);
      throw new FileTooLargeError(
        "uploaded file is " + size + " bytes, over the limit of " + this.limits.maxSizeBytes
      );
    }

    const allowed = this.limits.allowedMimeTypes;
    if (allowed && allowed.length > 0) {
      if (!allowed.includes(mimeType)) {
        await this.safeDeleteObject(key);
        throw new InvalidFileTypeError("file type " + mimeType + " is not allowed");
      }
    }
  }

  // to reject and upload
  private async safeDeleteObject(key: string): Promise<void> {
    try {
      await this.s3.deleteObject(key);
    } catch (err) {
      this.logger.error("failed to delete rejected upload from S3", err);
    }
  }

  // after file saved either publish a mesage or run the callback right here
  private async dispatch(file: FileMetadata): Promise<void> {
    if (this.kafka) {
      await this.kafka.publish(file);
      return;
    }
    if (this.afterUpload) {
      await this.afterUpload(file);
      this.logger.info("afterUpload callback completed", { id: file.id });
    }
  }
}

// keep the value as a non-empty string, otherwise, throws a 400 error.
function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new ValidationError(field + " is required and must be a non-empty string");
  }
  return value;
}
