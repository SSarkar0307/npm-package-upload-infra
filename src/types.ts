import type { RequestHandler, Router } from "express";
import type { SASLOptions } from "kafkajs";

// AWS s3 credentials
export interface S3Credentials {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
}

// s3 interface config 
export interface S3Config {
  region: string;
  bucket: string;
  credentials?: S3Credentials;
  endpoint?: string;
  forcePathStyle?: boolean;
  keyPrefix?: string;
  uploadUrlExpiresIn?: number;
  downloadUrlExpiresIn?: number;
}

// file metadata config 
export interface FileMetadata {
  id: string;
  key: string;
  bucket: string;
  originalName: string;
  mimeType: string;
  size: number;
  etag: string;
  uploadedAt: string;
}

// the function doesn't care where or how the metadata is stored.
export interface MetadataStore {
  save(file: FileMetadata): Promise<void> | void;
  find(id: string): Promise<FileMetadata | null | undefined> | FileMetadata | null | undefined;
  delete(id: string): Promise<void> | void;
}

// (Optional) limits
export interface UploadLimits {
  maxSizeBytes?: number;
  allowedMimeTypes?: string[];
}

// (Optional) Kafka
export interface KafkaConfig {
  brokers: string[];
  topic: string;
  clientId?: string;
  groupId?: string;
  ssl?: boolean;
  sasl?: SASLOptions;
  // recommended to set the config although optional
  deadLetterTopic?: string;
}

// the developer callback runs after upload completes
export type AfterUploadCallback = (file: FileMetadata) => Promise<void> | void;

export type AuthMiddleware = RequestHandler;

export interface Logger {
  info(message: string, meta?: unknown): void;
  warn(message: string, meta?: unknown): void;
  error(message: string, meta?: unknown): void;
}

export interface UploaderConfig {
  s3: S3Config;
  metadata: MetadataStore;
  kafka?: KafkaConfig;
  afterUpload?: AfterUploadCallback;
  auth?: AuthMiddleware | AuthMiddleware[];
  limits?: UploadLimits;
  logger?: Logger;
}

export interface CreateUploadUrlInput {
  originalName: string;
  mimeType: string;
  size?: number;
}

export interface CreateUploadUrlResult {
  id: string;
  key: string;
  uploadUrl: string;
  expiresIn: number;
}

// the client sends back the id and key it got from /upload-url. rest is read from s3.
export interface CompleteUploadInput {
  id: string;
  key: string;
}

export interface FileWithDownloadUrl extends FileMetadata {
  downloadUrl: string;
}

// message structure published to kafka
export interface UploadEvent {
  eventId: string;
  occurredAt: string;
  file: FileMetadata;
}

// structure of the uploader object returned by createUploader.
export interface Uploader {
  router(): Router;
  createUploadUrl(input: CreateUploadUrlInput): Promise<CreateUploadUrlResult>;
  completeUpload(input: CompleteUploadInput): Promise<FileMetadata>;
  getFile(id: string): Promise<FileWithDownloadUrl>;
  deleteFile(id: string): Promise<void>;
  startWorker(): Promise<void>;
  close(): Promise<void>;
  ready(): Promise<void>;
}
