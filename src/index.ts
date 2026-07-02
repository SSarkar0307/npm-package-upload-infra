import { normalizeConfig } from "./config";
import { S3Service } from "./s3-service";
import { KafkaService } from "./kafka-service";
import { UploaderImpl } from "./uploader";
import type { Uploader, UploaderConfig } from "./types";

// ENTRY POINT OF PACKAGE 

// Pass the config and it returns an uploader object. mount uploader.router() in the express app

// does NOT start any background work automatically. Need to call uploader.ready() before listening (it connects the Kafka producer). In a separate worker process call uploader.startWorker() to consume messages.

export function createUploader(config: UploaderConfig): Uploader {
  const normalized = normalizeConfig(config);

  const s3 = new S3Service(normalized.s3);

  // optional (builds only if Kafka was configured for use)
  let kafka: KafkaService | undefined;
  if (normalized.kafka) {
    kafka = new KafkaService(normalized.kafka, normalized.logger);
  }

  const uploader = new UploaderImpl({
    s3,
    store: normalized.metadata,
    afterUpload: normalized.afterUpload,
    kafka,
    auth: normalized.auth,
    limits: normalized.limits,
    keyPrefix: normalized.s3.keyPrefix,
    uploadUrlExpiresIn: normalized.s3.uploadUrlExpiresIn,
    logger: normalized.logger,
  });

  return uploader;
}

// re-exports for the dev
export { UploaderImpl } from "./uploader";
export { bearerTokenAuth } from "./auth";
export { UploadKitError, ValidationError, UnauthorizedError, NotFoundError, UploadIncompleteError, FileTooLargeError, InvalidFileTypeError } from "./errors";

export * from "./types";
