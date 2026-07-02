import { S3Client, HeadObjectCommand, DeleteObjectCommand, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import type { NormalizedS3Config } from "./config";
import { normalizeEtag } from "./metadata";

// s3 head object
export interface HeadResult {
  exists: boolean;
  size: number;
  etag: string;
  contentType?: string;
}

export class S3Service {
  private client: S3Client;
  private bucket: string;
  private region: string;
  private endpoint?: string;
  private forcePathStyle: boolean;
  private uploadExpiresIn: number;
  private downloadExpiresIn: number;

  // the arg(client) is for testing purposes only with a fake client.
  constructor(config: NormalizedS3Config, client?: S3Client) {
    this.bucket = config.bucket;
    this.region = config.region;
    this.endpoint = config.endpoint;
    this.forcePathStyle = config.forcePathStyle;
    this.uploadExpiresIn = config.uploadUrlExpiresIn;
    this.downloadExpiresIn = config.downloadUrlExpiresIn;

    if (client) {
      this.client = client;
    } else {
      this.client = new S3Client({
        region: config.region,
        endpoint: config.endpoint,
        forcePathStyle: config.forcePathStyle,
        credentials: config.credentials,
      });
    }
  }

  get bucketName(): string {
    return this.bucket;
  }

  // upload URL
  async createPresignedUploadUrl(key: string, contentType: string): Promise<string> {
    const command = new PutObjectCommand({
      Bucket: this.bucket,
      Key: key,
      ContentType: contentType,
    });
    return getSignedUrl(this.client, command, { expiresIn: this.uploadExpiresIn });
  }
  
  // download URL
  async createPresignedDownloadUrl(key: string): Promise<string> {
    const command = new GetObjectCommand({ Bucket: this.bucket, Key: key });
    return getSignedUrl(this.client, command, { expiresIn: this.downloadExpiresIn });
  }

  // object existence check in s3
  async head(key: string): Promise<HeadResult> {
    try {
      const out = await this.client.send(
        new HeadObjectCommand({ Bucket: this.bucket, Key: key })
      );
      return {
        exists: true,
        size: out.ContentLength ?? 0,
        etag: normalizeEtag(out.ETag),
        contentType: out.ContentType,
      };
    } catch (err) {
      if (isNotFound(err)) {
        return { exists: false, size: 0, etag: "" };
      }
      throw err;
    }
  }

  // deletes the object
  async deleteObject(key: string): Promise<void> {
    await this.client.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: key }));
  }

  // public URL of object (for publicly accessible bucket in s3)
  buildObjectUrl(key: string): string {
    const encodedKey = key.split("/").map(encodeURIComponent).join("/");

    if (this.endpoint) {
      const base = this.endpoint.replace(/\/+$/, "");
      if (this.forcePathStyle) {
        return base + "/" + this.bucket + "/" + encodedKey;
      }
      return base + "/" + encodedKey;
    }

    if (this.forcePathStyle) {
      return "https://s3." + this.region + ".amazonaws.com/" + this.bucket + "/" + encodedKey;
    }
    return "https://" + this.bucket + ".s3." + this.region + ".amazonaws.com/" + encodedKey;
  }
}

// layered check of NOT FOUND error for surety, otherwise throw err
function isNotFound(err: unknown): boolean {
  const anyErr = err as any;
  if (!anyErr) {
    return false;
  }
  if (anyErr.name === "NotFound" || anyErr.name === "NoSuchKey") {
    return true;
  }
  if (anyErr.$metadata && anyErr.$metadata.httpStatusCode === 404) {
    return true;
  }
  return false;
}
