import type { FileMetadata } from "./types";

// remove the quotes of Etag from s3
export function normalizeEtag(etag: string | undefined): string {
  if (!etag) {
    return "";
  }
  return etag.replace(/^"+|"+$/g, "");
}

export interface BuildMetadataInput {
  id: string;
  key: string;
  bucket: string;
  originalName: string;
  mimeType: string;
  size: number;
  etag: string;
  uploadedAt?: Date;
}

// this object is stored and passed on to callback
export function buildFileMetadata(input: BuildMetadataInput): FileMetadata {
  let uploadedAt = input.uploadedAt;
  if (uploadedAt === undefined) {
    uploadedAt = new Date();
  }

  return {
    id: input.id,
    key: input.key,
    bucket: input.bucket,
    originalName: input.originalName,
    mimeType: input.mimeType,
    size: input.size,
    etag: normalizeEtag(input.etag),
    uploadedAt: uploadedAt.toISOString(),
  };
}
