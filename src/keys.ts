import { randomUUID } from "node:crypto";

const UNSAFE_CHARS = /[^a-zA-Z0-9._-]+/g;

export function sanitizeFileName(name: string): string {
  let base = (name ?? "").trim();
  if (base === "") {
    base = "file";
  }

  const parts = base.split(/[\\/]/);
  const lastSegment = parts[parts.length - 1];

  let cleaned = lastSegment.replace(UNSAFE_CHARS, "-");
  cleaned = cleaned.replace(/^-+|-+$/g, "");

  if (cleaned === "") {
    return "file";
  }
  return cleaned;
}

export function generateId(): string {
  return randomUUID();
}

// S3 object key format --> uploads/<id>/<file name>.
export function buildObjectKey(prefix: string, id: string, originalName: string): string {
  const safeName = sanitizeFileName(originalName);
  return prefix + id + "/" + safeName;
}

export function getFileNameFromKey(key: string): string {
  const parts = key.split("/");
  const last = parts[parts.length - 1];
  if (!last) {
    return "file";
  }
  return last;
}
