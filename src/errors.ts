// base error type for the whole package.
// primary use in router

export class UploadKitError extends Error {
  status: number;
  code: string;

  constructor(message: string, status = 500, code = "upload_kit_error") {
    super(message);
    this.name = "UploadKitError";
    this.status = status;
    this.code = code;
  }
}

// Bad or missing request data. -> HTTP 400.
export class ValidationError extends UploadKitError {
  constructor(message: string) {
    super(message, 400, "validation_error");
    this.name = "ValidationError";
  }
}

// Request is not allowed. -> HTTP 401.
export class UnauthorizedError extends UploadKitError {
  constructor(message = "Unauthorized") {
    super(message, 401, "unauthorized");
    this.name = "UnauthorizedError";
  }
}

// Not found. -> HTTP 404. (here for file specifically)
export class NotFoundError extends UploadKitError {
  constructor(message = "File not found") {
    super(message, 404, "not_found");
    this.name = "NotFoundError";
  }
}

// The object not found in S3. -> HTTP 409.
export class UploadIncompleteError extends UploadKitError {
  constructor(message = "Uploaded object was not found in S3") {
    super(message, 409, "upload_incomplete");
    this.name = "UploadIncompleteError";
  }
}

// The file size is larger the max size allowed. -> HTTP 413.
export class FileTooLargeError extends UploadKitError {
  constructor(message = "Uploaded file is too large") {
    super(message, 413, "file_too_large");
    this.name = "FileTooLargeError";
  }
}

// Restricted file type. -> HTTP 415.
export class InvalidFileTypeError extends UploadKitError {
  constructor(message = "Uploaded file type is not allowed") {
    super(message, 415, "invalid_file_type");
    this.name = "InvalidFileTypeError";
  }
}
