import express from "express";
import type { ErrorRequestHandler, RequestHandler, Router } from "express";
import { UploadKitError } from "./errors";
import type { Logger, Uploader } from "./types";

// in latest express versions, a route param can be received as a string or maybe an array of strings. this always gives us back a single string.
function firstParam(value: string | string[] | undefined): string {
  if (Array.isArray(value)) {
    if (value.length > 0) return value[0];
    return "";
  }

  if (value === undefined) return "";
  return value;
}

// 4 upload routes. 'core' is the main uploader. 'auth' is optional (list of middlewares).

export function createRouter(core: Uploader, logger: Logger, auth?: RequestHandler[]): Router {
  const router = express.Router();
  // auth first
  if (auth) {
    for (const middleware of auth) {
      router.use(middleware);
    }
  }

  router.use(express.json());

  // POST /upload-url -> returns a presigned upload URL.
  router.post("/upload-url", async (req, res, next) => {
    try {
      const result = await core.createUploadUrl(req.body);
      res.status(201).json(result);
    } catch (err) {
      next(err);
    }
  });

  // POST /complete -> verifies the upload and saves the metadata.
  router.post("/complete", async (req, res, next) => {
    try {
      const file = await core.completeUpload(req.body);
      res.status(200).json({ success: true, file });
    } catch (err) {
      next(err);
    }
  });

  // GET /:id -> returns the metadata +  download URL.
  router.get("/:id", async (req, res, next) => {
    try {
      const file = await core.getFile(firstParam(req.params.id));
      res.status(200).json(file);
    } catch (err) {
      next(err);
    }
  });

  // DELETE /:id -> deletes the file and its metadata.
  router.delete("/:id", async (req, res, next) => {
    try {
      await core.deleteFile(firstParam(req.params.id));
      res.status(200).json({ success: true });
    } catch (err) {
      next(err);
    }
  });

  const errorHandler: ErrorRequestHandler = (err, _req, res, _next) => {
    if (err instanceof UploadKitError) {
      res.status(err.status).json({ error: { code: err.code, message: err.message } });
      return;
    }
    logger.error("unexpected error handling upload request", err);
    res
      .status(500)
      .json({ error: { code: "internal_error", message: "Internal server error" } });
  };
  router.use(errorHandler);

  return router;
}
