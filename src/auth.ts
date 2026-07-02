import type { RequestHandler } from "express";
import { timingSafeEqual } from "node:crypto";
import { UnauthorizedError } from "./errors";


// secret values comparison (** Without leaking timing info***)
function safeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}

// Requests format in HEADER ==> Authorization: Bearer <token>
// Self-written middlewares can be passed as well to createUploader instead.

export function bearerTokenAuth(token: string | string[]): RequestHandler {
  // Accepts either a single token or an array of tokens.
  let rawTokens: string[];
  if (Array.isArray(token)) rawTokens = token;
  else rawTokens = [token];

  // remove the empty strings
  const tokens = rawTokens.filter((t) => typeof t === "string" && t.length > 0);
  if (tokens.length === 0) {
    throw new Error("bearerTokenAuth requires at least one non-empty token");
  }

  return (req, _res, next) => {
    const header = req.headers.authorization ?? "";

    // Parse the token
    const match = /^Bearer\s+(.+)$/i.exec(header);
    let provided = "";
    if (match) {
      provided = match[1].trim();
    }

    // Check the provided token against each of the accepted tokens
    let ok = false;
    if (provided) {
      for (const t of tokens) {
        if (safeEqual(t, provided)) {
          ok = true;
          break;
        }
      }
    }

    if (ok) {
      next();
      return;
    }
    next(new UnauthorizedError());
  };
}
