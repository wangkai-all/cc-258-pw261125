'use strict';

/**
 * Source-level errors. Every error carries enough structure for the coordinator
 * to persist a resumable failure position and for the UI to explain itself.
 */
class SourceError extends Error {
  constructor(code, message, extra = {}) {
    super(message);
    this.name = 'SourceError';
    this.code = code;
    this.retryable = extra.retryable !== false;
    this.details = extra.details || {};
    this.status = extra.status || 500;
  }

  toJSON() {
    return {
      code: this.code,
      message: this.message,
      retryable: this.retryable,
      details: this.details,
      status: this.status,
    };
  }
}

function errorBody(err) {
  if (err instanceof SourceError) return err.toJSON();
  const status = err && err.status ? err.status : 500;
  return {
    code: (err && err.code) || (status === 404 ? 'not_found' : 'internal_error'),
    message: err && err.message ? err.message : String(err),
    retryable: status >= 500,
    details: (err && err.details) || {},
    status,
  };
}

module.exports = { SourceError, errorBody };
