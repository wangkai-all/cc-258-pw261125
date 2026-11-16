'use strict';

class CoordinatorError extends Error {
  constructor(code, message, extra = {}) {
    super(message);
    this.name = 'CoordinatorError';
    this.code = code;
    this.status = extra.status || 409;
    this.details = extra.details || {};
  }

  toJSON() {
    return { code: this.code, message: this.message, details: this.details, status: this.status };
  }
}

module.exports = { CoordinatorError };
