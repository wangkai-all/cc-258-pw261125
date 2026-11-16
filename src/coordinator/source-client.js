'use strict';

class UpstreamError extends Error {
  constructor({ source, code, message, status, details, retryable, phase }) {
    super(message);
    this.name = 'UpstreamError';
    this.source = source;
    this.code = code;
    this.status = status || 503;
    this.details = details || {};
    this.retryable = retryable !== false;
    this.phase = phase || null;
  }
}

class SourceClient {
  constructor({ baseUrl, name, timeoutMs = 5000 }) {
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.name = name;
    this.timeoutMs = timeoutMs;
  }

  async get(path, params = {}) {
    const url = new URL(this.baseUrl + path);
    for (const [key, value] of Object.entries(params)) {
      if (value === undefined || value === null || value === '') continue;
      url.searchParams.set(key, String(value));
    }
    return this.request(url, 'GET');
  }

  async post(path, body = {}) {
    const url = new URL(this.baseUrl + path);
    return this.request(url, 'POST', body);
  }

  async request(url, method, body) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    let response;
    try {
      response = await fetch(url, {
        method,
        headers: body === undefined ? undefined : { 'content-type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal,
      });
    } catch (err) {
      const aborted = err && err.name === 'AbortError';
      throw new UpstreamError({
        source: this.name,
        code: aborted ? 'source_timeout' : 'source_unreachable',
        message: aborted
          ? `${this.name} did not answer within ${this.timeoutMs}ms`
          : `${this.name} is unreachable: ${err && err.message ? err.message : String(err)}`,
        status: 503,
        retryable: true,
      });
    } finally {
      clearTimeout(timer);
    }
    let payload = null;
    const text = await response.text();
    try {
      payload = text ? JSON.parse(text) : null;
    } catch {
      throw new UpstreamError({
        source: this.name,
        code: 'source_protocol_error',
        message: `${this.name} returned a non-JSON response (HTTP ${response.status})`,
        status: 502,
        retryable: true,
        details: { status: response.status },
      });
    }
    if (!response.ok) {
      const error = (payload && payload.error) || {};
      throw new UpstreamError({
        source: this.name,
        code: error.code || `source_http_${response.status}`,
        message: error.message || `${this.name} returned HTTP ${response.status}`,
        status: response.status,
        details: error.details || {},
        retryable: error.retryable !== undefined ? error.retryable : response.status >= 500,
      });
    }
    return payload;
  }
}

module.exports = { SourceClient, UpstreamError };
