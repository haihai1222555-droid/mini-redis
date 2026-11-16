/**
 * MiniRedis HTTP(REST) API 서버. (의존성 없이 node:http 만 사용)
 *
 *   GET    /health
 *   GET    /stats
 *   GET    /keys?prefix=&limit=
 *   PUT    /keys/:key            { value, ttl?(초), ttlMs?, tags?, nx?, xx? }
 *   GET    /keys/:key            → { key, value, ttlMs }
 *   DELETE /keys/:key
 *   GET    /keys/:key/ttl        → { key, ttlMs }   (ttlMs: null = 만료 없음)
 *   POST   /keys/:key/expire     { ttl | ttlMs }
 *   POST   /keys/:key/persist
 *   POST   /keys/:key/incr       { by? }
 *   POST   /invalidate           { key } | { prefix } | { tag } | { all: true }
 *
 * 키는 URL 경로에 들어가므로 반드시 encodeURIComponent 로 인코딩해서 보낸다.
 * 만료되었거나 무효화된 키는 404 로 응답한다 (값이 응답으로 새어 나가지 않는다).
 *
 * 동시성: 요청 본문 수신은 비동기지만, 본문이 모두 도착한 뒤 store 명령은 동기 실행되므로
 * 동시에 수천 요청이 들어와도 각 명령은 원자적으로 순서대로 처리된다.
 */
import http from 'node:http';
import crypto from 'node:crypto';
import { InvalidArgumentError, MiniRedisError, PayloadTooLargeError } from './errors.js';

const DEFAULT_MAX_BODY = 1024 * 1024; // 1MB

export function createServer({
  store,
  authToken,
  maxBodyBytes = DEFAULT_MAX_BODY,
  logger = () => {},
} = {}) {
  if (!store) throw new Error('store is required');
  const expectedAuth = authToken ? Buffer.from(`Bearer ${authToken}`) : null;

  function isAuthorized(req) {
    if (expectedAuth === null) return true;
    const given = Buffer.from(req.headers.authorization ?? '');
    // 길이가 다르면 timingSafeEqual 이 throw 하므로 먼저 비교
    return given.length === expectedAuth.length && crypto.timingSafeEqual(given, expectedAuth);
  }

  const server = http.createServer(async (req, res) => {
    const started = process.hrtime.bigint();
    try {
      // new URL() 은 경로의 '.' / '..' 를 정규화해 버리므로(키 ".." 가 "/" 로 바뀜) 원본 경로를 직접 분리한다.
      if (typeof req.url !== 'string' || !req.url.startsWith('/')) {
        throw new InvalidArgumentError('request target must be an absolute path');
      }
      const queryAt = req.url.indexOf('?');
      const rawPath = queryAt === -1 ? req.url : req.url.slice(0, queryAt);
      const url = { searchParams: new URLSearchParams(queryAt === -1 ? '' : req.url.slice(queryAt + 1)) };
      const parts = rawPath.split('/').slice(1); // ['keys', ':key', 'ttl']
      // GET /keys/ 는 목록으로 허용. (PUT /keys/ 는 "빈 키" → 400)
      if (req.method === 'GET' && parts.length === 2 && parts[0] === 'keys' && parts[1] === '') parts.pop();

      if (req.method === 'GET' && parts.length === 1 && parts[0] === 'health') {
        return send(res, 200, { status: 'ok' }); // 인증 없이 헬스체크 가능
      }
      if (!isAuthorized(req)) {
        res.setHeader('WWW-Authenticate', 'Bearer');
        return send(res, 401, error('UNAUTHORIZED', 'missing or invalid bearer token'));
      }

      const result = await route(req, url, parts);
      send(res, result.status, result.body);
    } catch (err) {
      if (err instanceof MiniRedisError) {
        if (err instanceof PayloadTooLargeError) {
          res.setHeader('Connection', 'close');
        }
        send(res, err.status, error(err.code, err.message));
      } else {
        logger('error', err);
        send(res, 500, error('INTERNAL', 'internal server error'));
      }
    } finally {
      logger('access', {
        method: req.method,
        url: req.url,
        status: res.statusCode,
        ms: Number(process.hrtime.bigint() - started) / 1e6,
      });
    }
  });

  async function route(req, url, parts) {
    const method = req.method;
    const [root, rawKey, action] = parts;

    if (root === 'stats' && parts.length === 1) {
      allow(method, ['GET']);
      return ok(store.stats());
    }

    if (root === 'invalidate' && parts.length === 1) {
      allow(method, ['POST']);
      const body = await readJson(req, maxBodyBytes);
      requireObject(body);
      const invalidated = store.invalidate(body);
      return ok({ invalidated });
    }

    if (root === 'keys') {
      if (parts.length === 1) {
        allow(method, ['GET']);
        const prefix = url.searchParams.get('prefix') ?? '';
        const limit = parseLimit(url.searchParams.get('limit'));
        const keys = store.keys({ prefix, limit });
        return ok({ keys, count: keys.length });
      }

      if (parts.length === 2 || parts.length === 3) {
        let key;
        try {
          key = decodeURIComponent(rawKey);
        } catch {
          throw new InvalidArgumentError('key is not valid percent-encoding');
        }
        return keyRoute(req, method, key, action, parts.length === 3);
      }
    }

    return { status: 404, body: error('NOT_FOUND', 'unknown route') };
  }

  async function keyRoute(req, method, key, action, hasAction) {
    if (!hasAction) {
      switch (method) {
        case 'GET': {
          const found = store.inspect(key);
          if (found === undefined) return notFound(key);
          return ok({ key, value: found.value, ttlMs: found.ttlMs });
        }
        case 'PUT': {
          const body = await readJson(req, maxBodyBytes);
          requireObject(body);
          if (!('value' in body)) throw new InvalidArgumentError('"value" is required');
          const stored = store.set(key, body.value, {
            ttlMs: parseTtl(body),
            tags: body.tags,
            nx: body.nx === true,
            xx: body.xx === true,
          });
          if (!stored) {
            return {
              status: 409,
              body: error('CONDITION_NOT_MET', body.nx ? 'key already exists' : 'key does not exist'),
            };
          }
          return ok({ key, stored: true });
        }
        case 'DELETE':
          return store.del(key) ? ok({ key, deleted: true }) : notFound(key);
        default:
          return methodNotAllowed(['GET', 'PUT', 'DELETE']);
      }
    }

    switch (action) {
      case 'ttl': {
        allow(method, ['GET']);
        const ttl = store.ttl(key);
        if (ttl === -2) return notFound(key);
        return ok({ key, ttlMs: ttl === -1 ? null : ttl });
      }
      case 'expire': {
        allow(method, ['POST']);
        const body = await readJson(req, maxBodyBytes);
        requireObject(body);
        const ttlMs = parseTtl(body);
        if (ttlMs === undefined) throw new InvalidArgumentError('"ttl" or "ttlMs" is required');
        return store.expire(key, ttlMs) ? ok({ key, updated: true }) : notFound(key);
      }
      case 'persist': {
        allow(method, ['POST']);
        const had = store.persist(key);
        if (!had && !store.exists(key)) return notFound(key);
        return ok({ key, persisted: had });
      }
      case 'incr': {
        allow(method, ['POST']);
        const body = await readJson(req, maxBodyBytes);
        const by = body === undefined ? 1 : (requireObject(body), body.by ?? 1);
        return ok({ key, value: store.incr(key, by) });
      }
      default:
        return { status: 404, body: error('NOT_FOUND', 'unknown route') };
    }
  }

  return server;
}

// ───────────────────────── helpers ─────────────────────────

function ok(body) {
  return { status: 200, body };
}

function notFound(key) {
  return { status: 404, body: error('NOT_FOUND', `key not found: ${key}`) };
}

function methodNotAllowed(allowed) {
  const err = new MiniRedisError(`method not allowed (allowed: ${allowed.join(', ')})`, {
    status: 405,
    code: 'METHOD_NOT_ALLOWED',
  });
  throw err;
}

function allow(method, allowed) {
  if (!allowed.includes(method)) methodNotAllowed(allowed);
}

function error(code, message) {
  return { error: { code, message } };
}

function requireObject(body) {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    throw new InvalidArgumentError('request body must be a JSON object');
  }
}

function parseLimit(raw) {
  if (raw === null) return 1000;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 1 || n > 10_000) {
    throw new InvalidArgumentError('limit must be an integer between 1 and 10000');
  }
  return n;
}

/** body 의 ttl(초) / ttlMs 를 ms 로 변환. 둘 다 없으면 undefined. */
function parseTtl(body) {
  if (body.ttl !== undefined && body.ttlMs !== undefined) {
    throw new InvalidArgumentError('use either "ttl" (seconds) or "ttlMs", not both');
  }
  if (body.ttl !== undefined) {
    if (typeof body.ttl !== 'number') throw new InvalidArgumentError('"ttl" must be a number');
    return body.ttl * 1000;
  }
  if (body.ttlMs !== undefined) {
    if (typeof body.ttlMs !== 'number') throw new InvalidArgumentError('"ttlMs" must be a number');
    return body.ttlMs;
  }
  return undefined;
}

/** 본문을 읽어 JSON 파싱. 본문이 비어 있으면 undefined. */
function readJson(req, limit) {
  return new Promise((resolve, reject) => {
    const declared = Number(req.headers['content-length']);
    if (Number.isFinite(declared) && declared > limit) {
      reject(new PayloadTooLargeError(`request body exceeds ${limit} bytes`));
      return;
    }
    const chunks = [];
    let received = 0;
    let settled = false;
    const fail = (err) => {
      if (settled) return;
      settled = true;
      reject(err);
    };
    req.on('data', (chunk) => {
      if (settled) return;
      received += chunk.length;
      if (received > limit) {
        fail(new PayloadTooLargeError(`request body exceeds ${limit} bytes`));
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (settled) return;
      settled = true;
      if (received === 0) return resolve(undefined);
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        reject(new InvalidArgumentError('request body is not valid JSON'));
      }
    });
    req.on('error', fail);
  });
}

function send(res, status, body) {
  if (res.headersSent) return;
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(payload),
    'Cache-Control': 'no-store',
  });
  res.end(payload);
}
