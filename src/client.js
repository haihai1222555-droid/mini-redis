/**
 * MiniRedis HTTP 클라이언트 (Node 18+ 내장 fetch 사용).
 *
 *   const cache = new MiniRedisClient('http://localhost:6380', { token: '...' });
 *   await cache.set('user:1', { name: 'kim' }, { ttl: 60, tags: ['user'] });
 *   await cache.get('user:1');          // { name: 'kim' }  (없거나 만료되면 undefined)
 *   await cache.getOrSet('k', loader, { ttl: 30 });   // cache-aside 패턴
 */

export class MiniRedisHttpError extends Error {
  constructor(status, code, message) {
    super(message);
    this.name = 'MiniRedisHttpError';
    this.status = status;
    this.code = code;
  }
}

export class MiniRedisClient {
  #baseUrl;
  #token;
  #timeoutMs;
  #inflight = new Map(); // single-flight: key -> Promise

  /**
   * @param {string} baseUrl
   * @param {{ token?: string, timeoutMs?: number }} [options]
   */
  constructor(baseUrl, { token, timeoutMs = 2000 } = {}) {
    this.#baseUrl = baseUrl.replace(/\/+$/, '');
    this.#token = token;
    this.#timeoutMs = timeoutMs;
  }

  async #request(method, path, body) {
    const headers = {};
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    if (this.#token) headers.Authorization = `Bearer ${this.#token}`;
    const res = await fetch(this.#baseUrl + path, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(this.#timeoutMs),
    });
    const text = await res.text();
    const json = text ? JSON.parse(text) : undefined;
    return { status: res.status, json };
  }

  #throwIfError({ status, json }) {
    if (status >= 400) {
      throw new MiniRedisHttpError(
        status,
        json?.error?.code ?? 'UNKNOWN',
        json?.error?.message ?? `HTTP ${status}`,
      );
    }
  }

  static #keyPath(key) {
    // URL 표준(fetch 포함)은 경로의 '.' / '..' 세그먼트를 (퍼센트 인코딩해도) 정규화해 버리므로
    // 이 두 키는 URL 경로로 안전하게 전달할 수 없다. 엉뚱한 경로로 가기 전에 명시적으로 거부한다.
    if (key === '.' || key === '..') {
      throw new MiniRedisHttpError(400, 'INVALID_ARGUMENT', 'keys "." and ".." are not supported over HTTP');
    }
    return `/keys/${encodeURIComponent(key)}`;
  }

  /** @returns {Promise<*>} 값. 없거나 만료됐으면 undefined (저장된 null 과 구분) */
  async get(key) {
    const res = await this.#request('GET', MiniRedisClient.#keyPath(key));
    if (res.status === 404) return undefined;
    this.#throwIfError(res);
    return res.json.value;
  }

  /**
   * @param {{ ttl?: number, ttlMs?: number, tags?: string[], nx?: boolean, xx?: boolean }} [opts]
   * @returns {Promise<boolean>} nx/xx 조건 때문에 저장되지 않았으면 false
   */
  async set(key, value, opts = {}) {
    const res = await this.#request('PUT', MiniRedisClient.#keyPath(key), { value, ...opts });
    if (res.status === 409 && res.json?.error?.code === 'CONDITION_NOT_MET') return false;
    this.#throwIfError(res);
    return true;
  }

  async del(key) {
    const res = await this.#request('DELETE', MiniRedisClient.#keyPath(key));
    if (res.status === 404) return false;
    this.#throwIfError(res);
    return true;
  }

  async exists(key) {
    return (await this.ttl(key)) !== undefined;
  }

  /** @returns {Promise<number|null|undefined>} 남은 ms, 만료 없음이면 null, 키가 없으면 undefined */
  async ttl(key) {
    const res = await this.#request('GET', `${MiniRedisClient.#keyPath(key)}/ttl`);
    if (res.status === 404) return undefined;
    this.#throwIfError(res);
    return res.json.ttlMs;
  }

  async expire(key, ttlSeconds) {
    const res = await this.#request('POST', `${MiniRedisClient.#keyPath(key)}/expire`, {
      ttl: ttlSeconds,
    });
    if (res.status === 404) return false;
    this.#throwIfError(res);
    return true;
  }

  async persist(key) {
    const res = await this.#request('POST', `${MiniRedisClient.#keyPath(key)}/persist`);
    if (res.status === 404) return false;
    this.#throwIfError(res);
    return res.json.persisted;
  }

  async incr(key, by = 1) {
    const res = await this.#request('POST', `${MiniRedisClient.#keyPath(key)}/incr`, { by });
    this.#throwIfError(res);
    return res.json.value;
  }

  async keys({ prefix = '', limit = 1000 } = {}) {
    const q = new URLSearchParams({ prefix, limit: String(limit) });
    const res = await this.#request('GET', `/keys?${q}`);
    this.#throwIfError(res);
    return res.json.keys;
  }

  /** @param {{ key?: string, prefix?: string, tag?: string, all?: boolean }} criteria */
  async invalidate(criteria) {
    const res = await this.#request('POST', '/invalidate', criteria);
    this.#throwIfError(res);
    return res.json.invalidated;
  }

  async stats() {
    const res = await this.#request('GET', '/stats');
    this.#throwIfError(res);
    return res.json;
  }

  async health() {
    try {
      const res = await this.#request('GET', '/health');
      return res.status === 200;
    } catch {
      return false;
    }
  }

  /**
   * Cache-aside 헬퍼.
   *  1. 캐시 조회 → 있으면 반환
   *  2. 없으면 loader() 로 원본 조회 후 캐시에 저장
   *
   * 장애/동시성 edge case 처리:
   *  - single-flight: 같은 키로 동시에 miss 가 나도 loader 는 한 번만 실행 (cache stampede 방지)
   *  - fail-open: 캐시 서버가 죽었거나 느려도 loader 결과를 그대로 반환 (캐시는 "있으면 좋은 것")
   *  - loader 가 throw 하면 아무것도 캐시하지 않고 그대로 전파 (에러를 캐시하지 않음)
   *  - loader 가 null 을 반환하면 null 도 캐시 (negative caching → 없는 id 반복 조회로 DB 가 뚫리는 것 방지)
   *    loader 가 undefined 를 반환하면 캐시하지 않는다.
   *
   * @param {string} key
   * @param {() => Promise<*>} loader
   * @param {{ ttl?: number | ((value: *) => number), tags?: string[] }} [opts]  ttl: 초 단위. 함수면 값에 따라 결정.
   * @returns {Promise<{ value: *, hit: boolean }>}
   */
  async getOrSet(key, loader, { ttl, tags } = {}) {
    try {
      const cached = await this.get(key);
      if (cached !== undefined) return { value: cached, hit: true };
    } catch {
      // 캐시 장애 → 캐시 없이 진행 (fail-open)
    }

    const pending = this.#inflight.get(key);
    if (pending !== undefined) {
      return { value: await pending, hit: false };
    }

    const promise = (async () => {
      const value = await loader();
      if (value !== undefined) {
        const seconds = typeof ttl === 'function' ? ttl(value) : ttl;
        try {
          await this.set(key, value, { ttl: seconds, tags });
        } catch {
          // 저장 실패는 응답에 영향 없음
        }
      }
      return value;
    })();
    this.#inflight.set(key, promise);
    try {
      return { value: await promise, hit: false };
    } finally {
      this.#inflight.delete(key);
    }
  }
}
