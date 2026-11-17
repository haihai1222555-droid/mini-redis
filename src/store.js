/**
 * MiniRedis 저장소 코어.
 *
 * 동시성 모델
 * -----------
 * Redis 와 마찬가지로 "모든 명령은 단일 스레드에서 순서대로, 원자적으로" 실행된다.
 * 이 클래스의 모든 public 메서드는 동기(sync) 함수이고 await 지점이 없으므로,
 * Node.js 이벤트 루프 상에서 한 명령이 끝나기 전에 다른 명령이 끼어들 수 없다.
 * 즉 락(mutex) 없이도 read-modify-write(INCR, SET NX 등)가 안전하다.
 * 네트워크 I/O 는 서버 레이어(server.js)가 비동기로 받고, 파싱이 끝난 명령만 이 코어에 동기 호출한다.
 *
 * 만료(TTL) 처리 = 두 가지 전략을 함께 사용 (Redis 와 동일)
 * ---------------------------------------------------
 * 1. Lazy expiration  : 키에 접근할 때 만료 시각이 지났으면 그 자리에서 삭제하고 "없음"으로 응답.
 *                       → 만료된 값이 절대 응답으로 나가지 않는다.
 * 2. Active expiration: 주기적으로 해시 테이블 일부를 scan 해서 만료 키를 지운다.
 *                       → 아무도 다시 읽지 않는 키가 메모리를 영원히 차지하는 것을 방지.
 *
 * 무효화(invalidate)
 * -----------------
 * DEL 은 "키 하나를 지우는" 것이고, invalidate 는 "원본 데이터가 바뀌어 더는 믿을 수 없게 된 캐시를
 * 한 번에 폐기하는" 것이다. key / prefix / tag / 전체 단위를 지원한다.
 * 태그는 set 할 때 붙이고, 태그 → 키 집합의 보조 인덱스로 O(해당 키 수) 에 무효화한다.
 */
import { HashTable } from './hashtable.js';
import { Aof } from './aof.js';
import { InvalidArgumentError, WrongTypeError, StoreFullError } from './errors.js';

export const MAX_KEY_LENGTH = 512;
export const MAX_TAGS = 32;
export const MAX_TTL_MS = 100 * 365 * 24 * 60 * 60 * 1000; // 100년

const TTL_MISSING = -2;
const TTL_NO_EXPIRY = -1;

function validateKey(key) {
  if (typeof key !== 'string' || key.length === 0) {
    throw new InvalidArgumentError('key must be a non-empty string');
  }
  if (key.length > MAX_KEY_LENGTH) {
    throw new InvalidArgumentError(`key is too long (max ${MAX_KEY_LENGTH} chars)`);
  }
}

/** JSON 으로 직렬화 가능한 값만 허용하고, 호출자의 객체와 분리된 사본을 저장한다. */
function normalizeValue(value) {
  switch (typeof value) {
    case 'string':
    case 'boolean':
      return value;
    case 'number':
      if (!Number.isFinite(value)) throw new InvalidArgumentError('value must be a finite number');
      return value;
    case 'object': {
      if (value === null) return null;
      let json;
      try {
        json = JSON.stringify(value);
      } catch (err) {
        throw new InvalidArgumentError(`value is not JSON-serializable: ${err.message}`);
      }
      return JSON.parse(json);
    }
    default: // undefined, function, symbol, bigint
      throw new InvalidArgumentError(`value of type ${typeof value} is not supported`);
  }
}

function cloneOut(value) {
  return typeof value === 'object' && value !== null ? structuredClone(value) : value;
}

function validateTtlMs(ttlMs) {
  if (typeof ttlMs !== 'number' || !Number.isFinite(ttlMs) || ttlMs <= 0) {
    throw new InvalidArgumentError('ttl must be a positive number');
  }
  if (ttlMs > MAX_TTL_MS) throw new InvalidArgumentError('ttl is too large');
  return Math.ceil(ttlMs);
}

function normalizeTags(tags) {
  if (tags === undefined || tags === null) return null;
  if (!Array.isArray(tags)) throw new InvalidArgumentError('tags must be an array of strings');
  if (tags.length > MAX_TAGS) throw new InvalidArgumentError(`too many tags (max ${MAX_TAGS})`);
  for (const t of tags) {
    if (typeof t !== 'string' || t.length === 0 || t.length > MAX_KEY_LENGTH) {
      throw new InvalidArgumentError('each tag must be a non-empty string');
    }
  }
  const unique = [...new Set(tags)];
  return unique.length > 0 ? unique : null;
}

/** AOF 복구용 검증: 예외를 던지지 않고 boolean 만 반환한다. */
function isValidRecordKey(k) {
  return typeof k === 'string' && k.length > 0 && k.length <= MAX_KEY_LENGTH;
}

function isValidRecordTags(t) {
  return (
    Array.isArray(t) &&
    t.length <= MAX_TAGS &&
    t.every((tag) => typeof tag === 'string' && tag.length > 0 && tag.length <= MAX_KEY_LENGTH)
  );
}

export class MiniRedis {
  #table = new HashTable();
  #tagIndex = new Map(); // tag -> Set<key>
  #now;
  #maxKeys;
  #lru = false;
  #lruHead = null; // 가장 최근에 쓴 엔트리
  #lruTail = null; // 가장 오래 안 쓴 엔트리 = 축출 대상
  #aof = null;
  #aofLines = 0;
  #aofRewriteMinLines;
  #sweepTimer = null;
  #sweepBatch;
  #sweepCursor = 0;
  #lastFullSweep = 0;
  #startedAt;
  #closed = false;
  #stats = {
    hits: 0,
    misses: 0,
    sets: 0,
    deletes: 0,
    expiredLazy: 0,
    expiredActive: 0,
    invalidated: 0,
    evicted: 0,
  };
  loadInfo = null;

  /**
   * @param {object} [options]
   * @param {() => number} [options.now]            현재 시각(ms). 테스트에서 가짜 시계를 주입.
   * @param {string} [options.aofPath]              지정하면 AOF 영속성 활성화.
   * @param {'always'|'everysec'|'no'} [options.fsync]
   * @param {number} [options.sweepIntervalMs]      active expiration 주기. 0 이면 끔.
   * @param {number} [options.sweepBatch]           한 번의 sweep 에서 검사할 엔트리 수.
   * @param {number} [options.maxKeys]              저장 가능한 최대 키 수.
   * @param {'noeviction'|'lru'} [options.evictionPolicy]
   *   maxKeys 가 가득 찼을 때: noeviction(기본) = StoreFullError / lru = 가장 오래 안 쓴 키를 축출.
   * @param {number} [options.aofRewriteMinLines]   이 줄 수를 넘고 살아있는 키의 2배를 넘으면 자동 compaction.
   */
  constructor({
    now = Date.now,
    aofPath,
    fsync = 'everysec',
    sweepIntervalMs = 1000,
    sweepBatch = 200,
    maxKeys = Infinity,
    evictionPolicy = 'noeviction',
    aofRewriteMinLines = 10_000,
  } = {}) {
    if (!['noeviction', 'lru'].includes(evictionPolicy)) {
      throw new InvalidArgumentError(`invalid evictionPolicy: ${evictionPolicy} (expected noeviction|lru)`);
    }
    if (maxKeys !== Infinity && !(Number.isInteger(maxKeys) && maxKeys >= 1)) {
      throw new InvalidArgumentError('maxKeys must be a positive integer');
    }
    if (evictionPolicy === 'lru' && maxKeys === Infinity) {
      throw new InvalidArgumentError("evictionPolicy 'lru' requires maxKeys");
    }
    this.#now = now;
    this.#maxKeys = maxKeys;
    this.#lru = evictionPolicy === 'lru';
    this.#sweepBatch = sweepBatch;
    this.#aofRewriteMinLines = aofRewriteMinLines;
    this.#startedAt = now();

    if (aofPath) {
      this.#aof = new Aof(aofPath, { fsync });
      this.loadInfo = this.#aof.load((rec) => this.#replay(rec));
      this.#aofLines = this.loadInfo.records;
      this.#evictToFit(this.#maxKeys); // maxKeys 를 줄여서 재시작한 경우
      this.#maybeCompact();
    }

    if (sweepIntervalMs > 0) {
      this.#sweepTimer = setInterval(() => this.sweep(), sweepIntervalMs);
      this.#sweepTimer.unref(); // 이 타이머 때문에 프로세스가 종료되지 못하는 일이 없도록
    }
  }

  // ───────────────────────── 내부 헬퍼 ─────────────────────────

  #isExpired(entry, now) {
    return entry.expiresAt !== null && entry.expiresAt <= now;
  }

  /** 살아있는 엔트리만 반환. 만료됐다면 지금 삭제(lazy expiration). */
  #live(key) {
    const entry = this.#table.get(key);
    if (entry === undefined) return undefined;
    if (this.#isExpired(entry, this.#now())) {
      this.#remove(key, entry);
      this.#stats.expiredLazy++;
      return undefined;
    }
    this.#lruTouch(entry); // 접근 = 최근 사용
    return entry;
  }

  #remove(key, entry) {
    this.#table.delete(key);
    this.#unindexTags(key, entry.tags);
    this.#lruUnlink(entry);
  }

  // ── LRU: 해시 테이블(키 → 엔트리)과 이중 연결 리스트(사용 순서)를 함께 쓴다.
  //    찾기는 해시 테이블로 O(1), 순서 갱신/제거는 리스트 포인터 조작으로 O(1).
  //    head = 최근, tail = 가장 오래됨. (noeviction 이면 아래 함수들은 아무 일도 하지 않는다)

  #lruLink(entry) {
    if (!this.#lru) return;
    entry.prev = null;
    entry.next = this.#lruHead;
    if (this.#lruHead !== null) this.#lruHead.prev = entry;
    this.#lruHead = entry;
    if (this.#lruTail === null) this.#lruTail = entry;
  }

  #lruUnlink(entry) {
    if (!this.#lru) return;
    if (entry.prev !== null) entry.prev.next = entry.next;
    else this.#lruHead = entry.next;
    if (entry.next !== null) entry.next.prev = entry.prev;
    else this.#lruTail = entry.prev;
    entry.prev = null;
    entry.next = null;
  }

  #lruTouch(entry) {
    if (!this.#lru || this.#lruHead === entry) return;
    this.#lruUnlink(entry);
    this.#lruLink(entry);
  }

  #lruReset() {
    this.#lruHead = null;
    this.#lruTail = null;
  }

  /** 가장 오래 안 쓴 엔트리 하나를 축출. AOF 에는 삭제로 기록해 재시작 후 되살아나지 않게 한다. */
  #evictOne() {
    const victim = this.#lruTail;
    if (victim === null) return false;
    this.#remove(victim.key, victim);
    this.#log({ o: 'd', k: victim.key });
    this.#stats.evicted++;
    return true;
  }

  #evictToFit(limit) {
    if (!this.#lru) return;
    while (this.#table.size > limit && this.#evictOne());
  }

  #indexTags(key, tags) {
    if (tags === null) return;
    for (const tag of tags) {
      let keys = this.#tagIndex.get(tag);
      if (keys === undefined) {
        keys = new Set();
        this.#tagIndex.set(tag, keys);
      }
      keys.add(key);
    }
  }

  #unindexTags(key, tags) {
    if (tags === null) return;
    for (const tag of tags) {
      const keys = this.#tagIndex.get(tag);
      if (keys === undefined) continue;
      keys.delete(key);
      if (keys.size === 0) this.#tagIndex.delete(tag);
    }
  }

  /** 엔트리를 저장(덮어쓰기 포함)하고 AOF 에 기록. */
  #put(key, entry, { log = true } = {}) {
    const old = this.#table.get(key);
    if (old !== undefined) {
      this.#unindexTags(key, old.tags);
      this.#lruUnlink(old);
    }
    if (this.#lru) entry.key = key; // 축출 시 테이블에서 지우려면 키가 필요
    this.#table.set(key, entry);
    this.#indexTags(key, entry.tags);
    this.#lruLink(entry);
    if (log) this.#log({ o: 'p', k: key, v: entry.value, e: entry.expiresAt, t: entry.tags });
  }

  #log(record) {
    if (this.#aof === null) return;
    this.#aof.append(record);
    this.#aofLines++;
    this.#maybeCompact();
  }

  #maybeCompact() {
    if (
      this.#aof !== null &&
      this.#aofLines > this.#aofRewriteMinLines &&
      this.#aofLines > 2 * this.#table.size
    ) {
      this.compact();
    }
  }

  /**
   * AOF 레코드 하나를 메모리에 적용한다.
   * JSON 으로는 올바르지만 필드가 잘못된 레코드(수동 편집, 디스크 손상 등)는 적용하지 않고 false 를 반환한다.
   * → Aof.load 가 skipped 로 세고, 서버는 죽지 않으며 내부 상태(태그 인덱스 등)도 오염되지 않는다.
   * @returns {boolean} 적용(또는 알 수 없는 레코드라 무시)했으면 true, 잘못된 레코드라 건너뛰었으면 false
   */
  #replay(rec) {
    switch (rec.o) {
      case 'p': {
        if (!isValidRecordKey(rec.k) || rec.v === undefined) return false;
        const e = rec.e ?? null;
        if (e !== null && !Number.isFinite(e)) return false;
        const tags = rec.t ?? null;
        if (tags !== null && !isValidRecordTags(tags)) return false;

        if (e !== null && e <= this.#now()) {
          // 서버가 꺼진 사이 만료된 키 → 복원하지 않는다. (이전 값이 있었다면 그것도 지운다)
          const old = this.#table.get(rec.k);
          if (old !== undefined) this.#remove(rec.k, old);
          return true;
        }
        this.#put(
          rec.k,
          { value: rec.v, expiresAt: e, tags: tags === null || tags.length === 0 ? null : [...new Set(tags)] },
          { log: false },
        );
        return true;
      }
      case 'd': {
        if (!isValidRecordKey(rec.k)) return false;
        const old = this.#table.get(rec.k);
        if (old !== undefined) this.#remove(rec.k, old);
        return true;
      }
      case 'f':
        this.#table.clear();
        this.#tagIndex.clear();
        this.#lruReset();
        return true;
      default:
        // 알 수 없는 레코드는 무시 (상위 버전 호환)
        return true;
    }
  }

  #ensureCapacity() {
    if (this.#table.size < this.#maxKeys) return;
    // 가득 찼다면 만료된 키부터 정리해 본다 (최대 초당 1회, 연속된 O(n) 스캔 방지).
    const now = this.#now();
    if (now - this.#lastFullSweep >= 1000) {
      this.#lastFullSweep = now;
      this.#sweepAll();
    }
    // 만료 키를 정리해도 여전히 가득 차 있다면: lru 는 가장 오래 안 쓴 키를 버리고, noeviction 은 거부.
    this.#evictToFit(this.#maxKeys - 1);
    if (this.#table.size >= this.#maxKeys) {
      throw new StoreFullError(`store is full (maxKeys=${this.#maxKeys})`);
    }
  }

  #sweepAll() {
    const now = this.#now();
    for (const [key, entry] of [...this.#table]) {
      if (this.#isExpired(entry, now)) {
        this.#remove(key, entry);
        this.#stats.expiredActive++;
      }
    }
  }

  // ───────────────────────── 기본 명령 ─────────────────────────

  /**
   * @param {string} key
   * @param {*} value JSON 직렬화 가능한 값
   * @param {{ ttlMs?: number, tags?: string[], nx?: boolean, xx?: boolean }} [opts]
   *   nx: 키가 없을 때만 저장 / xx: 키가 있을 때만 저장
   * @returns {boolean} 저장했으면 true
   */
  set(key, value, opts = {}) {
    this.#assertOpen();
    const { ttlMs, tags, nx = false, xx = false } = opts;
    validateKey(key);
    if (nx && xx) throw new InvalidArgumentError('nx and xx cannot be used together');
    const normalized = normalizeValue(value);
    const expiresAt = ttlMs === undefined ? null : this.#now() + validateTtlMs(ttlMs);
    const normalizedTags = normalizeTags(tags);

    const existing = this.#live(key);
    if (nx && existing !== undefined) return false;
    if (xx && existing === undefined) return false;
    if (existing === undefined) this.#ensureCapacity();

    this.#put(key, { value: normalized, expiresAt, tags: normalizedTags });
    this.#stats.sets++;
    return true;
  }

  /** @returns {{ value: *, ttlMs: number|null } | undefined} 없거나 만료됐으면 undefined */
  inspect(key) {
    this.#assertOpen();
    validateKey(key);
    const entry = this.#live(key);
    if (entry === undefined) {
      this.#stats.misses++;
      return undefined;
    }
    this.#stats.hits++;
    return {
      value: cloneOut(entry.value),
      ttlMs: entry.expiresAt === null ? null : Math.max(0, entry.expiresAt - this.#now()),
    };
  }

  /** @returns {*} 값. 없거나 만료됐으면 undefined (저장된 null 과 구분됨) */
  get(key) {
    return this.inspect(key)?.value;
  }

  exists(key) {
    this.#assertOpen();
    validateKey(key);
    return this.#live(key) !== undefined;
  }

  /** @returns {boolean} 지웠으면 true */
  del(key) {
    this.#assertOpen();
    validateKey(key);
    const entry = this.#live(key);
    if (entry === undefined) return false;
    this.#remove(key, entry);
    this.#log({ o: 'd', k: key });
    this.#stats.deletes++;
    return true;
  }

  /** @returns {number} 남은 ms. 키가 없으면 -2, 만료 설정이 없으면 -1 */
  ttl(key) {
    this.#assertOpen();
    validateKey(key);
    const entry = this.#live(key);
    if (entry === undefined) return TTL_MISSING;
    if (entry.expiresAt === null) return TTL_NO_EXPIRY;
    return Math.max(0, entry.expiresAt - this.#now());
  }

  /** 기존 키에 TTL 설정. @returns {boolean} 키가 있어서 설정됐으면 true */
  expire(key, ttlMs) {
    this.#assertOpen();
    validateKey(key);
    const ms = validateTtlMs(ttlMs);
    const entry = this.#live(key);
    if (entry === undefined) return false;
    entry.expiresAt = this.#now() + ms;
    this.#log({ o: 'p', k: key, v: entry.value, e: entry.expiresAt, t: entry.tags });
    return true;
  }

  /** TTL 제거. @returns {boolean} 만료 설정이 있었으면 true */
  persist(key) {
    this.#assertOpen();
    validateKey(key);
    const entry = this.#live(key);
    if (entry === undefined || entry.expiresAt === null) return false;
    entry.expiresAt = null;
    this.#log({ o: 'p', k: key, v: entry.value, e: null, t: entry.tags });
    return true;
  }

  /** 원자적 증가. 키가 없으면 0 에서 시작. TTL 은 유지. */
  incr(key, by = 1) {
    this.#assertOpen();
    validateKey(key);
    if (!Number.isSafeInteger(by)) throw new InvalidArgumentError('increment must be a safe integer');
    const entry = this.#live(key);
    if (entry === undefined) {
      this.#ensureCapacity();
      this.#put(key, { value: by, expiresAt: null, tags: null });
      this.#stats.sets++;
      return by;
    }
    if (!Number.isSafeInteger(entry.value)) {
      throw new WrongTypeError('value is not an integer');
    }
    const next = entry.value + by;
    if (!Number.isSafeInteger(next)) throw new WrongTypeError('increment would overflow');
    entry.value = next;
    this.#log({ o: 'p', k: key, v: next, e: entry.expiresAt, t: entry.tags });
    return next;
  }

  /** 살아있는 키 목록 (prefix 필터). 전체 순회 O(n) 이므로 운영 용도보다는 디버깅/관리용. */
  keys({ prefix = '', limit = 1000 } = {}) {
    this.#assertOpen();
    const now = this.#now();
    const result = [];
    for (const [key, entry] of [...this.#table]) {
      if (this.#isExpired(entry, now)) {
        this.#remove(key, entry);
        this.#stats.expiredLazy++;
        continue;
      }
      if (key.startsWith(prefix)) {
        result.push(key);
        if (result.length >= limit) break;
      }
    }
    return result;
  }

  // ───────────────────────── 무효화 ─────────────────────────

  /**
   * 캐시 무효화. 정확히 하나의 기준을 지정한다.
   *   { key }    키 하나
   *   { prefix } 접두사가 같은 모든 키  (O(n) 전체 순회)
   *   { tag }    태그가 붙은 모든 키    (O(해당 키 수))
   *   { all: true } 전체
   * @returns {number} 무효화된(살아있던) 키 개수
   */
  invalidate(criteria) {
    this.#assertOpen();
    if (criteria === null || typeof criteria !== 'object') {
      throw new InvalidArgumentError('invalidate requires one of: key, prefix, tag, all');
    }
    const specified = ['key', 'prefix', 'tag', 'all'].filter((k) => criteria[k] !== undefined);
    if (specified.length !== 1) {
      throw new InvalidArgumentError('invalidate requires exactly one of: key, prefix, tag, all');
    }

    let count = 0;
    const drop = (key) => {
      const entry = this.#live(key);
      if (entry === undefined) return;
      this.#remove(key, entry);
      this.#log({ o: 'd', k: key });
      count++;
    };

    if (criteria.key !== undefined) {
      validateKey(criteria.key);
      drop(criteria.key);
    } else if (criteria.prefix !== undefined) {
      if (typeof criteria.prefix !== 'string' || criteria.prefix.length === 0) {
        throw new InvalidArgumentError('prefix must be a non-empty string');
      }
      for (const key of [...this.#table.keys()]) {
        if (key.startsWith(criteria.prefix)) drop(key);
      }
    } else if (criteria.tag !== undefined) {
      if (typeof criteria.tag !== 'string' || criteria.tag.length === 0) {
        throw new InvalidArgumentError('tag must be a non-empty string');
      }
      const keys = this.#tagIndex.get(criteria.tag);
      if (keys !== undefined) for (const key of [...keys]) drop(key);
    } else {
      if (criteria.all !== true) throw new InvalidArgumentError('all must be true');
      count = this.#countLive();
      this.#table.clear();
      this.#tagIndex.clear();
      this.#lruReset();
      this.#log({ o: 'f' });
    }
    this.#stats.invalidated += count;
    return count;
  }

  #countLive() {
    const now = this.#now();
    let n = 0;
    for (const [, entry] of this.#table) if (!this.#isExpired(entry, now)) n++;
    return n;
  }

  // ───────────────────────── 만료 청소 / 운영 ─────────────────────────

  /**
   * Active expiration: 테이블을 조금씩 훑어 만료된 키를 지운다.
   * 검사한 키의 25% 넘게 만료되어 있었다면 만료 키가 많다는 뜻이므로 바로 한 번 더 돈다 (최대 5회).
   * @returns {number} 이번 호출에서 지운 키 수
   */
  sweep() {
    if (this.#closed) return 0;
    let removedTotal = 0;
    for (let round = 0; round < 5; round++) {
      const { cursor, entries } = this.#table.scan(this.#sweepCursor, this.#sweepBatch);
      this.#sweepCursor = cursor;
      const now = this.#now();
      let removed = 0;
      for (const [key, entry] of entries) {
        if (this.#isExpired(entry, now)) {
          this.#remove(key, entry);
          this.#stats.expiredActive++;
          removed++;
        }
      }
      removedTotal += removed;
      if (entries.length === 0 || removed / entries.length <= 0.25) break;
    }
    return removedTotal;
  }

  /** AOF 를 살아있는 키만으로 새로 쓴다. */
  compact() {
    if (this.#aof === null) return;
    const now = this.#now();
    const self = this;
    function* records() {
      for (const [key, entry] of self.#table) {
        if (entry.expiresAt !== null && entry.expiresAt <= now) continue;
        yield { o: 'p', k: key, v: entry.value, e: entry.expiresAt, t: entry.tags };
      }
    }
    this.#aof.rewrite(records());
    this.#aofLines = this.#countLive();
  }

  /** 버퍼에 남은 AOF 데이터를 디스크에 강제 반영. */
  flushToDisk() {
    this.#aof?.sync();
  }

  get size() {
    return this.#table.size;
  }

  stats() {
    const total = this.#stats.hits + this.#stats.misses;
    return {
      ...this.#stats,
      hitRatio: total === 0 ? null : this.#stats.hits / total,
      keys: this.#table.size,
      tags: this.#tagIndex.size,
      table: this.#table.stats(),
      aof: this.#aof
        ? { enabled: true, path: this.#aof.path, lines: this.#aofLines, load: this.loadInfo }
        : { enabled: false },
      uptimeMs: this.#now() - this.#startedAt,
    };
  }

  #assertOpen() {
    if (this.#closed) throw new Error('store is closed');
  }

  close() {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#sweepTimer !== null) clearInterval(this.#sweepTimer);
    this.#aof?.close();
  }
}
