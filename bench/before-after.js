/**
 * 처음 버전의 방식 vs 기능을 쓴 방식
 *   npm run bench:before-after
 *   (환경변수 KEYS 로 SCAN 비교의 키 개수 조절, 기본 1,000,000 / STATIONARY=1 이면 인기 키가 안 바뀌는 상황)
 *
 * "처음 버전의 방식" = 새 기능을 쓰지 않았을 때의 동작이다. 같은 코드에서 옵션만 바꿔 비교한다.
 *   - LRU  : evictionPolicy 'noeviction'(가득 차면 새 키 거부) vs 'lru'
 *   - SCAN : keys()(전체 테이블을 복사하며 훑음) vs scan() 한 페이지
 * 배치와 트랜잭션은 별도 스크립트로 잰다:  npm run bench:batch / npm run demo:tx
 */
import { performance } from 'node:perf_hooks';
import { MiniRedis } from '../src/index.js';
import { StoreFullError } from '../src/errors.js';

const KEYS = Number(process.env.KEYS ?? 1_000_000);
const STATIONARY = process.env.STATIONARY === '1'; // 1 이면 인기 키가 바뀌지 않는 상황으로 LRU 를 비교

function rng(seed) {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ───────────── 1. LRU: 인기 키가 시간에 따라 바뀌는 상황의 적중률 ─────────────
// 전체 키 10,000개 중 캐시에는 1,000개만 들어간다. 5단계로 나눠서, 단계마다 인기 키 500개가 바뀐다.
// 요청의 90%는 지금 인기 키, 10%는 아무 키나. 캐시에 없으면(miss) 원본에서 읽어 와서 캐시에 넣으려고 시도한다.
const UNIVERSE = 10_000;
const CAP = 1_000;
const HOT = 500;
const PHASES = 5;
const REQ = 20_000;

function hitRatios(policy) {
  const store = new MiniRedis({ maxKeys: CAP, evictionPolicy: policy, sweepIntervalMs: 0 });
  const rand = rng(7);
  const perPhase = [];
  let rejected = 0;
  for (let p = 0; p < PHASES; p++) {
    let hits = 0;
    for (let i = 0; i < REQ; i++) {
      const id = rand() < 0.9 ? (STATIONARY ? 0 : p * HOT) + Math.floor(rand() * HOT) : Math.floor(rand() * UNIVERSE);
      const key = `k${id}`;
      if (store.get(key) !== undefined) {
        hits++;
      } else {
        try {
          store.set(key, id);
        } catch (err) {
          if (!(err instanceof StoreFullError)) throw err;
          rejected++; // 가득 차서 캐시에 못 넣음
        }
      }
    }
    perPhase.push(hits / REQ);
  }
  store.close();
  return { perPhase, overall: perPhase.reduce((a, b) => a + b, 0) / PHASES, rejected };
}

const pct = (x) => `${(x * 100).toFixed(1)}%`;
const noev = hitRatios('noeviction');
const lru = hitRatios('lru');

console.log(`① LRU — 캐시 용량 ${CAP.toLocaleString()}개 / 전체 키 ${UNIVERSE.toLocaleString()}개 / ${STATIONARY ? '인기 키가 바뀌지 않음' : `인기 키가 ${PHASES}번 바뀜`}\n`);
console.log('| 단계 | noeviction (처음 방식) | lru |');
console.log('|---|---|---|');
for (let p = 0; p < PHASES; p++) console.log(`| ${p + 1} | ${pct(noev.perPhase[p])} | ${pct(lru.perPhase[p])} |`);
console.log(`| **전체 평균** | **${pct(noev.overall)}** | **${pct(lru.overall)}** |`);
console.log(`\nnoeviction 이 가득 차서 저장을 거부한 횟수: ${noev.rejected.toLocaleString()}번\n`);

// ───────────── 2. SCAN: 키가 많을 때 서버가 막히는 시간 ─────────────
console.log(`② SCAN — 키 ${KEYS.toLocaleString()}개를 저장하는 중...`);
const store = new MiniRedis({ sweepIntervalMs: 0 });
for (let i = 0; i < KEYS; i++) store.set(`k${i}`, i);

// 처음 방식: GET /keys (limit 기본 1000). limit 이 작아도 테이블 전체를 복사해서 훑기 시작한다.
store.keys({ limit: 1000 }); // 워밍업
const keysMs = [];
for (let r = 0; r < 5; r++) {
  const a = performance.now();
  store.keys({ limit: 1000 });
  keysMs.push(performance.now() - a);
}

// SCAN: 같은 테이블을 count=100 으로 끝까지 돌며 "한 페이지" 걸리는 시간을 잰다.
let cursor = 0;
const pageMs = [];
do {
  const a = performance.now();
  const r = store.scan(cursor, { count: 100 });
  pageMs.push(performance.now() - a);
  cursor = r.cursor;
} while (cursor !== 0);
const pages = pageMs.length;
const sum = pageMs.reduce((a, b) => a + b, 0);
const sorted = [...pageMs].sort((a, b) => a - b);
const p99 = sorted[Math.floor(pages * 0.99)];
const worst = sorted[pages - 1];
const slow = sorted.filter((d) => d > 1).length; // 1ms 를 넘긴 페이지 수 (GC 등 일시적인 지연)

const avgKeys = keysMs.reduce((a, b) => a + b, 0) / keysMs.length;
console.log(`\n| | GET /keys (처음 방식) | SCAN 한 페이지 (count=100) |`);
console.log('|---|---|---|');
console.log(`| 서버가 막히는 시간 | **${avgKeys.toFixed(0)} ms** (5회 평균) | 평균 **${(sum / pages).toFixed(3)} ms** · 99% **${p99.toFixed(3)} ms** · 최대 ${worst.toFixed(1)} ms |`);
console.log(`| 1ms 를 넘긴 페이지 | - | ${slow}개 / ${pages.toLocaleString()}개 (GC 등 일시적 지연) |`);
console.log(`| 전체를 보려면 | 1번 | ${pages.toLocaleString()}페이지 (사이사이 다른 요청 처리 가능) |`);
console.log(`\n※ ${KEYS.toLocaleString()}개 키, 같은 PC에서 측정한 값. 키가 늘수록 GET /keys 는 비례해서 길어지고 SCAN 한 페이지는 그대로다.`);

store.close();
