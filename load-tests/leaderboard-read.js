import http from 'k6/http';
import { check } from 'k6';
import { Rate, Counter } from 'k6/metrics';

const cacheHitRate = new Rate('cache_hit_rate');
const notFoundCount = new Counter('leaderboard_not_found');

const GATEWAY = __ENV.GATEWAY_URL || 'http://localhost:8080';
const GAME_ID = __ENV.GAME_ID || 'load-test-game-1';

export const options = {
  scenarios: {
    leaderboard_reads: {
      executor: 'constant-vus',
      vus: 1000,
      duration: '60s',
    },
  },
  thresholds: {
    // In-process cache should serve most requests under 10ms
    http_req_duration: ['p(95)<50', 'p(99)<200'],
    http_req_failed: ['rate<0.001'],
  },
};

export function setup() {
  // Warm the cache with one request before the flood
  http.get(`${GATEWAY}/leaderboard/${GAME_ID}?page=1&size=50`);
  return { gameId: GAME_ID };
}

export default function (data) {
  // Vary page to exercise pagination; size 50 is the default
  const page = Math.ceil(Math.random() * 3);

  const res = http.get(`${GATEWAY}/leaderboard/${data.gameId}?page=${page}&size=50`);

  const ok = check(res, {
    'status 200 or 404': (r) => r.status === 200 || r.status === 404,
    'has request id': (r) => r.headers['X-Request-Id'] !== undefined,
  });

  if (res.status === 200) {
    // Sub-10ms responses are almost certainly served from the in-process cache
    cacheHitRate.add(res.timings.duration < 10 ? 1 : 0);
  } else if (res.status === 404) {
    notFoundCount.add(1);
  }
}
