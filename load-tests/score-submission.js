import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const accepted = new Counter('scores_accepted');
const rateLimited = new Counter('scores_rate_limited');
const errorRate = new Rate('score_error_rate');

const GATEWAY = __ENV.GATEWAY_URL || 'http://localhost:8080';
const GAME_ID = __ENV.GAME_ID || 'load-test-game-1';
const PLAYER_COUNT = 50;

export const options = {
  stages: [
    { duration: '30s', target: 100 },
    { duration: '30s', target: 300 },
    { duration: '30s', target: 500 },
    { duration: '60s', target: 500 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<100'],
    score_error_rate: ['rate<0.01'],
  },
};

export function setup() {
  const players = [];
  const headers = { 'Content-Type': 'application/json' };

  for (let i = 0; i < PLAYER_COUNT; i++) {
    const res = http.post(
      `${GATEWAY}/players`,
      JSON.stringify({ username: `k6-user-${Date.now()}-${i}`, display_name: `K6 User ${i}` }),
      { headers },
    );
    if (res.status === 201) {
      players.push(JSON.parse(res.body).id);
    }
  }

  if (players.length === 0) {
    throw new Error('setup: failed to create any players — is the stack running?');
  }

  return { players, gameId: GAME_ID };
}

export default function (data) {
  const playerID = data.players[Math.floor(Math.random() * data.players.length)];
  const score = Math.floor(Math.random() * 100000);

  const res = http.post(
    `${GATEWAY}/scores`,
    JSON.stringify({ player_id: playerID, game_id: data.gameId, score }),
    { headers: { 'Content-Type': 'application/json' } },
  );

  const ok = check(res, {
    'accepted (202)': (r) => r.status === 202,
    'has request id': (r) => r.headers['X-Request-Id'] !== undefined,
  });

  if (res.status === 202) {
    accepted.add(1);
  } else if (res.status === 429) {
    rateLimited.add(1);
  } else {
    errorRate.add(1);
  }
}
