/**
 * Mixed-load scenario: 200 writer VUs submit scores concurrently with
 * 800 reader VUs pulling leaderboard data. Models production traffic where
 * writes are a fraction of reads.
 *
 * Run: k6 run load-tests/mixed-load.js
 */

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const writesAccepted = new Counter('writes_accepted');
const readsOk = new Counter('reads_ok');
const writeErrors = new Rate('write_error_rate');
const readErrors = new Rate('read_error_rate');
const writeDuration = new Trend('write_duration_ms', true);
const readDuration = new Trend('read_duration_ms', true);

const GATEWAY = __ENV.GATEWAY_URL || 'http://localhost:8080';
const GAME_ID = __ENV.GAME_ID || 'load-test-game-1';
const PLAYER_COUNT = 100;

export const options = {
  scenarios: {
    writers: {
      executor: 'constant-vus',
      vus: 200,
      duration: '90s',
      exec: 'submitScore',
    },
    readers: {
      executor: 'constant-vus',
      vus: 800,
      duration: '90s',
      exec: 'readLeaderboard',
    },
  },
  thresholds: {
    write_duration_ms: ['p(95)<100'],
    read_duration_ms: ['p(95)<50'],
    write_error_rate: ['rate<0.01'],
    read_error_rate: ['rate<0.001'],
  },
};

export function setup() {
  const players = [];
  const headers = { 'Content-Type': 'application/json' };

  for (let i = 0; i < PLAYER_COUNT; i++) {
    const res = http.post(
      `${GATEWAY}/players`,
      JSON.stringify({ username: `k6-mixed-${Date.now()}-${i}`, display_name: `Mixed User ${i}` }),
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

export function submitScore(data) {
  const playerID = data.players[Math.floor(Math.random() * data.players.length)];
  const score = Math.floor(Math.random() * 100000);

  const res = http.post(
    `${GATEWAY}/scores`,
    JSON.stringify({ player_id: playerID, game_id: data.gameId, score }),
    { headers: { 'Content-Type': 'application/json' } },
  );

  writeDuration.add(res.timings.duration);

  const ok = check(res, { 'write accepted (202)': (r) => r.status === 202 });
  if (res.status === 202) {
    writesAccepted.add(1);
  } else if (res.status !== 429) {
    writeErrors.add(1);
  }
}

export function readLeaderboard(data) {
  const page = Math.ceil(Math.random() * 5);

  const res = http.get(`${GATEWAY}/leaderboard/${data.gameId}?page=${page}&size=50`);

  readDuration.add(res.timings.duration);

  const ok = check(res, { 'read ok (200/404)': (r) => r.status === 200 || r.status === 404 });
  if (ok) {
    readsOk.add(1);
  } else {
    readErrors.add(1);
  }
}
