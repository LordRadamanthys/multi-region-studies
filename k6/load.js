// Continuous load for the multi-region lab. Metrics go to Prometheus (remote write) and show up in Grafana.
//
//   writers  : create / read / update / delete through the global router (:8080)
//   readers  : list + get through the router (a 404 here = the record is not replicated to that region yet)
//   verifier : write through the router, then poll the OTHER region directly until the record is visible.
//              The time it takes is the replication visibility lag; giving up is a "replication timeout".
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://haproxy:8080';
const REGION_URLS = {
  a: __ENV.REGION_A_URL || 'http://service-a:8080',
  b: __ENV.REGION_B_URL || 'http://service-b:8080',
};
const WRITE_RATE = Number(__ENV.WRITE_RATE || 20);
const READ_RATE = Number(__ENV.READ_RATE || 30);
const VERIFY_RATE = Number(__ENV.VERIFY_RATE || 2);
const DURATION = __ENV.DURATION || '30m';
const VERIFY_TIMEOUT_MS = Number(__ENV.VERIFY_TIMEOUT_MS || 10000);

const requests = new Counter('requests');
const latency = new Trend('latency_ms');
const replicationLag = new Trend('replication_lag_ms');
const replicationTimeouts = new Counter('replication_timeouts');

export const options = {
  discardResponseBodies: false,
  scenarios: {
    writers: {
      executor: 'constant-arrival-rate', exec: 'writer',
      rate: WRITE_RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 20, maxVUs: 200,
    },
    readers: {
      executor: 'constant-arrival-rate', exec: 'reader',
      rate: READ_RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 20, maxVUs: 200,
    },
    verifier: {
      executor: 'constant-arrival-rate', exec: 'verifier',
      rate: VERIFY_RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 25, maxVUs: 100,
    },
  },
};

const JSON_HEADERS = { 'Content-Type': 'application/json' };
let seq = 0;
const known = []; // ids this VU created (module scope = per VU)

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

function payload() {
  const n = `${__VU}-${__ITER}-${seq++}`;
  return JSON.stringify({
    name: `User ${n}`,
    address: `Rua Exemplo ${seq}, Sao Paulo`,
    email: `user${n}@example.com`,
    nickname: `nick${n}`,
  });
}

// Counts the request under the region that actually answered (X-Region header), or "none" if nobody did.
function record(op, res) {
  const region = (res.headers['X-Region'] || 'none').toLowerCase();
  requests.add(1, { region, op, status: String(res.status) });
  latency.add(res.timings.duration, { region, op });
  return region;
}

function remember(id) {
  known.push(id);
  if (known.length > 200) known.shift();
}

export function writer() {
  const roll = Math.random();
  if (known.length === 0 || roll < 0.6) {
    const res = http.post(`${BASE_URL}/customers`, payload(), {
      headers: { ...JSON_HEADERS, 'Idempotency-Key': uuid() },
      tags: { op: 'create' }, timeout: '5s',
    });
    record('create', res);
    if (res.status === 201 || res.status === 200) remember(res.json('id'));
  } else if (roll < 0.85) {
    const id = known[Math.floor(Math.random() * known.length)];
    record('update', http.put(`${BASE_URL}/customers/${id}`, payload(), {
      headers: JSON_HEADERS, tags: { op: 'update' }, timeout: '5s',
    }));
  } else if (roll < 0.95) {
    const id = known[Math.floor(Math.random() * known.length)];
    record('get', http.get(`${BASE_URL}/customers/${id}`, { tags: { op: 'get' }, timeout: '5s' }));
  } else {
    const id = known.splice(Math.floor(Math.random() * known.length), 1)[0];
    record('delete', http.del(`${BASE_URL}/customers/${id}`, null, { tags: { op: 'delete' }, timeout: '5s' }));
  }
}

export function reader() {
  const list = http.get(`${BASE_URL}/customers?limit=20`, { tags: { op: 'list' }, timeout: '5s' });
  record('list', list);
  if (list.status !== 200) return;
  const items = list.json('items') || [];
  if (items.length === 0) return;
  const id = items[Math.floor(Math.random() * items.length)].id;
  record('get', http.get(`${BASE_URL}/customers/${id}`, { tags: { op: 'get' }, timeout: '5s' }));
}

export function verifier() {
  const res = http.post(`${BASE_URL}/customers`, payload(), {
    headers: { ...JSON_HEADERS, 'Idempotency-Key': uuid() },
    tags: { op: 'verify_write' }, timeout: '5s',
  });
  const writerRegion = record('verify_write', res);
  if (res.status !== 201 || !REGION_URLS[writerRegion]) return;

  const id = res.json('id');
  const other = writerRegion === 'a' ? 'b' : 'a';
  const start = Date.now();
  while (Date.now() - start < VERIFY_TIMEOUT_MS) {
    const r = http.get(`${REGION_URLS[other]}/customers/${id}`, { tags: { op: 'verify_read' }, timeout: '2s' });
    if (r.status === 200) {
      replicationLag.add(Date.now() - start, { from: writerRegion, to: other });
      return;
    }
    sleep(0.1);
  }
  replicationTimeouts.add(1, { from: writerRegion, to: other });
}
