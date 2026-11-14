import http from 'k6/http';
import { check, sleep } from 'k6';

// k6 Load Test Configuration: Simulating Final 10-Second Bidding Rush
export const options = {
  scenarios: {
    final_rush: {
      executor: 'ramping-vus',
      startVUs: 100,
      stages: [
        { duration: '3s', target: 500 },   // Warmup
        { duration: '5s', target: 2000 },  // Peak 10-second storm
        { duration: '2s', target: 0 },     // Cooldown
      ],
      gracefulRampDown: '1s',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<50', 'p(99)<100'], // 95% of bids must complete in under 50ms
    http_req_failed: ['rate<0.05'],               // Error rate below 5%
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://localhost:8000';

export default function () {
  const userId = `vu_user_${__VU}_${Math.floor(Math.random() * 1000)}`;
  const bidId = `bid_${__VU}_${Date.now()}_${Math.floor(Math.random() * 10000)}`;
  const amount = 105.0 + Math.floor(Math.random() * 50);

  const payload = JSON.stringify({
    bid_id: bidId,
    auction_id: 'auction_alpha',
    user_id: userId,
    amount: amount,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    timeout: '5s',
  };

  const res = http.post(`${BASE_URL}/bid`, payload, params);

  // Status can be 200 (Accepted), 400 (Bid too low / normal rejected), 429 (Rate limited)
  check(res, {
    'valid response status': (r) => r.status === 200 || r.status === 400 || r.status === 429,
    'latency under 100ms': (r) => r.timings.duration < 100,
  });

  sleep(0.05); // Rapid 50ms pause before next counter-bid
}
