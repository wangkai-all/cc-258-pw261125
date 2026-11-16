'use strict';

/**
 * Deterministic demo corpus. Both sources are seeded from the same account ids:
 * Source A owns account attributes (snapshot reads), Source B owns activity
 * attributes (revision-only reads).
 */
function mulberry32(seed) {
  let a = seed >>> 0;
  return function next() {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const ADJECTIVES = [
  'Northwind', 'Copper', 'Harbor', 'Velvet', 'Summit', 'Quartz', 'Willow', 'Falcon',
  'Orchid', 'Ironwood', 'Beacon', 'Cedar', 'Lumen', 'Pioneer', 'Granite', 'Mosaic',
];
const NOUNS = [
  'Labs', 'Trading', 'Logistics', 'Studio', 'Health', 'Robotics', 'Foods', 'Systems',
  'Partners', 'Analytics', 'Textiles', 'Energy', 'Media', 'Freight', 'Devices', 'Works',
];
const REGIONS = ['EU', 'US', 'APAC'];
const PLANS = ['Basic', 'Pro', 'Enterprise'];
const SEGMENTS = ['retail', 'smb', 'mid-market', 'enterprise'];

function buildAccounts(count = 240) {
  const rnd = mulberry32(0xc0ffee);
  const accounts = [];
  for (let i = 1; i <= count; i += 1) {
    const n = i - 1;
    accounts.push({
      account_id: `ACC-${String(i).padStart(4, '0')}`,
      name: `${ADJECTIVES[n % ADJECTIVES.length]} ${NOUNS[(n * 7) % NOUNS.length]} ${String(i).padStart(3, '0')}`,
      region: REGIONS[n % REGIONS.length],
      plan: PLANS[(n * 5) % PLANS.length],
      balance_cents: 25_000 + Math.floor(rnd() * 9_975_000),
    });
  }
  return accounts;
}

function buildActivity(accounts) {
  const rnd = mulberry32(0xbadc0de);
  return accounts.map((account, index) => ({
    account_id: account.account_id,
    segment: SEGMENTS[(index * 3) % SEGMENTS.length],
    risk_score: 5 + Math.floor(rnd() * 94),
  }));
}

module.exports = {
  buildAccounts,
  buildActivity,
  REGIONS,
  PLANS,
  SEGMENTS,
  mulberry32,
};
