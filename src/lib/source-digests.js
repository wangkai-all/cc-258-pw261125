'use strict';

const { sha256, canonical } = require('./canonical');

/**
 * Digest definitions are shared by the sources and the coordinator on purpose:
 * the coordinator recomputes every digest it receives, so a transport or
 * pagination bug cannot masquerade as a consistent result.
 */

function accountRow(row) {
  return {
    account_id: row.account_id,
    name: row.name,
    region: row.region,
    plan: row.plan,
    balance_cents: row.balance_cents,
  };
}

function activityRow(row) {
  return {
    account_id: row.account_id,
    segment: row.segment,
    risk_score: row.risk_score,
  };
}

/** Digest of an ordered account list (already filtered by the source). */
function accountRowsDigest(rows) {
  return sha256(rows.map((row) => canonical(accountRow(row))).join('\n'));
}

/** Digest of an ordered activity list. */
function activityRowsDigest(rows) {
  return sha256(rows.map((row) => canonical(activityRow(row))).join('\n'));
}

/**
 * Digest of a by-ids lookup, in request order. Missing ids are part of the
 * digest so "not found" is a verifiable fact rather than an omission.
 */
function activityIdsDigest(ids, rowsById) {
  const entries = ids.map((id) => {
    const row = rowsById.get(id);
    if (!row) return { account_id: id, present: false };
    return {
      account_id: id,
      present: true,
      segment: row.segment,
      risk_score: row.risk_score,
    };
  });
  return sha256(entries.map((entry) => canonical(entry)).join('\n'));
}

/** Chain of page digests; re-verifiable at the end of a run. */
function pageChainDigest(sliceDigests) {
  return sha256(sliceDigests.join('\n'));
}

module.exports = {
  accountRow,
  activityRow,
  accountRowsDigest,
  activityRowsDigest,
  activityIdsDigest,
  pageChainDigest,
};
