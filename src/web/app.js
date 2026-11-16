'use strict';

const state = {
  runId: null,
  run: null,
  verify: null,
  policies: [],
  retryChoice: null,
  retryOptions: [],
  busy: false,
  pageDetail: null,
  sourceA: null,
  sourceB: null,
};

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

function esc(value) {
  return String(value === null || value === undefined ? '' : value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function short(value, n = 12) {
  if (value === null || value === undefined) return '—';
  const s = String(value);
  return s.length > n ? `${s.slice(0, n)}…` : s;
}

function time(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleTimeString('zh-CN', { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

function toast(message, kind = 'info', ms = 5200) {
  let stack = $('.toast-stack');
  if (!stack) {
    stack = document.createElement('div');
    stack.className = 'toast-stack';
    document.body.appendChild(stack);
  }
  // Collapse duplicates: a poll or a burst of steps should not carpet the page.
  const existing = Array.from(stack.children).find((el) => el.dataset.msg === message);
  if (existing) {
    existing.remove();
  }
  while (stack.children.length >= 3) stack.firstElementChild.remove();
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.dataset.msg = message;
  el.textContent = message;
  stack.appendChild(el);
  setTimeout(() => el.remove(), ms);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || 'GET',
    headers: options.body ? { 'content-type': 'application/json' } : undefined,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  const text = await response.text();
  let payload = null;
  try { payload = text ? JSON.parse(text) : null; } catch { payload = { raw: text }; }
  if (!response.ok) {
    const error = new Error((payload && payload.error && payload.error.message) || `HTTP ${response.status}`);
    error.payload = payload;
    error.code = payload && payload.error && payload.error.code;
    error.status = response.status;
    throw error;
  }
  return payload;
}

function setBusy(busy, label) {
  state.busy = busy;
  for (const id of ['step1', 'step3', 'stepall', 'cancel-run', 'resume-run', 'verify-run', 'apply-retry']) {
    const el = document.getElementById(id);
    if (el) el.disabled = busy;
  }
  if (busy && label) toast(label, 'info', 1800);
}

/* ---------------- sources ---------------- */

async function refreshSources() {
  try {
    const [a, b] = await Promise.all([
      api('/api/sources/a/admin/state'),
      api('/api/sources/b/admin/state'),
    ]);
    state.sourceA = a;
    state.sourceB = b;
    const latestA = a.snapshots[0];
    const latestB = b.revisions[0];
    $('#live-a').textContent = latestA ? `${latestA.token} · seq ${latestA.data_seq}` : '—';
    $('#live-b').textContent = latestB ? `r${latestB.revision} · floor ${b.floor}` : '—';
    $('#a-version').textContent = latestA ? latestA.token : '—';
    $('#a-meta').textContent = latestA
      ? `${latestA.row_count} 行 · ${latestA.reason} · ${time(latestA.created_at)}`
      : '—';
    $('#b-version').textContent = latestB ? `revision ${latestB.revision}` : '—';
    $('#b-meta').textContent = latestB
      ? `${latestB.row_count} 行 · 保留 ${b.retention} 个修订 · floor ${b.floor}`
      : '—';
    const live = $('#live-status');
    const aFault = a.faults.delay_ms > 0 || a.faults.fail_next > 0 || a.faults.fail_reads.some((r) => r.times > 0);
    const bFault = b.faults.delay_ms > 0 || b.faults.fail_next > 0 || b.faults.fail_reads.some((r) => r.times > 0);
    live.querySelector('[data-src="A"]').classList.toggle('degraded', aFault);
    live.querySelector('[data-src="B"]').classList.toggle('degraded', bFault);
    renderReadLog(a, b);
  } catch (err) {
    toast(`来源状态读取失败：${err.message}`, 'bad');
  }
}

function renderReadLog(a, b) {
  const lines = [];
  for (const entry of (a.reads || []).slice(0, 6)) {
    lines.push(`<span class="dim">${time(entry.at)}</span> <span class="chip a">A</span> ${esc(entry.kind)}${entry.snapshot ? ' ' + esc(entry.snapshot) : ''}${entry.after ? ' after=' + esc(entry.after) : ''} ${entry.status === 200 ? '<span class="ok">200</span>' : `<span class="bad">${esc(entry.status)} ${esc(entry.code || '')}</span>`}${entry.rows !== undefined ? ` rows=${entry.rows}` : ''}`);
  }
  for (const entry of (b.reads || []).slice(0, 6)) {
    lines.push(`<span class="dim">${time(entry.at)}</span> <span class="chip b">B</span> ${esc(entry.kind)}${entry.revision ? ' r' + esc(entry.revision) : ''}${entry.after ? ' after=' + esc(entry.after) : ''} ${entry.status === 200 ? '<span class="ok">200</span>' : `<span class="bad">${esc(entry.status)} ${esc(entry.code || '')}</span>`}${entry.rows !== undefined ? ` rows=${entry.rows}` : ''}`);
  }
  $('#read-log').innerHTML = lines.length ? lines.join('\n') : '<span class="dim">尚无读取</span>';
}

async function sourceAction(action, body = {}) {
  const [source, name] = action.split(':');
  try {
    const result = await api(`/api/sources/${source}/admin/${name}`, { method: 'POST', body });
    toast(`${source.toUpperCase()} 已执行 ${name}`, 'ok');
    await refreshSources();
    if (state.runId) {
      await loadRun(state.runId);
      await refreshGuidance();
    }
    return result;
  } catch (err) {
    toast(`${source.toUpperCase()} ${name} 失败：${err.message}`, 'bad');
    return null;
  }
}

async function applyFaults(source) {
  const prefix = source === 'a' ? '#a' : '#b';
  const delay = Number($(`${prefix}-delay`).value) || 0;
  const failNext = Number($(`${prefix}-failn`).value) || 0;
  const failAt = Number($(`${prefix}-failat`).value) || 0;
  const body = { delay_ms: delay, fail_next: failNext, fail_reads: [] };
  if (failAt > 0) body.fail_reads = [{ at: failAt, times: 1, code: 'injected_failure', message: `页面注入：第 ${failAt} 次读取失败` }];
  await sourceAction(`${source}:faults`, body);
}

async function clearFaults(source) {
  await sourceAction(`${source}:faults`, { delay_ms: 0, fail_next: 0, fail_reads: [] });
  $(`#${source}-delay`).value = '0';
  $(`#${source}-failn`).value = '0';
}

/* ---------------- run lifecycle ---------------- */

function runStateLabel(stateName) {
  return {
    binding: '绑定中',
    bound: '已绑定',
    running: '读取中',
    completed: '已完成',
    failed: '已停止',
    cancelled: '已取消',
  }[stateName] || stateName;
}

function contractCard(run) {
  const c = run.contract;
  return `
    <div><b>${esc(c.name)}</b> <span class="guarantee">${esc(c.guarantee)}</span></div>
    <div style="margin-top:5px">承诺：${esc(c.promise)}</div>
    <div style="margin-top:4px" class="cost">代价：${esc(c.cost)}</div>
    <div style="margin-top:4px" class="muted">失败时：${esc(c.failure_behavior)}</div>`;
}

async function createRun(event) {
  event.preventDefault();
  if (state.busy) return;
  const form = event.currentTarget;
  const data = new FormData(form);
  const body = {
    policy: data.get('policy'),
    join: data.get('join'),
    limit: Number(data.get('limit')),
    filters: {
      region: data.get('region') || null,
      plan: data.get('plan') || null,
      min_balance: data.get('min_balance') === '' ? null : Number(data.get('min_balance')),
    },
  };
  setBusy(true, '正在绑定两个来源的版本…');
  try {
    const result = await api('/api/runs', { method: 'POST', body });
    state.runId = result.run.id;
    state.verify = null;
    toast(`已绑定 ${result.run.binding.a.token} / r${result.run.binding.b.revision}`, 'ok');
    await loadRun(state.runId);
    await loadRecent();
  } catch (err) {
    const details = err.payload && err.payload.error && err.payload.error.details;
    toast(`绑定失败：${err.message}${details && details.failure ? ' · ' + details.failure.source : ''}`, 'bad', 9000);
    if (details && details.run_id) {
      state.runId = details.run_id;
      await loadRun(state.runId);
    }
  } finally {
    setBusy(false);
  }
}

/**
 * Re-probe the pinned versions for a stopped run so the retry options the page
 * shows reflect the sources as they are now, not as they were when it stopped.
 */
async function refreshGuidance() {
  if (!state.runId || !state.run) return null;
  if (state.run.state !== 'failed' && state.run.state !== 'cancelled') return null;
  try {
    const result = await api(`/api/runs/${state.runId}/retry-guidance`, { method: 'POST' });
    state.run = result.run;
    renderRun();
    return result;
  } catch (err) {
    // Guidance is advisory: a failed probe must not break the rest of the page.
    return null;
  }
}

async function loadRun(runId) {
  try {
    const payload = await api(`/api/runs/${encodeURIComponent(runId)}`);
    state.run = payload.run;
    state.runId = payload.run.id;
    $('#cache-stats').textContent = `页缓存 ${payload.cache.entries} 条 · 命中 ${payload.cache.hits}`;
    renderRun();
    return payload.run;
  } catch (err) {
    if (err.status === 404) {
      state.run = null;
      state.runId = null;
      renderEmpty();
      return null;
    }
    toast(`读取运行失败：${err.message}`, 'bad');
    return null;
  }
}

async function step(pages) {
  if (!state.runId || state.busy) return;
  setBusy(true, pages === 'all' ? '持续读取直到结束…' : '读取中…');
  try {
    let remaining = pages === 'all' ? 40 : Number(pages);
    let last = null;
    while (remaining > 0) {
      const batch = pages === 'all' ? Math.min(10, remaining) : remaining;
      last = await api(`/api/runs/${state.runId}/step`, { method: 'POST', body: { pages: batch } });
      remaining -= batch;
      if (last.stopped === 'failed') {
        toast(`在第 ${last.run.failure.pageNo} 页停止：${last.run.failure.source} ${last.run.failure.code}`, 'bad', 9000);
        break;
      }
      if (last.run.state === 'completed' || last.run.state === 'failed' || last.run.state === 'cancelled') break;
    }
    if (last && last.run.state === 'completed' && pages !== 'all') {
      toast(`已完成：${last.run.emitted.pages} 页 / ${last.run.emitted.rows} 行`, 'ok');
    }
    if (last && last.run.state === 'completed' && pages === 'all') {
      toast(`已完成：${last.run.emitted.pages} 页 / ${last.run.emitted.rows} 行`, 'ok');
    }
    state.run = last.run;
    renderRun();
    await loadRecent();
    await refreshSources();
  } catch (err) {
    toast(`读取失败：${err.message}`, 'bad');
    if (err.payload && err.payload.error && err.payload.error.details && err.payload.error.details.run_id) {
      await loadRun(err.payload.error.details.run_id);
    }
  } finally {
    setBusy(false);
  }
}

async function cancelRun() {
  if (!state.runId) return;
  try {
    const result = await api(`/api/runs/${state.runId}/cancel`, { method: 'POST', body: { reason: '页面点击取消' } });
    state.run = result.run;
    renderRun();
    toast('已取消。已输出的分页保持原版本，恢复时从同一位置继续。', 'warn');
  } catch (err) {
    toast(`取消失败：${err.message}`, 'bad');
  }
}

async function resumeRun() {
  if (!state.runId) return;
  try {
    const result = await api(`/api/runs/${state.runId}/resume`, { method: 'POST' });
    state.run = result.run;
    renderRun();
    toast('已恢复，接下来按同一固定版本继续读取。', 'ok');
  } catch (err) {
    toast(`恢复失败：${err.message}`, 'bad');
    await loadRun(state.runId);
  }
}

async function verifyRun() {
  if (!state.runId) return;
  setBusy(true, '正在从来源与分页重算摘要…');
  try {
    state.verify = await api(`/api/runs/${state.runId}/verify`);
    renderRun();
    toast(state.verify.ok ? '校验通过：结果与该组版本一致' : '校验发现问题', state.verify.ok ? 'ok' : 'bad');
  } catch (err) {
    toast(`校验失败：${err.message}`, 'bad');
  } finally {
    setBusy(false);
  }
}

/* ---------------- rendering ---------------- */

function renderEmpty() {
  $('#empty').hidden = false;
  $('#run-view').hidden = true;
}

function renderRun() {
  const run = state.run;
  if (!run) return renderEmpty();
  $('#empty').hidden = true;
  $('#run-view').hidden = false;

  $('#run-id').textContent = run.id;
  const stateEl = $('#run-state');
  stateEl.textContent = runStateLabel(run.state);
  stateEl.className = `state ${run.state}`;
  $('#run-updated').textContent = `更新于 ${time(run.updated_at)} · epoch ${run.epoch}`;
  $('#run-contract').innerHTML = contractCard(run);

  const binding = run.binding;
  $('#stat-a').textContent = binding ? binding.a.token : '—';
  $('#stat-a-detail').textContent = binding
    ? `seq ${binding.a.data_seq} · ${binding.a.row_count} 行 · 摘要 ${short(binding.a.content_digest, 10)}`
    : '尚未绑定';
  $('#stat-b').textContent = binding ? `revision ${binding.b.revision}` : '—';
  $('#stat-b-detail').textContent = binding
    ? `${binding.b.row_count} 行 · 摘要 ${short(binding.b.content_digest, 10)}`
    : '尚未绑定';
  $('#stat-emitted').textContent = run.emitted.pages
    ? `${run.emitted.pages} 页 / ${run.emitted.rows} 行`
    : '尚未输出';
  $('#stat-emitted-detail').textContent = run.emitted.first_page
    ? `页 ${run.emitted.first_page}–${run.emitted.last_page} · ${run.emitted.first_account || '—'} → ${run.emitted.last_account || '—'}`
    : '—';
  $('#stat-cursor').textContent = run.cursor.after || (run.state === 'completed' ? '已读完' : '从头开始');
  $('#stat-cursor-detail').textContent = run.state === 'completed'
    ? `共 ${run.emitted.pages} 页，无下一页`
    : `下一页 #${run.cursor.pageNo}${run.cursor.after ? ' · after ' + run.cursor.after : ''}`;

  const hasEmitted = run.emitted.pages > 0;
  const done = run.state === 'completed';
  $('#step1').disabled = state.busy || done || run.state === 'failed' || run.state === 'cancelled';
  $('#step3').disabled = state.busy || done || run.state === 'failed' || run.state === 'cancelled';
  $('#stepall').disabled = state.busy || done || run.state === 'failed' || run.state === 'cancelled';
  $('#cancel-run').disabled = state.busy || done || run.state === 'cancelled';
  $('#cancel-run').hidden = run.state === 'cancelled' || done;
  $('#resume-run').hidden = run.state !== 'cancelled';
  $('#resume-run').disabled = state.busy;
  $('#verify-run').disabled = state.busy;

  renderFailure(run);
  renderRetry(run);
  renderVerify(run);
  renderPages(run);
  renderSegments(run);
  renderEvents(run);
  $('#pages-caption').textContent = hasEmitted
    ? `· ${run.emitted.pages} 页，覆盖 ${run.emitted.first_account || '—'} → ${run.emitted.last_account || '—'}`
    : '';
}

function renderFailure(run) {
  const card = $('#failure-card');
  if (!run.failure) { card.hidden = true; return; }
  card.hidden = false;
  const f = run.failure;
  const loss = run.version_loss;
  $('#failure-body').innerHTML = `
    <div class="failure-grid">
      <div class="fcard"><div class="k">失败页</div><div class="v">${f.pageNo === null ? '绑定阶段' : '#' + esc(f.pageNo)}</div></div>
      <div class="fcard"><div class="k">失败来源</div><div class="v"><span class="tag ${f.source === 'B' ? 'b' : 'a'}">${esc(f.source)}</span> ${esc(f.phase)}</div></div>
      <div class="fcard"><div class="k">失败原因</div><div class="v">${esc(f.code)} <span class="muted">HTTP ${esc(f.status)}</span></div></div>
      <div class="fcard"><div class="k">已输出到此</div><div class="v">${esc(f.emitted_pages === null ? run.emitted.pages : f.emitted_pages)} 页${f.cursor && f.cursor.after ? ' · after ' + esc(f.cursor.after) : ''}</div></div>
    </div>
    <div class="failure-note">${esc(f.message)}${f.note ? '<br><span class="muted">' + esc(f.note) + '</span>' : ''}</div>
    ${loss ? `<div class="failure-note" style="border-left-color:var(--warn)">
      <b>版本已不可重读。</b> ${loss.source === 'B'
        ? `B 的 revision ${loss.lost ? loss.lost.revision : '?'} 已低于保留下限${loss.floor !== null && loss.floor !== undefined ? '（floor ' + esc(loss.floor) + '）' : ''}`
        : `A 的快照 ${loss.lost ? esc(loss.lost.token) : '?'} 已被清理`}。
      固定版本口径（pin）已无法继续，只能在「整次重试」与「显式不一致」之间选择。
    </div>` : ''}`;
}

function renderRetry(run) {
  const card = $('#retry-card');
  const retryable = run.state === 'failed' || run.state === 'cancelled';
  if (!retryable) { card.hidden = true; return; }
  card.hidden = false;
  const guidance = run.next_retry || {};
  const options = guidance.options || [];
  state.retryOptions = options;
  const currentPolicy = run.policy;
  const loss = run.version_loss;
  const alive = !loss;

  $('#retry-note').textContent = guidance.note || '';

  const grid = $('#retry-options');
  grid.innerHTML = options.map((opt) => {
    const recommended = guidance.recommended === opt.policy;
    const blocked = opt.possible === false;
    const isCurrent = opt.policy === currentPolicy;
    const selected = state.retryChoice === opt.policy || (!state.retryChoice && (recommended || isCurrent));
    const req = opt.requires_acknowledgement ? `<div class="req">需要显式确认：${esc(opt.requires_acknowledgement)}</div>` : '';
    const blockedNote = blocked ? `<div class="req">${esc(opt.blocked_reason || '当前不可用')}</div>` : '';
    return `
      <button type="button" class="retry-opt ${recommended ? 'recommended' : ''}" data-policy="${esc(opt.policy)}"
        aria-pressed="${selected}" ${blocked ? 'disabled' : ''}>
        <span class="name">${policyName(opt.policy)} ${recommended ? '<span class="chip cache">推荐</span>' : ''}${isCurrent ? '<span class="chip">当前</span>' : ''}</span>
        <span class="effect">${esc(opt.effect)}</span>
        ${req}
        ${blockedNote}
      </button>`;
  }).join('');

  for (const button of $$('.retry-opt', grid)) {
    button.addEventListener('click', () => {
      state.retryChoice = button.dataset.policy;
      for (const other of $$('.retry-opt', grid)) other.setAttribute('aria-pressed', String(other === button));
      updateAck(run);
    });
  }
  if (!state.retryChoice || !options.some((o) => o.policy === state.retryChoice && o.possible !== false)) {
    state.retryChoice = (options.find((o) => o.policy === guidance.recommended && o.possible !== false) || options.find((o) => o.possible !== false) || options[0] || {}).policy || null;
  }
  updateAck(run);
}

function policyName(policy) {
  return { pin: '固定版本 pin', restart: '整次重试 restart', mixed: '显式不一致 mixed' }[policy] || policy;
}

function updateAck(run) {
  const policy = state.retryChoice;
  const hasEmitted = run.emitted.pages > 0;
  const needsAck = hasEmitted && (policy === 'restart' || policy === 'mixed');
  const ackWrap = $('#ack-wrap');
  ackWrap.style.display = needsAck ? 'flex' : 'none';
  $('#ack').checked = false;
  if (policy === 'restart') {
    $('#ack-label').innerHTML = `我知道第 ${esc(run.emitted.first_page)}–${esc(run.emitted.last_page)} 页（${esc(run.emitted.rows)} 行）会被作废，必须由消费方丢弃。`;
  } else if (policy === 'mixed') {
    $('#ack-label').innerHTML = `我接受第 ${esc(run.emitted.first_page)}–${esc(run.emitted.last_page)} 页与之后的分页来自不同修订版本，并在结果末尾看到版本边界声明。`;
  }
}

async function applyRetry() {
  if (!state.runId || !state.retryChoice) return;
  const run = state.run;
  const hasEmitted = run.emitted.pages > 0;
  const needsPolicyAck = hasEmitted && (state.retryChoice === 'restart' || state.retryChoice === 'mixed');
  const body = { policy: state.retryChoice };
  if (state.retryChoice === 'restart') body.acknowledge_invalidation = $('#ack').checked;
  if (state.retryChoice === 'mixed') body.acknowledge_mixed = $('#ack').checked;
  if (needsPolicyAck && !$('#ack').checked) {
    toast('该口径会改变已交付结果的承诺，需要先勾选确认。', 'warn');
    return;
  }
  setBusy(true, '正在按所选口径重试…');
  try {
    const result = await api(`/api/runs/${state.runId}/retry`, { method: 'POST', body });
    state.run = result.run;
    state.retryChoice = null;
    const v = result.verification;
    if (result.invalidated) {
      toast(`已作废 epoch ${result.invalidated.epoch} 的第 ${result.invalidated.pages.from_page}–${result.invalidated.pages.to_page} 页，从页 1 重新开始。`, 'warn', 9000);
    } else if (v && v.ok) {
      toast('两个固定版本仍可重读且内容未变，从失败位置继续。', 'ok');
    } else if (result.contract_change) {
      toast(`口径已从 ${result.contract_change.from} 改为 ${result.contract_change.to}，该变化对消费方可见。`, 'warn', 9000);
    } else {
      toast('重试已就绪。', 'ok');
    }
    renderRun();
    await loadRecent();
  } catch (err) {
    const details = err.payload && err.payload.error && err.payload.error.details;
    toast(`重试被拒绝：${err.message}`, 'bad', 9000);
    if (details && details.checks) {
      state.verify = null;
    }
    await loadRun(state.runId);
  } finally {
    setBusy(false);
  }
}

function checkRows(checks) {
  const failing = checks.filter((c) => !c.ok);
  const shown = failing.length ? failing : checks.slice(0, 40);
  return shown.map((c) => `
    <div class="check ${c.ok ? 'ok' : 'bad'}">
      <span class="icon">${c.ok ? '✓' : '✗'}</span>
      <span><span class="name">${esc(c.source)} · ${esc(c.name)}</span>
        ${c.detail ? `<br><span class="muted">${esc(c.detail)}</span>` : ''}</span>
      <span class="val">${c.expected !== undefined ? '期望 ' + short(c.expected, 14) + '<br>' : ''}${c.actual !== undefined ? '实际 ' + short(c.actual, 14) : ''}</span>
    </div>`).join('');
}

function renderVerify(run) {
  const card = $('#verify-card');
  const result = state.verify;
  if (!result || result.run_id !== run.id) { card.hidden = true; return; }
  card.hidden = false;
  const verdictClass = result.verdict === 'integrity_failed' ? 'bad' : 'ok';
  $('#verify-summary').innerHTML = `
    <span class="verdict ${verdictClass}">${esc(result.verdict_text || (result.ok ? '校验通过' : '校验未通过'))}</span>
    <span class="muted">重算 ${result.pages_checked} 页 · ${result.mixed ? '跨 ' + result.versions.length + ' 个版本段' : '单一版本段'} · 状态 ${runStateLabel(result.state)}</span>
    <span class="chip ${result.re_readable ? 'cache' : 'warn'}">来源当前${result.re_readable ? '可重读' : '已不可重读'}</span>`;

  const total = result.versions.reduce((sum, v) => sum + ((v.to_page ?? run.emitted.last_page ?? v.from_page) - v.from_page + 1), 0) || 1;
  $('#version-timeline').innerHTML = result.versions.map((v, i) => {
    const to = v.to_page ?? run.emitted.last_page ?? v.from_page;
    const span = Math.max(1, to - v.from_page + 1);
    const pct = (span / total) * 100;
    const color = i % 2 === 0 ? '#58b6d8' : '#d9a441';
    return `<div class="seg ${i > 0 ? 'drift' : ''}" style="flex:0 0 ${pct}%;background:${color}"
      title="页 ${v.from_page}–${to} · A ${esc(v.a)} · B r${esc(v.b)}">${v.from_page}–${to}</div>`;
  }).join('');

  $('#verify-checks').innerHTML = `
    <div class="check-group-title">已交付结果的自洽性（与来源当前是否保留无关）</div>
    ${checkRows(result.integrity)}
    <div class="check-group-title">来源当前可重读性（只影响能否继续/重放，不影响已交付结果）</div>
    ${checkRows(result.availability)}`;
}

function renderPages(run) {
  const body = $('#pages-body');
  if (!run.pages.length) {
    body.innerHTML = '<tr><td colspan="8" class="muted" style="padding:14px">还没有输出任何分页。</td></tr>';
    return;
  }
  body.innerHTML = run.pages.slice().reverse().map((page) => {
    const drift = page.versions.b.revision !== run.binding.b.revision || page.versions.a.token !== run.binding.a.token;
    return `
    <tr>
      <td class="mono">#${page.pageNo}</td>
      <td><span class="chip a">${esc(short(page.versions.a.token, 20))}</span></td>
      <td><span class="chip b">r${esc(page.versions.b.revision)}</span>${drift ? ' <span class="chip warn">旧段</span>' : ''}</td>
      <td class="mono">${page.row_count}</td>
      <td class="mono">${esc(page.account_range.from || '—')} → ${esc(page.account_range.to || '—')}</td>
      <td class="mono">${esc(short(page.digest, 14))}</td>
      <td>${page.cache && page.cache.hit ? `<span class="chip cache">缓存 ${esc(page.cache.kind === 'run-page' ? '本运行' : '跨运行')}</span>` : '<span class="chip">来源读取</span>'}${page.warnings && page.warnings.length ? ' <span class="chip warn">' + page.warnings.length + ' 警告</span>' : ''}</td>
      <td><button class="small ghost" type="button" data-page="${page.pageNo}">查看</button></td>
    </tr>`;
  }).join('');
  for (const button of $$('[data-page]', body)) {
    button.addEventListener('click', () => showPage(Number(button.dataset.page)));
  }
}

async function showPage(pageNo) {
  if (!state.runId) return;
  try {
    const result = await api(`/api/runs/${state.runId}/pages/${pageNo}`);
    state.pageDetail = result.page;
    const page = result.page;
    const detail = $('#page-detail');
    detail.hidden = false;
    detail.innerHTML = `
      <h3>第 ${page.pageNo} 页 · epoch ${page.epoch} · ${page.row_count} 行</h3>
      <div class="meta-row">
        <span>A after <span class="mono">${esc(page.cursor_before || '∅')}</span></span>
        <span>A next <span class="mono">${esc(page.cursor_after || '结束')}</span></span>
        <span>A 行摘要 <span class="mono">${esc(short(page.a.digest, 16))}</span></span>
        <span>B 查询 <span class="mono">${page.b.ids.length}</span> 个 id</span>
        <span>B 缺失 <span class="mono">${page.b.missing.length}</span></span>
        <span>B 摘要 <span class="mono">${esc(short(page.b.digest, 16))}</span></span>
        <span>读于 ${time(page.read_at.a)} / ${time(page.read_at.b)}</span>
        <span>页摘要 <span class="mono">${esc(short(page.page_digest, 20))}</span></span>
      </div>
      <div class="tablewrap"><table class="rows">
        <thead><tr><th>account</th><th>name</th><th>region</th><th>plan</th><th>balance</th><th>segment</th><th>risk</th></tr></thead>
        <tbody>${page.rows.map((row) => `
          <tr>
            <td class="mono">${esc(row.account_id)}</td>
            <td>${esc(row.name)}</td>
            <td>${esc(row.region)}</td>
            <td>${esc(row.plan)}</td>
            <td class="mono">${esc((row.balance_cents / 100).toFixed(2))}</td>
            <td>${row.activity_present ? esc(row.segment) : '<span class="missing">B 无记录</span>'}</td>
            <td class="mono">${row.risk_score === null ? '—' : esc(row.risk_score)}</td>
          </tr>`).join('')}
        </tbody>
      </table></div>`;
  } catch (err) {
    toast(`该页不在当前 epoch：${err.message}`, 'bad');
  }
}

function renderSegments(run) {
  const el = $('#segments');
  if (!run.version_segments.length) {
    el.innerHTML = '<p class="muted">尚未绑定版本。</p>';
    return;
  }
  el.innerHTML = run.version_segments.map((seg, i) => `
    <div class="segment">
      <span class="range">页 ${seg.from_page}–${seg.to_page === null ? '…' : seg.to_page}</span>
      <span><span class="tag a">A</span><span class="mono">${esc(seg.a.token)}</span><br><span class="muted">seq ${esc(seg.a.data_seq)} · ${time(seg.a.created_at)}</span></span>
      <span><span class="tag b">B</span><span class="mono">revision ${esc(seg.b.revision)}</span><br><span class="muted">${time(seg.b.created_at)}</span></span>
      <span class="muted">${esc(seg.reason)}<br><span class="mono" style="font-size:10px">binding ${esc(short(seg.binding_digest, 16))}</span></span>
    </div>`).join('');
}

function renderEvents(run) {
  const lines = (run.events || []).slice().reverse().map((event) => {
    const body = event.body || {};
    let summary = '';
    if (event.kind === 'page_emitted') {
      summary = `page ${body.pageNo} · ${body.rows} 行${body.cache_hit ? ' · 缓存命中' : ''} · ${short(body.page_digest, 10)}`;
    } else if (event.kind === 'page_failed') {
      summary = `${body.failure.source} ${body.failure.code} @ page ${body.failure.pageNo}`;
    } else if (event.kind === 'bound' || event.kind === 'retry') {
      summary = JSON.stringify(body).slice(0, 120);
    } else if (event.kind === 'run_completed') {
      summary = `${body.emitted.pages} 页 / ${body.emitted.rows} 行`;
    } else {
      summary = body.message || body.reason || '';
    }
    const cls = event.kind.includes('fail') ? 'bad' : event.kind.includes('complete') ? 'ok' : event.kind.includes('cancel') ? 'warn' : '';
    return `<span class="dim">${time(event.at)}</span> <span class="${cls}">${esc(event.kind)}</span> ${esc(summary)}`;
  });
  $('#events').innerHTML = lines.length ? lines.join('\n') : '<span class="dim">尚无事件</span>';
}

async function loadRecent() {
  try {
    const payload = await api('/api/runs');
    const current = state.runId;
    const el = $('#recent-runs');
    if (!payload.runs.length) {
      el.innerHTML = '<p class="muted">还没有运行。</p>';
      return;
    }
    el.innerHTML = payload.runs.map((run) => `
      <button class="ghost run-item ${run.id === current ? 'active' : ''}" type="button" data-run="${esc(run.id)}">
        <span><span class="id">${esc(short(run.id, 18))}</span><br>
          <span class="muted">${runStateLabel(run.state)} · ${run.emitted.pages} 页</span></span>
        <span class="chip ${run.state === 'completed' ? 'cache' : run.state === 'failed' ? 'bad' : ''}">${esc(run.state)}</span>
      </button>`).join('');
    for (const button of $$('[data-run]', el)) {
      button.addEventListener('click', async () => {
        state.runId = button.dataset.run;
        state.verify = null;
        state.retryChoice = null;
        await loadRun(state.runId);
        await loadRecent();
      });
    }
  } catch (err) {
    toast(`运行列表失败：${err.message}`, 'bad');
  }
}

/* ---------------- wiring ---------------- */

function wire() {
  $('#new-run').addEventListener('submit', createRun);
  $('#step1').addEventListener('click', () => step(1));
  $('#step3').addEventListener('click', () => step(3));
  $('#stepall').addEventListener('click', () => step('all'));
  $('#cancel-run').addEventListener('click', cancelRun);
  $('#resume-run').addEventListener('click', resumeRun);
  $('#verify-run').addEventListener('click', verifyRun);
  $('#apply-retry').addEventListener('click', applyRetry);
  $('#refresh-all').addEventListener('click', async () => {
    await refreshSources();
    if (state.runId) {
      await loadRun(state.runId);
      await refreshGuidance();
    }
    await loadRecent();
  });
  for (const button of $$('[data-src-action]')) {
    button.addEventListener('click', () => {
      const action = button.dataset.srcAction;
      const confirmText = action === 'a:update'
        ? '更新 A 的一部分账户并生成新快照？'
        : '在 B 上提交一个新修订？';
      if (!window.confirm(confirmText)) return;
      sourceAction(action, action === 'a:update' ? { count: 17, reason: '页面更新' } : { count: 23, reason: '页面提交' });
    });
  }
  $('#a-apply-fault').addEventListener('click', () => applyFaults('a'));
  $('#b-apply-fault').addEventListener('click', () => applyFaults('b'));
  $('#a-clear-fault').addEventListener('click', () => clearFaults('a'));
  $('#b-clear-fault').addEventListener('click', () => clearFaults('b'));
  $('#purge-runs').addEventListener('click', async () => {
    if (!window.confirm('清空所有协调器运行、分页与缓存？来源数据不受影响。')) return;
    await api('/api/runs/purge', { method: 'POST' });
    state.runId = null;
    state.run = null;
    state.verify = null;
    renderEmpty();
    await loadRecent();
    toast('协调器状态已清空。', 'ok');
  });
  document.addEventListener('keydown', (event) => {
    if (event.target.matches('input, select, textarea')) return;
    if (event.key === 'n' && state.runId && !state.busy) step(1);
    if (event.key === 'a' && state.runId && !state.busy) step(3);
    if (event.key === 'v' && state.runId && !state.busy) verifyRun();
  });
}

async function boot() {
  wire();
  renderEmpty();
  await refreshSources();
  await loadRecent();
  // Restore the newest run so a page reload demonstrates persistence.
  try {
    const payload = await api('/api/runs');
    if (payload.runs.length) {
      state.runId = payload.runs[0].id;
      await loadRun(state.runId);
      await loadRecent();
    }
  } catch { /* empty state is fine */ }
  setInterval(() => { refreshSources().catch(() => {}); }, 5000);
}

boot();
