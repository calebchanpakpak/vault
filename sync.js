/* Vault V3.16 — 離線優先資料層（V3.16：forcePull 升版重拉；V3.12：設定期初結餘 openings；V3.11：轉帳多 toAcct；V3.3：記錄多咗 acct 帳戶，fingerprint 唔包帳戶）（IndexedDB 係部機嘅真相來源）
 *  - 所有新增／修改／刪除／設定：先寫 IndexedDB（transaction complete）先回覆，畫面即刻更新，唔使等網絡
 *  - 背景同步：single-flight、按次序清隊、指數退避、拉 getData 合併（唔會蓋過未同步嘅改動）
 *  - 改緊「同步緊」嘅記錄：開新 op 串住（client id 或者預計嘅 fingerprint），唔會阻用戶
 *  - 冇 DOM：網頁、service worker（Background Sync）、node 測試共用 */
(function (root) {
  'use strict';
  const OFFLINE_SRC = '離線估計（同步時由伺服器重新計）';
  const CFG_FNS = ['setBudget', 'addCategory', 'addCurrency', 'removeCurrency', 'addPayment', 'removePayment', 'setSetting'];
  const DEFAULTS = { pullGapMs: 15000, backoffBaseMs: 5000, backoffMaxMs: 300000, batch: 100, intervalMs: 600000 };
  const CAT_COLORS = ['#f97316', '#22c55e', '#3b82f6', '#8b5cf6', '#0ea5e9', '#ec4899', '#14b8a6', '#f59e0b', '#6366f1', '#a16207', '#ef4444', '#06b6d4', '#d946ef', '#16a34a', '#dc2626', '#0891b2'];
  const round2 = n => Math.round(Number(n) * 100) / 100;
  const fpOf = o => [o.date, o.item, String(o.cur || 'EUR').toUpperCase(), Number(o.orig).toFixed(2)].join('|');
  const fpObj = r => ({ date: r.date, item: r.item, cur: r.cur, orig: r.orig });
  const clone = o => JSON.parse(JSON.stringify(o));
  // 同伺服器 clean_ 一樣嘅正規化（用嚟預計「同步完之後」嘅 fingerprint）
  const normItem = e => String(e.item || '').trim().slice(0, 100) || e.cat;
  // V3.3：帳戶 A／B／C／D／F（同伺服器 normAcct_ 一樣）；空白／唔啱 = 'A'。舊記錄（冇 acct）一律當 A
  // V3.11：轉帳用資產帳戶 Personal／Retire／Year End／Investment；欄位 toAcct = 轉入
  const ACCTS = ['A', 'B', 'C', 'D', 'F'];   // V3.8：加 F
  const ASSET_ACCTS = ['Personal-bank', 'Personal-investment', 'Retirement-bank', 'Retirement-investment', 'Year End'];
  const normAcct = v => { const a = String(v == null ? '' : v).trim().toUpperCase(); return ACCTS.indexOf(a) >= 0 ? a : ''; };
  const normAsset = v => { const a = String(v == null ? '' : v).trim(); return ASSET_ACCTS.indexOf(a) >= 0 ? a : ''; };
  const XFER_FROM_EXTRA = ['Interest'];
  const normXferFrom = v => { const a = String(v == null ? '' : v).trim(); return ASSET_ACCTS.indexOf(a) >= 0 || XFER_FROM_EXTRA.indexOf(a) >= 0 ? a : ''; };
  const acctOf = r => {
    if (r && r.type === '轉帳') return normXferFrom(r.acct) || ASSET_ACCTS[0];
    return normAcct(r && r.acct) || 'A';
  };
  const toAcctOf = r => (r && r.type === '轉帳') ? (normAsset(r.toAcct) || '') : '';
  const bySeq = (a, b) => a.seq - b.seq;

  /** fetch → Apps Script（Api.gs doPost）。text/plain = 唔觸發 CORS preflight；自動跟 302 去 googleusercontent */
  function fetchApi(getCfg, opts) {
    opts = opts || {};
    return async function api(action, payload, timeoutMs) {
      const c = getCfg() || {};
      if (!c.url) { const e = new Error('未設定 Apps Script 網址'); e.config = true; throw e; }
      const f = opts.fetch || root.fetch.bind(root);
      const ctrl = new AbortController(), timer = setTimeout(() => ctrl.abort(), timeoutMs || (action === 'getData' ? 60000 : 30000));
      const body = JSON.stringify({ action: action, payload: payload || [], token: c.token || '' });
      let res, text;
      try {
        res = await f(c.url, { method: 'POST', redirect: 'follow', cache: 'no-store', signal: ctrl.signal,
          headers: { 'Content-Type': 'text/plain;charset=utf-8' }, body: body });
        text = await res.text();
      } catch (err) {
        const e = new Error('連唔到伺服器（離線、網絡慢或者部署存取權）'); e.network = true; throw e;
      } finally { clearTimeout(timer); }
      let j;
      try { j = JSON.parse(text); } catch (err) { const e = new Error('伺服器回覆唔係 JSON：檢查網址（/exec）同「誰可以存取 = 任何人」'); e.server = true; throw e; }
      if (!j.ok) { const e = new Error(j.error || '伺服器出錯'); e.server = true; throw e; }
      return j.result;
    };
  }

  function create(o) {
    const db = o.db, now = o.now || (() => Date.now());
    const opt = Object.assign({}, DEFAULTS, o.options || {});
    const listeners = [];
    if (o.onChange) listeners.push(o.onChange);
    let api = o.api;
    let server = null, rows = [], queue = [], failed = [], rates = {};
    let cfg = Object.assign({ url: '', token: '' }, o.defaults || {});
    let meta = { syncedAt: 0, lastPullAt: 0, failCount: 0, nextAttemptAt: 0, lastError: '', lastOkAt: 0 };
    let inflight = new Set(), running = null, rerun = null, state = 'idle', lastSeq = 0;
    let timers = null, intervalId = null, retryId = null, ver = 0;

    const emit = data => { if (data) ver++; const s = status(); listeners.forEach(fn => { try { fn(s); } catch (e) { /* UI 錯唔好影響同步 */ } }); };
    const uid = () => 'v' + now().toString(36) + Math.random().toString(36).slice(2, 10) + Math.random().toString(36).slice(2, 6);
    const nextSeq = () => (lastSeq = Math.max(now() * 1000, lastSeq + 1));
    const putMeta = (k, v) => db.put('meta', { k: k, v: v });
    const saveMeta = () => db.tx('meta', 'readwrite', s => { Object.keys(meta).forEach(k => s.meta.put({ k: k, v: meta[k] })); });
    const isInflight = op => inflight.has(op.id);

    /* ---------- 載入 + 由 V2.2 PWA localStorage 搬過嚟 ---------- */
    async function migrateFromLocalStorage(ls) {
      const done = await db.get('meta', 'migratedV22');
      if (done) return null;
      const j = k => { try { const v = ls && ls.getItem(k); return v ? JSON.parse(v) : null; } catch (e) { return null; } };
      const data = j('vault.data'), q = j('vault.queue') || [], f = j('vault.failed') || [], r = j('vault.rates') || {}, synced = j('vault.syncedAt') || 0;
      const url = ls && ls.getItem('vault.apiUrl'), token = ls && ls.getItem('vault.apiToken');
      const info = { at: now(), rows: data && data.rows ? data.rows.length : 0, ops: q.length, failed: f.length, cfg: !!url };
      await db.tx(['records', 'meta', 'queue', 'rates', 'failed'], 'readwrite', s => {
        if (data && Array.isArray(data.rows)) {
          const c = Object.assign({}, data); delete c.rows;
          s.meta.put({ k: 'config', v: c });
          data.rows.forEach(row => { if (typeof row.row === 'number') s.records.put(row); });
          s.meta.put({ k: 'syncedAt', v: synced }); s.meta.put({ k: 'lastPullAt', v: synced });
        }
        q.forEach((op, i) => {
          const n = { id: op.id, op: op.op, seq: (op.at || now()) * 1000 + i, at: op.at || now() };
          if (op.entry) n.entry = op.entry;
          if (op.op !== 'add') n.ref = op.cid ? { cid: op.cid } : { row: op.row, orig: op.orig };
          s.queue.put(n);
        });
        f.forEach(x => { if (x && x.op && x.op.id) s.failed.put({ id: x.op.id, op: x.op, error: x.error, at: x.at || now() }); });
        Object.keys(r).forEach(cur => s.rates.put(Object.assign({ cur: cur }, r[cur])));
        if (url) s.meta.put({ k: 'cfg', v: { url: url, token: token || '' } });
        s.meta.put({ k: 'migratedV22', v: info });
      });
      return info;
    }

    async function init(io) {
      io = io || {};
      const migrated = io.localStorage ? await migrateFromLocalStorage(io.localStorage) : null;
      const [recs, metas, q, f, rs] = await Promise.all([db.getAll('records'), db.getAll('meta'), db.getAll('queue'), db.getAll('failed'), db.getAll('rates')]);
      rows = recs.sort((a, b) => a.row - b.row);
      const m = {}; metas.forEach(x => { m[x.k] = x.v; });
      server = m.config || null;
      if (m.cfg && m.cfg.url) cfg = Object.assign({ url: '', token: '' }, m.cfg);
      Object.keys(meta).forEach(k => { if (m[k] !== undefined) meta[k] = m[k]; });
      queue = q.sort(bySeq); lastSeq = queue.reduce((a, b) => Math.max(a, b.seq), 0);
      failed = f.sort((a, b) => a.at - b.at);
      rates = {}; rs.forEach(x => { rates[x.cur] = x; });
      if (!api) api = fetchApi(() => cfg);
      emit(true);
      return { migrated: migrated };
    }

    /* ---------- 畫面用：伺服器快取 + 排緊隊嘅改動 ---------- */
    function rateGuess(cur) {
      if (cur === 'HKD') return 1;
      if (rates[cur] && rates[cur].rate > 0) return rates[cur].rate;
      let best = null; rows.forEach(r => { if (r.cur === cur && r.rate > 0 && !r.est && (!best || r.row > best.row)) best = r; });
      if (best) return best.rate;
      const ci = (server && server.curInfo || []).find(c => c.code === cur);
      return ci && ci.est > 0 ? ci.est : 0;
    }
    function pendingAddRow(op) {
      const e = op.entry, g = rateGuess(e.cur);
      const type = e.type || '支出';
      return { row: 'p:' + op.id, cid: op.id, pending: true, _ops: [op.id], date: e.date, item: normItem(e), cat: type === '轉帳' ? '轉帳' : e.cat, pay: e.pay || '', note: e.note || '',
        acct: type === '轉帳' ? (normXferFrom(e.acct) || ASSET_ACCTS[0]) : acctOf(e),
        toAcct: type === '轉帳' ? (normAsset(e.toAcct) || '') : '',
        cur: String(e.cur).toUpperCase(), orig: round2(e.amount), type: type, rate: g, amount: e.cur === 'HKD' ? round2(e.amount) : round2(e.amount * g),
        src: e.cur === 'HKD' ? 'HKD' : OFFLINE_SRC, est: e.cur !== 'HKD', eurAmt: 0 };
    }
    function applyEdit(base, op) {
      const e = op.entry, cur = String(e.cur).toUpperCase(), same = base.cur === cur && base.rate > 0;
      const rate = same ? base.rate : rateGuess(cur);
      const type = e.type || '支出';
      const acct = type === '轉帳' ? (normAsset(e.acct) || acctOf(base)) : (normAcct(e.acct) || acctOf(base));
      const toAcct = type === '轉帳' ? (normAsset(e.toAcct) || toAcctOf(base) || '') : '';
      return Object.assign({}, base, { pending: true, date: e.date, item: normItem(e), cat: type === '轉帳' ? '轉帳' : e.cat, pay: e.pay || '', note: e.note || '', acct: acct, toAcct: toAcct,
        cur: cur, orig: round2(e.amount), type: type, rate: rate, amount: round2(e.amount * rate),
        est: same ? base.est : cur !== 'HKD', src: same ? base.src : OFFLINE_SRC });
    }
    function locate(list, ref) {
      if (ref.cid) return list.findIndex(r => r.cid === ref.cid);
      const f = fpOf(ref.orig || {});
      let i = list.findIndex(r => r.row === ref.row && fpOf(r) === f);
      if (i < 0) i = list.findIndex(r => typeof r.row === 'number' && fpOf(r) === f);
      return i;
    }
    function viewRows() {
      const out = rows.map(r => Object.assign({}, r, { _ops: [] }));
      queue.forEach(op => {
        if (op.op === 'add') { out.push(pendingAddRow(op)); return; }
        if (op.op !== 'update' && op.op !== 'delete') return;
        const i = locate(out, op.ref || {});
        if (i < 0) return;
        if (op.op === 'delete') out.splice(i, 1);
        else { const r = applyEdit(out[i], op); r._ops = out[i]._ops.concat(op.id); out[i] = r; }
      });
      return out;
    }
    /** 設定（類別、貨幣、付款方式、預算、預設值）= 伺服器 config + 排緊隊嘅設定改動 */
    function viewConfig() {
      const c = clone(server || { budget: 0, currencies: ['HKD'], curInfo: [], categories: [], payments: [], payInfo: [], settings: {}, today: '' });
      c.categories = c.categories || []; c.curInfo = c.curInfo || []; c.payInfo = c.payInfo || []; c.settings = c.settings || {};
      queue.forEach(op => {
        if (op.op !== 'cfg') return;
        const a = op.args || [];
        switch (op.fn) {
          case 'setBudget': c.budget = Number(a[0]) || c.budget; break;
          case 'addCategory': if (!c.categories.some(x => x.type === a[0].type && x.name === a[0].name))
            c.categories.push({ type: a[0].type, name: a[0].name, en: a[0].en || '', icon: a[0].icon || (a[0].type === '收入' ? '💰' : '📦'), color: CAT_COLORS[c.categories.length % CAT_COLORS.length], custom: true, pending: true }); break;
          case 'addCurrency': { const code = String(a[0].code).toUpperCase();
            if (!c.curInfo.some(x => x.code === code)) c.curInfo.push({ code: code, name: a[0].name || '', sym: a[0].sym || code + ' ', dec: Number(a[0].dec) === 0 ? 0 : 2, est: Number(a[0].est) || 0, pending: true }); break; }
          case 'removeCurrency': c.curInfo = c.curInfo.filter(x => x.code !== a[0]); break;
          case 'addPayment': if (!c.payInfo.some(x => x.name === a[0].name)) c.payInfo.push({ name: a[0].name, en: a[0].en || '', pending: true }); break;
          case 'removePayment': c.payInfo = c.payInfo.filter(x => x.name !== a[0]); break;
          case 'setSetting':
            if (a[0] === '預設貨幣') c.settings.defaultCur = a[1];
            else if (a[0] === '預設付款方式') c.settings.defaultPay = a[1];
            else if (a[0] === '期初日期') {
              c.settings.openings = c.settings.openings || { date: '', amounts: {}, adjustments: {} };
              c.settings.openings.date = a[1];
            } else if (String(a[0]).indexOf('期初結餘_') === 0) {
              c.settings.openings = c.settings.openings || { date: '2024-10-31', amounts: {}, adjustments: {} };
              c.settings.openings.amounts = c.settings.openings.amounts || {};
              const acct = String(a[0]).slice('期初結餘_'.length);
              c.settings.openings.amounts[acct] = Number(a[1]) || 0;
            } else if (String(a[0]).indexOf('調整日期_') === 0) {
              const acct = String(a[0]).slice('調整日期_'.length);
              c.settings.openings = c.settings.openings || { date: '2024-10-31', amounts: {}, adjustments: {} };
              c.settings.openings.adjustments = c.settings.openings.adjustments || {};
              const cur = c.settings.openings.adjustments[acct] || { amount: 0, date: a[1], label: '' };
              cur.date = a[1];
              c.settings.openings.adjustments[acct] = cur;
            } else if (String(a[0]).indexOf('調整_') === 0) {
              const acct = String(a[0]).slice('調整_'.length);
              c.settings.openings = c.settings.openings || { date: '2024-10-31', amounts: {}, adjustments: {} };
              c.settings.openings.adjustments = c.settings.openings.adjustments || {};
              const cur = c.settings.openings.adjustments[acct] || { amount: 0, date: '', label: '' };
              cur.amount = Number(a[1]) || 0;
              c.settings.openings.adjustments[acct] = cur;
            }
            break;
        }
      });
      c.currencies = c.curInfo.length ? c.curInfo.map(x => x.code) : (c.currencies || ['HKD']);
      c.payments = c.payInfo.length ? c.payInfo.map(x => x.name) : (c.payments || []);
      return c;
    }

    /* ---------- 寫入：先落 IndexedDB（complete）先 resolve ---------- */
    function liveOps(vr) { return (vr && vr._ops || []).map(id => queue.find(o => o.id === id)).filter(Boolean); }

    function enqueueAdd(entry) {
      const op = { id: uid(), seq: nextSeq(), op: 'add', entry: clone(entry), at: now() };
      queue.push(op);
      return db.put('queue', op).then(() => { emit(true); return op; });
    }
    function enqueueEdit(vr, entry) {
      const ops = liveOps(vr), last = ops[ops.length - 1];
      let op;
      if (last && !isInflight(last) && (last.op === 'add' || last.op === 'update')) {
        last.entry = clone(entry); op = last;                // 未送出：直接改排緊隊嗰個 op
      } else {
        // 冇排隊，或者上一個 op 送緊：開新 op 串住（PWA 加嘅用 client id；舊記錄用「同步後」嘅 fingerprint）
        const ref = vr.cid ? { cid: vr.cid } : { row: vr.row, orig: fpObj(vr) };
        if (!vr.cid && typeof vr.row !== 'number') return Promise.reject(new Error('搵唔到呢筆記錄'));
        op = { id: uid(), seq: nextSeq(), op: 'update', ref: ref, entry: clone(entry), at: now() };
        queue.push(op);
      }
      return db.put('queue', op).then(() => { emit(true); return op; });
    }
    function enqueueDelete(vr) {
      const ops = liveOps(vr), idle = ops.filter(x => !isInflight(x)), busy = ops.filter(isInflight);
      const idleAdd = idle.find(x => x.op === 'add'), idleUpd = idle.find(x => x.op === 'update');
      const drop = new Set(idle.map(x => x.id));
      queue = queue.filter(x => !drop.has(x.id));
      let op = null;
      if (!(idleAdd && !busy.length)) {                       // 未送過嘅新增：本機刪咗就算，唔使話伺服器
        let ref;
        if (vr.cid) ref = { cid: vr.cid };
        else ref = { row: idleUpd ? idleUpd.ref.row : vr.row, orig: idleUpd ? idleUpd.ref.orig : fpObj(vr) };
        op = { id: uid(), seq: nextSeq(), op: 'delete', ref: ref, at: now() };
        queue.push(op);
      }
      return db.tx('queue', 'readwrite', s => { drop.forEach(id => s.queue.delete(id)); if (op) s.queue.put(op); }).then(() => { emit(true); return op; });
    }
    function enqueueCfg(fn, args) {
      if (CFG_FNS.indexOf(fn) < 0) return Promise.reject(new Error('唔支援：' + fn));
      const op = { id: uid(), seq: nextSeq(), op: 'cfg', fn: fn, args: clone(args || []), at: now() };
      queue.push(op);
      return db.put('queue', op).then(() => { emit(true); return op; });
    }

    /* ---------- 伺服器結果 / 拉資料 ---------- */
    function persistRows(s) { s.records.clear(); rows.forEach(r => { const c = Object.assign({}, r); delete c._ops; s.records.put(c); }); }
    function setServer(d) {
      const c = Object.assign({}, d); delete c.rows;
      // getData 唔會回 client id：按 行號+fingerprint（或者唯一 fingerprint）抄返落新資料，令串住 cid 嘅 op 搵得返
      const byKey = {}, byFp = {}, fpCount = {};
      rows.forEach(r => { if (r.cid) { byKey[r.row + '#' + fpOf(r)] = r.cid; byFp[fpOf(r)] = r.cid; } });
      (d.rows || []).forEach(r => { const f = fpOf(r); fpCount[f] = (fpCount[f] || 0) + 1; });
      rows = (d.rows || []).map(r => { const f = fpOf(r), cid = byKey[r.row + '#' + f] || (fpCount[f] === 1 ? byFp[f] : null); return cid ? Object.assign({}, r, { cid: cid }) : r; });
      server = c; meta.syncedAt = meta.lastPullAt = now();
      return db.tx(['records', 'meta'], 'readwrite', s => {
        persistRows(s); s.meta.put({ k: 'config', v: c }); s.meta.put({ k: 'syncedAt', v: meta.syncedAt }); s.meta.put({ k: 'lastPullAt', v: meta.lastPullAt });
      }).then(() => emit(true));
    }
    function applyResults(results, batch, sum) {
      const byId = {}; (results || []).forEach(r => { byId[r.id] = r; });
      const done = [], newFailed = []; let rowsChanged = false;
      batch.forEach(op => {
        const r = byId[op.id]; if (!r) return;
        done.push(op.id);
        if (!r.ok) { const f = { id: op.id, op: op, error: r.error, at: now() }; failed.push(f); newFailed.push(f); sum.failed.push(f); return; }
        sum.ok++;
        if (r.skipped) { sum.skipped++; return; }
        rowsChanged = true;
        if (op.op === 'add' && r.entry) rows.push(Object.assign({}, r.entry, { cid: op.id }));
        else if (op.op === 'update' && r.entry) {
          const i = rows.findIndex(x => x.row === r.entry.row);
          if (i >= 0) rows[i] = Object.assign({}, rows[i], r.entry); else rows.push(r.entry);
        } else if (op.op === 'delete' && r.deletedAt) {
          const d = r.deletedAt;
          rows = rows.filter(x => x.row !== d); rows.forEach(x => { if (x.row > d) x.row--; });
          queue.forEach(q => { if (q.ref && typeof q.ref.row === 'number' && q.ref.row > d && done.indexOf(q.id) < 0) q.ref.row--; });
        }
      });
      const doneSet = new Set(done);
      queue = queue.filter(x => !doneSet.has(x.id));
      return db.tx(['queue', 'failed', 'records'], 'readwrite', s => {
        done.forEach(id => s.queue.delete(id));
        queue.forEach(q => s.queue.put(q));                    // 行號可能減咗 1
        newFailed.forEach(f => s.failed.put(f));
        if (rowsChanged) persistRows(s);
      });
    }

    const toWire = op => {
      const w = { id: op.id, op: op.op };
      if (op.entry) w.entry = op.entry;
      if (op.ref) { if (op.ref.cid) w.cid = op.ref.cid; else { w.row = op.ref.row; w.orig = op.ref.orig; } }
      return clone(w);
    };

    async function flushAll(sum) {
      for (let round = 0; round < 200 && queue.length; round++) {
        const first = queue[0];
        if (first.op === 'cfg') {
          inflight.add(first.id);
          try {
            let res = null, err = null;
            try { res = await api(first.fn, clone(first.args || [])); } catch (e) { if (!e.server) throw e; err = e; }
            if (err && !/已經有/.test(err.message)) { const f = { id: first.id, op: first, error: err.message, at: now() }; failed.push(f); sum.failed.push(f); }
            else sum.ok++;
            queue = queue.filter(x => x.id !== first.id);
            await db.tx(['queue', 'failed'], 'readwrite', s => { s.queue.delete(first.id); if (err && !/已經有/.test(err.message)) s.failed.put(failed[failed.length - 1]); }); emit(true);
            if (res && Array.isArray(res.rows)) { await setServer(res); sum.pulled = true; }
          } finally { inflight.delete(first.id); }
          sum.sent++;
          continue;
        }
        const batch = [];
        for (const op of queue) { if (op.op === 'cfg' || batch.length >= opt.batch) break; batch.push(op); }
        batch.forEach(op => inflight.add(op.id));               // 同步標記：之後改呢啲記錄會開新 op 串住
        const wire = batch.map(toWire);
        emit();
        try {
          const out = await api('sync', [wire]);
          await applyResults(out && out.results, batch, sum); emit(true);
          sum.sent += batch.length;
        } finally { batch.forEach(op => inflight.delete(op.id)); }
        emit();
      }
    }

    /** 攞到 lock 之後：以 IndexedDB 為準對返隊（service worker / 其他分頁可能已經送咗或者加咗） */
    async function reconcile() {
      const known = new Set(queue.map(x => x.id));            // 只會處理「讀之前已經喺記憶體」嘅 op
      const stored = await db.getAll('queue');
      const ids = new Set(stored.map(x => x.id));
      const before = queue.length;
      queue = queue.filter(x => !known.has(x.id) || ids.has(x.id));
      const dropped = before - queue.length;
      const mine = new Set(queue.map(x => x.id));
      let added = 0; stored.forEach(x => { if (!mine.has(x.id) && !known.has(x.id)) { queue.push(x); added++; } });
      if (added) queue.sort(bySeq);
      if (dropped || added) emit(true);
      return dropped;
    }
    function lockWrap(fn) {
      const L = root.navigator && root.navigator.locks;
      return L && L.request ? L.request('vault-v3-sync', fn) : fn();   // 多個分頁／service worker 唔會同時同步
    }

    /**
     * 同步一次（single-flight）：清隊 → 拉 getData → 再清期間新加嘅。
     * trigger：open / online / visible / timer / retry / write / manual / bg
     * 'write' 只送出唔拉（除非有 skipped）；其他會拉（15 秒內拉過就唔再拉，除非 manual）。
     */
    function syncAll(opts) {
      opts = opts || {};
      const trigger = opts.trigger || 'manual', manual = !!opts.manual || trigger === 'manual';
      if (!cfg.url) return Promise.resolve({ skipped: 'nocfg' });
      if (running) { if (!rerun || manual || trigger !== 'write') rerun = { trigger: trigger, manual: manual }; return running; }
      if (!manual && now() < meta.nextAttemptAt) return Promise.resolve({ skipped: 'backoff', retryAt: meta.nextAttemptAt });
      const sum = { trigger: trigger, sent: 0, ok: 0, skipped: 0, failed: [], pulled: false };
      running = (async () => {
        state = 'syncing'; emit();
        try {
          await lockWrap(async () => {
            if (await reconcile()) sum.skipped++;               // 其他分頁／service worker 送咗 → 要拉返最新
            await flushAll(sum);
            const wantPull = trigger !== 'write' || sum.skipped > 0 || !server;
            if (wantPull && !sum.pulled && (manual || !server || sum.skipped > 0 || now() - meta.lastPullAt > opt.pullGapMs)) {
              await setServer(await api('getData', [])); sum.pulled = true;
            }
            if (queue.length) await flushAll(sum);               // 拉資料期間用戶加嘅
          });
          meta.failCount = 0; meta.nextAttemptAt = 0; meta.lastError = ''; meta.lastOkAt = now(); state = 'idle';
        } catch (err) {
          meta.failCount++;
          meta.nextAttemptAt = now() + Math.min(opt.backoffMaxMs, opt.backoffBaseMs * Math.pow(2, meta.failCount - 1));
          meta.lastError = String(err && err.message || err);
          state = err && err.network ? 'offline' : 'error';
          sum.error = meta.lastError; sum.network = !!(err && err.network);
        }
        saveMeta().catch(() => {});
        return sum;
      })();
      const p = running;
      p.then(() => {
        running = null; emit();
        const r = rerun; rerun = null;
        if (r && (queue.length || r.manual || r.trigger !== 'write')) syncAll(r);
        else scheduleRetry();
      });
      return p;
    }

    function scheduleRetry() {
      if (!timers) return;
      if (retryId) { timers.clearTimeout(retryId); retryId = null; }
      if (meta.nextAttemptAt > now() && cfg.url) retryId = timers.setTimeout(() => { retryId = null; syncAll({ trigger: 'retry' }); }, meta.nextAttemptAt - now() + 50);
    }
    /** 開 App 期間：每 intervalMs（預設 10 分鐘）同步一次；失敗就按退避時間自動再試 */
    function startAuto(a) {
      a = a || {};
      timers = a.timers || { setTimeout: (f, ms) => setTimeout(f, ms), clearTimeout: id => clearTimeout(id), setInterval: (f, ms) => setInterval(f, ms), clearInterval: id => clearInterval(id) };
      if (intervalId) timers.clearInterval(intervalId);
      intervalId = timers.setInterval(() => syncAll({ trigger: 'timer' }), a.intervalMs || opt.intervalMs);
      scheduleRetry();
    }
    function stopAuto() { if (timers) { if (intervalId) timers.clearInterval(intervalId); if (retryId) timers.clearTimeout(retryId); } intervalId = retryId = null; }
    function resetBackoff() { meta.failCount = 0; meta.nextAttemptAt = 0; }

    function status() {
      return { state: running ? 'syncing' : state, pending: queue.length, inflight: inflight.size, failed: failed.length,
        syncedAt: meta.syncedAt, lastOkAt: meta.lastOkAt, lastError: meta.lastError, nextAttemptAt: meta.nextAttemptAt, failCount: meta.failCount, hasData: !!server, configured: !!cfg.url };
    }

    return {
      init, viewRows, viewConfig, rateGuess, enqueueAdd, enqueueEdit, enqueueDelete, enqueueCfg, syncAll, startAuto, stopAuto, resetBackoff, status,
      onChange: fn => listeners.push(fn),
      saveRate(cur, r) { if (r && r.rate > 0 && !r.est) { rates[cur] = { cur: cur, rate: r.rate, date: r.date || '', src: r.src || '', at: now() }; return db.put('rates', rates[cur]); } return Promise.resolve(); },
      cachedRate: cur => rates[cur] || null,
      setCfg(url, token) { cfg = { url: String(url || '').trim(), token: String(token || '').trim() }; resetBackoff(); return putMeta('cfg', cfg).then(emit); },
      forcePull() { meta.lastPullAt = 0; return putMeta('lastPullAt', 0).then(() => syncAll({ trigger: 'manual', pull: true })); },
      clearFailed() { failed = []; return db.clear('failed').then(() => emit(true)); },
      get cfg() { return cfg; }, get ver() { return ver; }, get server() { return server; }, get queue() { return queue; }, get failed() { return failed; }, get rows() { return rows; },
      get api() { return api; }, OFFLINE_SRC
    };
  }

  const VaultSync = { create, fetchApi, fpOf, acctOf, toAcctOf, ACCTS, ASSET_ACCTS, CFG_FNS, OFFLINE_SRC };
  if (typeof module !== 'undefined' && module.exports) module.exports = VaultSync; else root.VaultSync = VaultSync;
})(typeof self !== 'undefined' ? self : globalThis);
