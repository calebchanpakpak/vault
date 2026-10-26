/* Vault V3 — 細細個 IndexedDB 包裝（手寫，冇外部 library）
 * 重點：tx() 係「同步」開 transaction（喺第一個 await 之前），所以喺同一個 tick 改記憶體 + 開 tx，
 * 之後開嘅 transaction 一定睇到之前嘅寫入（IndexedDB 按開 transaction 嘅次序做）。 */
(function (root) {
  'use strict';
  const DB_NAME = 'vault-v3', DB_VERSION = 1;
  const STORES = {
    records: { keyPath: 'row' },   // 伺服器記錄（上次 getData + 已同步結果）
    meta:    { keyPath: 'k' },     // config（類別、貨幣、付款方式、設定、預算）、連線設定、同步資料
    queue:   { keyPath: 'id' },    // 等緊同步嘅 op（client id 做 key，seq 排次序）
    rates:   { keyPath: 'cur' },   // 上次攞到嘅匯率
    failed:  { keyPath: 'id' }     // 伺服器做唔到嘅 op
  };
  const req2p = r => new Promise((res, rej) => { r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); });

  function open(opts) {
    opts = opts || {};
    const idb = opts.indexedDB || root.indexedDB;
    return new Promise((res, rej) => {
      const r = idb.open(opts.name || DB_NAME, DB_VERSION);
      r.onupgradeneeded = () => {
        const db = r.result;
        Object.keys(STORES).forEach(n => { if (!db.objectStoreNames.contains(n)) db.createObjectStore(n, STORES[n]); });
      };
      r.onsuccess = () => res(wrap(r.result));
      r.onerror = () => rej(r.error);
      r.onblocked = () => rej(new Error('IndexedDB blocked（關咗其他 Vault 分頁再開）'));
    });
  }

  function wrap(db) {
    db.onversionchange = () => db.close();
    /** fn(stores) 同步行；回傳 Promise，等 transaction complete 先 resolve（= 已經寫落磁碟） */
    function tx(names, mode, fn) {
      names = [].concat(names);
      const t = db.transaction(names, mode);
      const s = {}; names.forEach(n => { s[n] = t.objectStore(n); });
      let out;
      try { out = fn(s); } catch (e) { try { t.abort(); } catch (x) {} return Promise.reject(e); }
      return new Promise((res, rej) => {
        t.oncomplete = () => Promise.resolve(out).then(res, rej);
        t.onerror = () => rej(t.error);
        t.onabort = () => rej(t.error || new Error('transaction aborted'));
      });
    }
    return {
      tx,
      req: req2p,
      get: (n, k) => { let r; return tx(n, 'readonly', s => { r = s[n].get(k); }).then(() => r.result); },
      getAll: (n) => { let r; return tx(n, 'readonly', s => { r = s[n].getAll(); }).then(() => r.result || []); },
      put: (n, v) => tx(n, 'readwrite', s => { s[n].put(v); }),
      del: (n, k) => tx(n, 'readwrite', s => { s[n].delete(k); }),
      clear: (n) => tx(n, 'readwrite', s => { s[n].clear(); }),
      close: () => db.close()
    };
  }

  const VaultIDB = { open, STORES, DB_NAME };
  if (typeof module !== 'undefined' && module.exports) module.exports = VaultIDB; else root.VaultIDB = VaultIDB;
})(typeof self !== 'undefined' ? self : globalThis);
