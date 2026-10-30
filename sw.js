/* Vault V3.5 service worker
 * - 安裝：一次過預先快取成個 App 外殼（index.html + idb.js + sync.js + icons + manifest），保證新舊唔會夾雜
 * - 開 App：外殼一律 cache-first（唔等網絡，飛航模式／網絡好慢都即開）；資料由 IndexedDB 出
 * - 更新：改咗任何外殼檔案就要改 CACHE 名（例如版本號） → 瀏覽器發現 sw.js 唔同 → 背景裝新版 → App 頂部問「更新」
 * - 唔同網域（script.google.com / googleusercontent API）：完全唔攔、唔快取
 * - Background Sync（只係錦上添花）：Chrome／Android 喺 App 關咗之後都可以幫手送出排緊隊嘅改動。
 *   iOS Safari／主畫面 App 冇 Background Sync 亦冇 Periodic Sync：App 關咗就唔會同步，下次打開先補送。
 */
const CACHE = 'vault-v3.5-shell';
const SHELL = ['./', './index.html', './idb.js', './sync.js', './manifest.json',
  './icons/icon-192.png', './icons/icon-512.png', './icons/apple-touch-icon.png'];

importScripts('idb.js', 'sync.js');

self.addEventListener('install', event => {
  event.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL.map(u => new Request(u, { cache: 'reload' })))));
  // 唔自動 skipWaiting：等頁面撳「更新」先換（免得打緊字突然 reload）；第一次安裝冇舊版，會即刻生效
});

self.addEventListener('message', event => {
  if (event.data && event.data.type === 'SKIP_WAITING') self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k.startsWith('vault-') && k !== CACHE).map(k => caches.delete(k))))   // 包括 V2.2 嘅 vault-v2.2-shell-*
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', event => {
  const req = event.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;           // API（Apps Script）同其他網域：唔攔、唔快取
  const isPage = req.mode === 'navigate' || url.pathname.endsWith('/index.html') || url.pathname === new URL('./', self.location).pathname;
  const key = isPage ? './index.html' : req;
  event.respondWith(
    caches.open(CACHE).then(c => c.match(key, { ignoreSearch: true })).then(hit => hit || fetch(req).then(res => {
      if (res.ok && res.type === 'basic' && !isPage) { const copy = res.clone(); caches.open(CACHE).then(c => c.put(req, copy)); }
      return res;
    }).catch(() => caches.match('./index.html')))
  );
});

/* Background Sync：頁面每次寫入後會 register('vault-sync')；有網時瀏覽器叫醒呢度送出。
   同頁面共用 IndexedDB + navigator.locks('vault-v3-sync')，唔會同時送；伺服器用 client id / op id 防重複。 */
self.addEventListener('sync', event => {
  if (event.tag !== 'vault-sync') return;
  event.waitUntil((async () => {
    const db = await VaultIDB.open();
    try {
      const eng = VaultSync.create({ db: db });
      await eng.init();
      if (!eng.queue.length || !eng.cfg.url) return;
      const r = await eng.syncAll({ trigger: 'write', manual: true });
      if (r.error) throw new Error(r.error);                  // 拋出 = 叫瀏覽器遲啲再試
      const cs = await self.clients.matchAll({ type: 'window' });
      cs.forEach(c => c.postMessage({ type: 'VAULT_BG_SYNCED', sent: r.sent }));
    } finally { db.close(); }
  })());
});
