(() => {
  const key = 'nemo.revenue.viewport';
  const paths = new Set(['/tools/revenue', '/tools/revenue/yearly']);
  let pending = null;
  let active = null;
  let leaving = false;
  let sequence = 0;

  function save(url) {
    try {
      sessionStorage.setItem(key, JSON.stringify({
        target: url.pathname + url.search, x: scrollX, y: scrollY, time: Date.now()
      }));
    } catch { /* Scroll preservation still works within this page without storage. */ }
  }

  try {
    const saved = JSON.parse(sessionStorage.getItem(key) || 'null');
    if (saved && saved.target === location.pathname + location.search &&
        Date.now() - saved.time < 120000 && Number.isFinite(saved.y)) {
      pending = saved;
    }
    sessionStorage.removeItem(key);
  } catch { /* Ignore unavailable storage or an obsolete saved position. */ }

  document.addEventListener('click', event => {
    const link = event.target.closest('.revenue-tabs a, #yearly-rows a');
    if (!link || event.defaultPrevented || event.button !== 0 ||
        event.metaKey || event.ctrlKey || event.shiftKey || event.altKey ||
        link.target === '_blank') return;
    const url = new URL(link.href, location.href);
    if (url.origin !== location.origin || !paths.has(url.pathname)) return;
    leaving = true;
    save(url);
  });
  window.addEventListener('pagehide', () => {
    if (!leaving) save(new URL(location.href));
  });
  window.addEventListener('pageshow', () => { leaving = false; });

  window.RevenueViewport = {
    begin() {
      const token = ++sequence;
      const previous = active;
      active = {
        token,
        position: pending || previous?.position || {x: scrollX, y: scrollY},
        minHeight: previous?.minHeight ?? document.body.style.minHeight,
        anchor: previous?.anchor ?? document.documentElement.style.overflowAnchor
      };
      pending = null;
      // Hiding a report must not shorten the page and clamp the browser's scroll position.
      document.body.style.minHeight = `${Math.max(document.body.scrollHeight, active.position.y + innerHeight)}px`;
      document.documentElement.style.overflowAnchor = 'none';
      window.scrollTo({left: active.position.x, top: active.position.y, behavior: 'instant'});
      return token;
    },
    async finish(token) {
      await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
      if (active?.token !== token) return;
      const {position, minHeight, anchor} = active;
      document.body.style.minHeight = minHeight;
      window.scrollTo({left: position.x, top: position.y, behavior: 'instant'});
      document.documentElement.style.overflowAnchor = anchor;
      active = null;
    }
  };
})();
