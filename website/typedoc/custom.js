try {
  localStorage.setItem('tsd-theme', 'light');
  document.documentElement.dataset.theme = 'light';
} catch {
  document.documentElement.dataset.theme = 'light';
}

const pathname = window.location.pathname;
const generatedApiIndex = pathname.indexOf('/generated-api/');
const siteBase =
  generatedApiIndex >= 0 ? pathname.slice(0, generatedApiIndex) : '';
const toolbarLinks = document.getElementById('tsd-toolbar-links');

if (toolbarLinks && !toolbarLinks.dataset.pondEnhanced) {
  const links = [
    { label: 'Pond', href: `${siteBase}/` },
    { label: 'Docs', href: `${siteBase}/docs/getting-started` },
    { label: 'Examples', href: `${siteBase}/docs/examples/cpu-metrics` },
    { label: 'GitHub', href: 'https://github.com/pjm17971/pond-ts' },
  ];

  for (const link of links) {
    const anchor = document.createElement('a');
    anchor.className = 'pond-toolbar-link';
    anchor.href = link.href;
    anchor.textContent = link.label;
    if (link.href.startsWith('https://')) {
      anchor.target = '_blank';
      anchor.rel = 'noreferrer';
    }
    toolbarLinks.append(anchor);
  }

  toolbarLinks.dataset.pondEnhanced = 'true';
}
