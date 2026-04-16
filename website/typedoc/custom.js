try {
  localStorage.setItem('tsd-theme', 'light');
  document.documentElement.dataset.theme = 'light';
} catch {
  document.documentElement.dataset.theme = 'light';
}
