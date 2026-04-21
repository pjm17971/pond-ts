import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'intro',
    'getting-started',
    'core-concepts',
    'api-reference',
    {
      type: 'category',
      label: 'Guides',
      items: [
        'guides/alignment-and-aggregation',
        'guides/aggregation-playbook',
        'guides/rolling-and-smoothing',
        'guides/json-ingest-and-timezones',
        'guides/live-series',
        'guides/live-transforms',
      ],
    },
    {
      type: 'category',
      label: 'Examples',
      items: ['examples/cpu-metrics'],
    },
  ],
};

export default sidebars;
