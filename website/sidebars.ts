import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    {
      type: 'category',
      label: 'Start here',
      items: ['start-here/intro', 'start-here/getting-started'],
    },
    {
      type: 'category',
      label: 'pond-ts (core)',
      link: { type: 'doc', id: 'pond-ts/pond-ts-index' },
      items: [
        {
          type: 'category',
          label: 'Concepts',
          items: ['pond-ts/concepts/overview'],
        },
        {
          type: 'category',
          label: 'Transforms',
          items: [
            'pond-ts/transforms/alignment-and-aggregation',
            'pond-ts/transforms/aggregation-playbook',
            'pond-ts/transforms/rolling-and-smoothing',
            'pond-ts/transforms/reducer-reference',
          ],
        },
        {
          type: 'category',
          label: 'Data shapes',
          items: [
            'pond-ts/data-shapes/arrays',
            'pond-ts/data-shapes/json-ingest-and-timezones',
          ],
        },
        {
          type: 'category',
          label: 'Live',
          items: ['pond-ts/live/live-series', 'pond-ts/live/live-transforms'],
        },
        {
          type: 'link',
          label: 'API reference (core)',
          href: 'pathname:///generated-api/core/',
        },
      ],
    },
    {
      type: 'category',
      label: '@pond-ts/react',
      link: { type: 'doc', id: 'react/react-index' },
      items: [
        {
          type: 'link',
          label: 'API reference (react)',
          href: 'pathname:///generated-api/react/',
        },
      ],
    },
    {
      type: 'category',
      label: 'Recipes',
      link: { type: 'doc', id: 'recipes/recipes-index' },
      items: [
        'recipes/cpu-metrics',
        'recipes/error-rate-dashboard',
        'recipes/streaming-dashboard',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      link: { type: 'doc', id: 'reference/reference-index' },
      items: ['reference/benchmarks'],
    },
  ],
};

export default sidebars;
