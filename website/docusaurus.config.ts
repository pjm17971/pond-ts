import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Pond',
  tagline: 'Typed time series primitives for modern TypeScript projects',
  favicon: 'img/favicon.ico',
  future: {
    v4: true,
  },
  url: 'https://pjm17971.github.io',
  baseUrl: '/pond-ts/',
  organizationName: 'pjm17971',
  projectName: 'pond-ts',
  onBrokenLinks: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },
  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/pjm17971/pond-ts/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],
  themeConfig: {
    image: 'img/docusaurus-social-card.jpg',
    colorMode: {
      defaultMode: 'light',
      disableSwitch: true,
      respectPrefersColorScheme: false,
    },
    navbar: {
      title: 'Pond',
      logo: {
        alt: 'Pond logo',
        src: 'img/logo.png',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          to: '/api',
          label: 'API',
          position: 'left',
        },
        {
          href: 'https://www.npmjs.com/package/pond-ts',
          label: 'npm',
          position: 'right',
        },
        {
          href: 'https://github.com/pjm17971/pond-ts',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Getting Started',
              to: '/docs/getting-started',
            },
            {
              label: 'Core Concepts',
              to: '/docs/core-concepts',
            },
            {
              label: 'API Reference',
              to: '/docs/api-reference',
            },
          ],
        },
        {
          title: 'Project',
          items: [
            {
              label: 'npm',
              href: 'https://www.npmjs.com/package/pond-ts',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/pjm17971/pond-ts',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Peter Murphy. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.github,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
