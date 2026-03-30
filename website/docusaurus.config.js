// @ts-check
import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Skardi',
  tagline: 'The declarative data runtime for AI and agents powered by Rust and Apache DataFusion.',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://skardilabs.github.io',
  baseUrl: '/skardi/',

  organizationName: 'SkardiLabs',
  projectName: 'skardi',

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/SkardiLabs/skardi/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: 'img/logo.png',
      colorMode: {
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'Skardi',
        logo: {
          alt: 'Skardi Logo',
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
            href: 'https://skardilabs.github.io/skardi/api',
            label: 'API Reference',
            position: 'left',
          },
          {
            href: 'https://github.com/SkardiLabs/skardi',
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
              {label: 'Getting Started', to: '/docs/intro'},
              {label: 'CLI', to: '/docs/cli'},
              {label: 'Server', to: '/docs/server/overview'},
              {label: 'Data Sources', to: '/docs/data-sources/overview'},
            ],
          },
          {
            title: 'More',
            items: [
              {label: 'API Reference', href: 'https://skardilabs.github.io/skardi/api'},
              {label: 'GitHub', href: 'https://github.com/SkardiLabs/skardi'},
              {label: 'Releases', href: 'https://github.com/SkardiLabs/skardi/releases'},
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} SkardiLabs. Built with Docusaurus.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['rust', 'bash', 'yaml', 'sql', 'json'],
      },
    }),
};

export default config;
