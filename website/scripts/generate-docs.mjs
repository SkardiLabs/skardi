#!/usr/bin/env node
/**
 * Splits README.md into individual Docusaurus docs pages.
 * Run automatically as part of `npm run build` via the prebuild hook.
 */

import { readFileSync, writeFileSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const README = readFileSync(join(__dirname, '../../README.md'), 'utf8');
const DOCS = join(__dirname, '../docs');
const GITHUB_BASE = 'https://github.com/SkardiLabs/skardi/blob/main';

// Rewrite relative file links to absolute GitHub URLs so Docusaurus doesn't
// try to resolve paths that only exist in the repo (e.g. demo/postgres/README.md)
function rewriteLinks(content) {
  // Matches [text](relative/path) — skips http(s):// and anchor-only links
  return content.replace(
    /\[([^\]]+)\]\((?!https?:\/\/)(?!#)([^)]+)\)/g,
    (_, text, href) => `[${text}](${GITHUB_BASE}/${href})`,
  );
}

// Split README into sections keyed by ## heading text
const sections = {};
let heading = null;
let lines = [];

for (const line of README.split('\n')) {
  if (line.startsWith('## ')) {
    if (heading) sections[heading] = lines.join('\n').trimEnd();
    heading = line.slice(3).trim();
    lines = [line];
  } else {
    lines.push(line);
  }
}
if (heading) sections[heading] = lines.join('\n').trimEnd();

// Intro: everything before the first ## heading
const intro = README.split(/\n## /)[0].trimEnd();

// Helper to write a doc file
function write(relPath, frontmatter, content) {
  const full = join(DOCS, relPath);
  mkdirSync(dirname(full), { recursive: true });
  writeFileSync(full, `---\n${frontmatter}\n---\n\n${rewriteLinks(content)}\n`);
  console.log(`  wrote ${relPath}`);
}

// Map sections to files
write('intro.md',
  'sidebar_position: 1\nslug: /intro',
  intro);

write('installation.md',
  'sidebar_position: 2',
  [sections['Installation'], sections['Building from Source']]
    .filter(Boolean).join('\n\n'));

write('quick-start.md',
  'sidebar_position: 3',
  sections['Quick Start'] ?? '');

write('cli.md',
  'sidebar_position: 4',
  sections['Skardi CLI'] ?? '');

write('server/overview.md',
  'sidebar_position: 1',
  sections['Skardi Server'] ?? '');

write('data-sources/overview.md',
  'sidebar_position: 1',
  sections['Supported Data Sources'] ?? '');

write('features/federated-queries.md',
  'sidebar_position: 1',
  sections['Federated Queries'] ?? '');

write('features/onnx-inference.md',
  'sidebar_position: 2',
  sections['ONNX Model Inference'] ?? '');

write('docker.md',
  'sidebar_position: 8',
  sections['Docker'] ?? '');

console.log('Docs generated from README.md');
