---
sidebar_position: 1
slug: /intro
---

<div align="center">
<p align="center">

<img src="asset/logo.png" alt="Skardi Logo" width="700" />

**The Declarative data runtime for AI and agents powered by Rust and Apache Datafusion.**<br/>
**Query files, databases, data lakes, and vector stores with SQL — locally with skardi-cli or as APIs with skardi-server.**

[CI]: https://github.com/SkardiLabs/skardi/actions/workflows/ci.yml
[CI Badge]: https://github.com/SkardiLabs/skardi/actions/workflows/ci.yml/badge.svg

[![CI Badge]][CI]

</p>
</div>

<hr />

Skardi lets AI agents and applications query files, databases, data lakes, and vector stores with SQL — no application code required.

- **`skardi-cli`** — Run SQL queries locally against files, object stores, databases, and datalake formats. Ideal for local agents like [OpenClaw](https://github.com/openclaw/openclaw) that need structured data access without a running server.
- **`skardi-server`** — Define SQL queries in YAML and serve them as parameterized HTTP APIs. Connect to multiple data sources, run federated queries, and expose results as REST endpoints.

> **⚠️Warning:** This software is in BETA. It may still contain bugs and unexpected behavior. Use caution with production data and ensure you have backups. Feel free to contact us if you want to have a POC for the product.
