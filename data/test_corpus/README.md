# Auto-RAG Markdown Test Corpus

This small corpus was assembled for testing chunking, retrieval, and answer synthesis.

Files included:

- `alice_sample.md` — narrative fiction with dialogue, scene changes, and whimsical entities
- `jane_eyre_sample.md` — literary prose with dialogue and denser sentence structure
- `art_of_war_sample.md` — expository / historical / strategy text with section headings

Suggested RAG tests:

1. Chunking by headings vs fixed token windows
2. Dialogue retrieval vs narrative retrieval
3. Quoted text retrieval
4. Multi-file retrieval with source attribution
5. Mixed-domain queries (fiction vs non-fiction)
