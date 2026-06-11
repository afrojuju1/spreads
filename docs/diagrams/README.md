# Diagrams

This directory holds the canonical Mermaid source files for repo planning and architecture diagrams.

Conventions:

- use `.mmd` for Mermaid source files
- keep diagrams grouped by area, such as `planning/`
- name files after the source document and diagram purpose
- planning documents should link to these files instead of keeping the only copy inline
- keep links inside markdown docs repo-relative so they work in git and local editors

Current subfolders:

- `current/` for diagrams that describe the current runtime architecture
- `planning/` for diagrams referenced by planning documents under `docs/planning/`

Current architecture:

- [`current/system_architecture.md`](current/system_architecture.md) - ASCII diagram in Markdown for plain-text renderers
- [`current/system_architecture.mmd`](current/system_architecture.mmd)

Primary planning entrypoint:

- [`docs/planning/README.md`](../planning/README.md)
