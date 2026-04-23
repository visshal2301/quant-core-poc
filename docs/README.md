# Quant Core Documentation

This folder is organized so reviewers can find the right material quickly without reading every note end to end.

## Recommended reading order

1. [Architecture overview](./architecture/solution-architecture.md)
2. [Business data model](./architecture/business-data-model-guide.md)
3. [Landing and monthly processing pattern](./operations/landing-pattern.md)
4. [Scheduling and CI/CD](./operations/scheduling-and-cicd.md)
5. [Presentation outline](./presentations/presentation-outline.md)

## Folder structure

- `architecture/`
  - core platform design, medallion flow, data model, and business meaning
- `operations/`
  - ingestion patterns, runtime behavior, scheduling, and implementation notes
- `presentations/`
  - stakeholder-facing summary material
- `reference/`
  - supporting technical reference material
- `reviews/`
  - project assessments, recommendations, and review-style summaries

## Document map

- [architecture/solution-architecture.md](./architecture/solution-architecture.md)
  - end-to-end POC design, scope, medallion flow, and platform layout
- [architecture/business-data-model-guide.md](./architecture/business-data-model-guide.md)
  - business meaning of dimensions, facts, relationships, and gold outputs
- [operations/landing-pattern.md](./operations/landing-pattern.md)
  - `YYYYMM` landing convention and daily fact-file organization
- [operations/partition-replacement-fix.md](./operations/partition-replacement-fix.md)
  - rationale for partition-aware fact replacement in Silver
- [operations/scheduling-and-cicd.md](./operations/scheduling-and-cicd.md)
  - Databricks workflow and GitHub Actions deployment path
- [presentations/presentation-outline.md](./presentations/presentation-outline.md)
  - concise business presentation storyline
- [reference/yaml-files-guide.md](./reference/yaml-files-guide.md)
  - explanation of project YAML assets and how they fit together
- [reviews/project-analysis-and-recommendations.md](./reviews/project-analysis-and-recommendations.md)
  - assessment of the current solution and improvement priorities
