# FAIR-O: An Ontology for FAIR Assessment and Scoring

<p align="center">
	<img src="assets/logo.png" alt="FAIR-O logo" width="150" />
</p>

FAIR-O provides a structured and extensible model for representing the evaluation of digital resources against the FAIR (Findable, Accessible, Interoperable, Reusable) principles. It supports detailed sub-principle results, evidence, scoring functions, and aggregation methods, with provenance-aware assessments aligned to the official FAIR Vocabulary.

## Overview

- Ontology IRI: https://w3id.org/fair-o
- DOI: [10.5281/zenodo.20027101](https://doi.org/10.5281/zenodo.20027101)
- License: CC BY 4.0
- Imports: PROV-O, SKOS, FAIR Vocabulary

## Repository structure

- Ontology: [FAIR-O.ttl](FAIR-O.ttl)
- SHACL shapes constrains: [FAIR-O_shape.ttl](FAIR-O_shape.ttl)
- Instance of the ontology with KGHeartBeat FAIR assessment results: [FAIR-O_data.ttl](FAIR-O_data.ttl)
- Documentation (Widoco output): [docs/index.html](docs/index.html)
- Competency queries (SPARQL): [queries](queries)
- Query results: [results](results)
- Data snapshots and mappings: [data](data)
- Utility scripts: [src](src)

## Documentation

- Main docs page: [docs/index.html](docs/index.html)
- WebVOWL visualization: [docs/webvowl/index.html](docs/webvowl/index.html)
- Provenance report: [docs/provenance/provenance-en.html](docs/provenance/provenance-en.html)

## Queries and results

The [queries](queries) folder contains competency queries (CQ1-CQ14) that exercise key modeling features of FAIR-O. The [results](results) folder includes example outputs and a summary CSV.

## Scripts

The [src](src) folder includes helper scripts for cleaning descriptions, organizing TTL files, validating data, and running the SPARQL queries that populate the results folder.

## Regenerate data, run CQs, and validate SHACL

### Dependencies

The scripts rely on Python with `pandas`, `rdflib`, and `pyshacl` installed.

### Regenerate the FAIR-O instance from KGHeartBeat snapshots

From the repository root:

```bash
python src/kgheartbeat_to_fairo.py \
	--input-folder data \
	--mapping-json data/fair_mapping.json \
	--principles-doc data/fair_principle_doc.json \
	--output FAIR-O_data.ttl \
	--organize
```

### Execute competency queries (CQs)

```bash
python src/run_queries.py
```

Results are written to [results](results) and summarized in [results/cq-evaluation-summary.csv](results/cq-evaluation-summary.csv).

### Validate data with SHACL constraints

```bash
python src/validate_data.py \
	--data FAIR-O_data.ttl \
	--shapes FAIR-O_shape.ttl \
	--ontology FAIR-O.ttl \
	--report results/shacl-validation-report.ttl
```

The command prints the validation report and returns a non-zero exit code when constraints are violated.

## License

Released under the Creative Commons Attribution 4.0 International (CC BY 4.0) license. See [LICENSE](LICENSE).