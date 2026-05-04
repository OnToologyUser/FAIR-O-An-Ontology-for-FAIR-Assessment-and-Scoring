from pathlib import Path
import csv
from rdflib import Graph

BASE_DIR = Path(__file__).resolve().parent.parent

FILES_TO_LOAD = [
    BASE_DIR / "FAIR-assessment-ontology.ttl",
    BASE_DIR / "FAIR-assessment-data.ttl",
]

QUERY_DIR = BASE_DIR / "queries"
RESULT_DIR = BASE_DIR / "results"
RESULT_DIR.mkdir(exist_ok=True)

graph = Graph()

for file_path in FILES_TO_LOAD:
    graph.parse(file_path, format="turtle")

summary_rows = []

for query_path in sorted(QUERY_DIR.glob("CQ*.rq")):
    query = query_path.read_text(encoding="utf-8")
    query_results = graph.query(query)

    variables = [str(var) for var in query_results.vars]
    rows = list(query_results)

    output_file = RESULT_DIR / query_path.name.replace(".rq", ".csv")

    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(variables)

        for row in rows:
            writer.writerow([
                str(value) if value is not None else ""
                for value in row
            ])

    cq_id = query_path.stem.split("-")[0]
    status = "Answered" if rows else "No results"

    summary_rows.append([
        cq_id,
        query_path.name,
        len(rows),
        status
    ])

summary_file = RESULT_DIR / "cq-evaluation-summary.csv"

with summary_file.open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["CQ", "Query file", "Rows returned", "Status"])
    writer.writerows(summary_rows)

print(f"Loaded {len(graph)} triples.")
print(f"Saved query results in {RESULT_DIR}.")
print(f"Saved summary in {summary_file}.")