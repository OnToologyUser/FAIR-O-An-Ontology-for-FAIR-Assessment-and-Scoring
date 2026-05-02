#!/usr/bin/env python3
"""Reorganize a Turtle (.ttl) file so that all triples sharing the same subject
are grouped into a single block.
"""

from __future__ import annotations

import sys
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import rdflib
from rdflib import Graph, URIRef, BNode, Literal, Namespace
from rdflib.namespace import OWL, RDF, RDFS, XSD

def extract_prefixes(ttl_text: str) -> List[str]:
    """Return the raw @prefix / @base lines from the file, in order."""
    lines = []
    for line in ttl_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("@prefix") or stripped.startswith("@base"):
            lines.append(stripped)
    return lines


def extract_section_comments(ttl_text: str) -> List[str]:
    """Return lines that are pure comment-blocks (# …) at the top level."""
    comments = []
    for line in ttl_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            comments.append(stripped)
    return comments

def subject_rank(subject: rdflib.term.Node, graph: Graph) -> int:
    """Return an integer that determines the group a subject belongs to."""
    types = set(graph.objects(subject, RDF.type))
    if OWL.Ontology in types:
        return 0
    if OWL.Class in types or RDFS.Class in types:
        return 1
    if OWL.ObjectProperty in types or URIRef(str(OWL) + "TransitiveObjectProperty") in types:
        return 2
    if OWL.DatatypeProperty in types or OWL.AnnotationProperty in types:
        return 3
    return 4  


def sort_key(subject: rdflib.term.Node, graph: Graph) -> Tuple[int, str]:
    rank = subject_rank(subject, graph)
    return (rank, str(subject))


PREDICATE_ORDER = [
    str(RDF.type),
    str(RDFS.subClassOf),
    str(RDFS.subPropertyOf),
    str(OWL.equivalentClass),
    str(OWL.equivalentProperty),
    str(OWL.oneOf),
    str(RDFS.domain),
    str(RDFS.range),
    str(RDFS.label),
    str(RDFS.comment),
    str(OWL.imports),
    str(OWL.versionInfo),
]


def predicate_sort_key(predicate: URIRef) -> Tuple[int, str]:
    p = str(predicate)
    try:
        return (PREDICATE_ORDER.index(p), p)
    except ValueError:
        return (len(PREDICATE_ORDER), p)


_RDF_TYPE = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
_RDF_FIRST = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#first")
_RDF_REST  = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest")
_RDF_NIL   = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil")


def _bnode_is_list(bnode: BNode, graph: Graph) -> bool:
    """Return True if this blank node is the head of an rdf:List."""
    return (bnode, _RDF_FIRST, None) in graph


def _render_rdf_list(bnode: BNode, graph: Graph, ns_mgr: rdflib.namespace.NamespaceManager) -> str:
    """Render an rdf:List blank-node chain as Turtle ( item1 item2 ... )."""
    items = []
    node = bnode
    while node != _RDF_NIL and not isinstance(node, type(None)):
        first_vals = list(graph.objects(node, _RDF_FIRST))
        if not first_vals:
            break
        items.append(term_to_turtle(first_vals[0], graph, ns_mgr))
        rest_vals = list(graph.objects(node, _RDF_REST))
        node = rest_vals[0] if rest_vals else _RDF_NIL
    return "( " + " ".join(items) + " )"


def term_to_turtle(term: rdflib.term.Node, graph: Graph, ns_mgr: rdflib.namespace.NamespaceManager) -> str:
    """Render an RDF term to a compact Turtle string."""
    if term == _RDF_TYPE:
        return "a"
    if isinstance(term, URIRef):
        try:
            qname = term.n3(ns_mgr)
            return qname
        except Exception:
            return f"<{term}>"
    if isinstance(term, BNode):
        if _bnode_is_list(term, graph):
            return _render_rdf_list(term, graph, ns_mgr)
        # Generic blank node: inline as [ p o ; p o ]
        preds = list(graph.predicate_objects(term))
        if not preds:
            return "[]"
        parts = []
        for p, o in preds:
            p_str = term_to_turtle(p, graph, ns_mgr)
            o_str = term_to_turtle(o, graph, ns_mgr)
            parts.append(f"{p_str} {o_str}")
        return "[ " + " ; ".join(parts) + " ]"
    if isinstance(term, Literal):
        return term.n3(ns_mgr)
    return str(term)


def serialize_subject_block(
    subject: rdflib.term.Node,
    graph: Graph,
    ns_mgr: rdflib.namespace.NamespaceManager,
) -> str:
    """Return a Turtle block for all predicate-object pairs of one subject."""
    subj_str = term_to_turtle(subject, graph, ns_mgr)

    # Collect (predicate, [objects]) grouped
    po_map: Dict[URIRef, List] = defaultdict(list)
    for p, o in graph.predicate_objects(subject):
        po_map[p].append(o)

    # Sort predicates
    sorted_preds = sorted(po_map.keys(), key=predicate_sort_key)

    if not sorted_preds:
        return ""

    # Build one "predicate-group" per predicate (a list of lines whose last
    # line receives either " ;" (more predicates follow) or " ." (last).
    pred_groups: List[List[str]] = []
    for pred in sorted_preds:
        pred_str = term_to_turtle(pred, graph, ns_mgr)
        objects = sorted(po_map[pred], key=lambda t: str(t))
        if len(objects) == 1:
            obj_str = term_to_turtle(objects[0], graph, ns_mgr)
            pred_groups.append([f"    {pred_str} {obj_str}"])
        else:
            # Multiple objects: align continuation under first object
            indent = " " * (4 + len(pred_str) + 1)
            obj_strs = [term_to_turtle(o, graph, ns_mgr) for o in objects]
            group_lines = [f"    {pred_str} {obj_strs[0]} ,"]
            for s in obj_strs[1:-1]:
                group_lines.append(f"{indent}{s} ,")
            group_lines.append(f"{indent}{obj_strs[-1]}")
            pred_groups.append(group_lines)

    block_lines = [f"{subj_str}"]
    for gi, group in enumerate(pred_groups):
        terminator = " ;" if gi < len(pred_groups) - 1 else " ."
        for li, line in enumerate(group):
            if li == len(group) - 1:
                block_lines.append(line + terminator)
            else:
                block_lines.append(line)

    return "\n".join(block_lines)

GROUP_HEADERS = {
    0: "Ontology",
    1: "Classes",
    2: "Object Properties",
    3: "Datatype Properties",
    4: "Individuals",
}


def section_header(label: str) -> str:
    bar = "#" * 80
    return f"{bar}\n# {label}\n{bar}"

def organize(input_path: str, output_path: str) -> None:
    src = Path(input_path).read_text(encoding="utf-8")

    g = Graph()
    g.parse(data=src, format="turtle")

    # Collect all non-blank-node subjects (bnodes are inlined)
    top_subjects = [s for s in set(g.subjects()) if not isinstance(s, BNode)]
    top_subjects.sort(key=lambda s: sort_key(s, g))

    # Build prefix declarations from the parsed graph
    prefix_lines: List[str] = []
    # Preserve the original @prefix lines so order/aliases match the source
    original_prefixes = extract_prefixes(src)
    prefix_lines.extend(original_prefixes)
    has_rdf_prefix = any("rdf:" in line and "<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" in line
                         for line in original_prefixes)
    if not has_rdf_prefix:
        prefix_lines.append("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .")

    groups: Dict[int, List] = defaultdict(list)
    for s in top_subjects:
        rank = subject_rank(s, g)
        groups[rank].append(s)

    output_parts: List[str] = []

    output_parts.append("\n".join(prefix_lines))
    output_parts.append("")

    last_rank = -1
    for rank in sorted(groups.keys()):
        subjects_in_group = groups[rank]
        if not subjects_in_group:
            continue

        if rank != last_rank:
            label = GROUP_HEADERS.get(rank, f"Group {rank}")
            output_parts.append("")
            output_parts.append(section_header(label))
            output_parts.append("")
            last_rank = rank

        for subj in subjects_in_group:
            block = serialize_subject_block(subj, g, g.namespace_manager)
            if block:
                output_parts.append(block)
                output_parts.append("")

    result = "\n".join(output_parts)
    result = re.sub(r"\n{3,}", "\n\n", result)

    Path(output_path).write_text(result.strip() + "\n", encoding="utf-8")
    print(f"Organized: {input_path} -> {output_path}")
    print(f"  Subjects grouped: {len(top_subjects)}")

def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: organize_ttl.py <input.ttl> [output.ttl]", file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) >= 3 else input_path
    organize(input_path, output_path)


if __name__ == "__main__":
    main()
