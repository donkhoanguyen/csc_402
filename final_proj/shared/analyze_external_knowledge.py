"""
Download and analyze Spider 2.0 external knowledge documents.
Builds a bipartite graph: tasks <-> knowledge docs <-> databases
and computes network statistics for the progress report.
"""

import urllib.request
import json
import re
import os
from collections import defaultdict, Counter

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import networkx as nx
import numpy as np

BASE_URL = "https://raw.githubusercontent.com/xlang-ai/spider2/main/spider2-snow/resource/documents/"
API_URL  = "https://api.github.com/repos/xlang-ai/spider2/contents/spider2-snow/resource/documents"
OUT_DIR  = "external_knowledge_docs"

os.makedirs(OUT_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# 1. Download docs
# ---------------------------------------------------------------------------

def fetch_doc_list():
    req = urllib.request.Request(API_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def download_docs(doc_list):
    docs = {}
    for item in doc_list:
        name = item["name"]
        path = os.path.join(OUT_DIR, name)
        if not os.path.exists(path):
            url = BASE_URL + urllib.request.quote(name)
            try:
                urllib.request.urlretrieve(url, path)
            except Exception as e:
                print(f"  [WARN] {name}: {e}")
                continue
        with open(path) as f:
            docs[name] = f.read()
    return docs


# ---------------------------------------------------------------------------
# 2. Load spider2-snow tasks (Snowflake-only slice of full Spider 2.0)
# ---------------------------------------------------------------------------

def load_tasks():
    import urllib.request, json
    url = ("https://raw.githubusercontent.com/xlang-ai/spider2/main"
           "/spider2-snow/spider2-snow.jsonl")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as r:
        rows = [json.loads(line)
                for line in r.read().decode().splitlines() if line.strip()]
    return [{"id": r["instance_id"], "db": r["db_id"],
             "ek": r["external_knowledge"], "question": r["instruction"]}
            for r in rows]


# ---------------------------------------------------------------------------
# 3. Build bipartite graph: DB <-> Doc <-> Task
# ---------------------------------------------------------------------------

def build_bipartite(tasks, docs):
    G = nx.DiGraph()

    db_counter   = Counter()
    doc_counter  = Counter()
    doc_db_map   = defaultdict(set)   # doc -> set of DBs that reference it
    db_doc_map   = defaultdict(set)   # db  -> set of docs it uses
    task_doc_map = defaultdict(set)   # task-> doc

    for t in tasks:
        db  = t["db"].upper()
        ek  = t["ek"]
        tid = t["id"]

        G.add_node(db,  ntype="db")
        G.add_node(tid, ntype="task")

        if ek and ek != "None":
            G.add_node(ek, ntype="doc")
            G.add_edge(db,  ek,  rel="uses")
            G.add_edge(tid, ek,  rel="references")
            doc_db_map[ek].add(db)
            db_doc_map[db].add(ek)
            task_doc_map[tid].add(ek)
            db_counter[db]  += 1
            doc_counter[ek] += 1

    return G, db_counter, doc_counter, doc_db_map, db_doc_map


# ---------------------------------------------------------------------------
# 4. Parse doc content for keywords / entities
# ---------------------------------------------------------------------------

def parse_doc(content):
    """Extract table/column mentions heuristically from a knowledge doc."""
    # Look for backtick-quoted identifiers
    identifiers = re.findall(r'`([A-Za-z_][A-Za-z0-9_.]*)`', content)
    # Words in ALL_CAPS (common for SQL column names in docs)
    caps = re.findall(r'\b([A-Z_]{3,})\b', content)
    # Section headers
    headers = re.findall(r'^#{1,3}\s+(.+)$', content, re.MULTILINE)
    word_count = len(content.split())
    return {
        "identifiers": identifiers,
        "caps_terms": caps,
        "headers": headers,
        "word_count": word_count,
    }


# ---------------------------------------------------------------------------
# 5. Plots
# ---------------------------------------------------------------------------

def make_plots(tasks, docs, db_counter, doc_counter, doc_db_map, db_doc_map):
    fig = plt.figure(figsize=(14, 10))
    gs  = gridspec.GridSpec(2, 2, figure=fig, hspace=0.45, wspace=0.35)

    # -- (a) Tasks with vs without external knowledge --
    ax0 = fig.add_subplot(gs[0, 0])
    n_with    = sum(1 for t in tasks if t["ek"] and t["ek"] != "None")
    n_without = len(tasks) - n_with
    ax0.bar(["With external\nknowledge", "Without external\nknowledge"],
            [n_with, n_without], color=["#4C72B0", "#cccccc"], edgecolor="white")
    ax0.set_title("(a) Spider 2.0 Tasks by Knowledge Requirement", fontsize=10, fontweight="bold")
    ax0.set_ylabel("Number of tasks")
    for i, v in enumerate([n_with, n_without]):
        ax0.text(i, v + 0.5, str(v), ha="center", fontsize=10)

    # -- (b) Doc word-count distribution --
    ax1 = fig.add_subplot(gs[0, 1])
    wcs = []
    for name, content in docs.items():
        wcs.append(len(content.split()))
    ax1.hist(wcs, bins=15, color="#2ca02c", edgecolor="white", alpha=0.85)
    ax1.set_title("(b) External Knowledge Doc Length Distribution", fontsize=10, fontweight="bold")
    ax1.set_xlabel("Word count")
    ax1.set_ylabel("Number of documents")
    ax1.axvline(np.median(wcs), color="red", linestyle="--", linewidth=1.2,
                label=f"Median: {int(np.median(wcs))} words")
    ax1.legend(fontsize=8)

    # -- (c) Top DBs by number of knowledge-requiring tasks --
    ax2 = fig.add_subplot(gs[1, 0])
    top_dbs = db_counter.most_common(12)
    names, counts = zip(*top_dbs) if top_dbs else ([], [])
    short_names = [n[:18] for n in names]
    bars = ax2.barh(short_names[::-1], counts[::-1], color="#ff7f0e", edgecolor="white")
    ax2.set_title("(c) Databases with Most Knowledge-Required Tasks", fontsize=10, fontweight="bold")
    ax2.set_xlabel("Number of tasks requiring external knowledge")
    ax2.tick_params(axis='y', labelsize=7)

    # -- (d) Bipartite graph: DB <-> Doc --
    ax3 = fig.add_subplot(gs[1, 1])
    B = nx.Graph()
    db_nodes  = list(db_doc_map.keys())
    doc_nodes = list(doc_db_map.keys())
    B.add_nodes_from(db_nodes,  bipartite=0)
    B.add_nodes_from(doc_nodes, bipartite=1)
    for doc, dbs in doc_db_map.items():
        for db in dbs:
            B.add_edge(db, doc)

    pos = {}
    for i, n in enumerate(db_nodes):
        pos[n] = (0, i)
    for i, n in enumerate(doc_nodes):
        pos[n] = (1, i * (len(db_nodes) / max(len(doc_nodes), 1)))

    nx.draw_networkx_nodes(B, pos, nodelist=db_nodes,  node_color="#4C72B0",
                           node_size=120, ax=ax3, label="Database")
    nx.draw_networkx_nodes(B, pos, nodelist=doc_nodes, node_color="#ff7f0e",
                           node_size=80,  ax=ax3, label="Doc")
    nx.draw_networkx_edges(B, pos, alpha=0.4, ax=ax3, edge_color="#aaaaaa")
    # Only label DB nodes (docs are too many)
    db_labels = {n: n[:12] for n in db_nodes}
    nx.draw_networkx_labels(B, pos, labels=db_labels, font_size=5, ax=ax3)
    ax3.set_title("(d) DB–Document Bipartite Graph", fontsize=10, fontweight="bold")
    ax3.axis("off")
    ax3.legend(fontsize=7, loc="lower right")

    plt.suptitle("Spider 2.0 External Knowledge Analysis", fontsize=13, fontweight="bold")
    plt.savefig("ek_analysis.png", dpi=180, bbox_inches="tight")
    print("Saved ek_analysis.png")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Fetching doc list from GitHub...")
    doc_list = fetch_doc_list()
    print(f"  {len(doc_list)} documents found")

    print("Downloading docs...")
    docs = download_docs(doc_list)
    print(f"  {len(docs)} docs downloaded")

    print("Loading Spider 2.0 tasks...")
    tasks = load_tasks()
    print(f"  {len(tasks)} tasks loaded")

    print("Building bipartite graph...")
    G, db_counter, doc_counter, doc_db_map, db_doc_map = build_bipartite(tasks, docs)

    # Summary stats
    n_with_ek = sum(1 for t in tasks if t["ek"] and t["ek"] != "None")
    print(f"\n=== External Knowledge Statistics ===")
    print(f"  Total tasks:                  {len(tasks)}")
    print(f"  Tasks requiring external doc: {n_with_ek} ({100*n_with_ek/len(tasks):.1f}%)")
    print(f"  Unique knowledge docs:        {len(docs)}")
    print(f"  DBs with >=1 knowledge doc:   {len(db_doc_map)}")
    print(f"  Bipartite graph nodes:        {G.number_of_nodes()}")
    print(f"  Bipartite graph edges:        {G.number_of_edges()}")

    # Doc word counts
    wcs = [len(c.split()) for c in docs.values()]
    print(f"  Doc word count — min: {min(wcs)}, max: {max(wcs)}, median: {int(np.median(wcs))}")

    # Most referenced docs
    print(f"\n  Top docs by task references:")
    for doc, cnt in doc_counter.most_common(5):
        print(f"    {doc}: {cnt} tasks")

    # Identifier analysis
    all_ids = []
    for content in docs.values():
        parsed = parse_doc(content)
        all_ids.extend(parsed["identifiers"])
    id_counter = Counter(all_ids)
    print(f"\n  Top referenced identifiers across all docs:")
    for ident, cnt in id_counter.most_common(10):
        print(f"    {ident}: {cnt}")

    print("\nGenerating plots...")
    make_plots(tasks, docs, db_counter, doc_counter, doc_db_map, db_doc_map)
    print("Done.")


if __name__ == "__main__":
    main()
