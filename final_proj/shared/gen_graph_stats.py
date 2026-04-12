import json, statistics
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

with open("graph_stats.json") as f:
    data = json.load(f)

cpt  = data["cpt"]   # columns per table
tpd  = data["tpd"]   # tables per DB
labs = data["tpd_labels"]

fig = plt.figure(figsize=(13, 5))
gs  = gridspec.GridSpec(1, 3, figure=fig, wspace=0.38)

# -- (a) Columns per table distribution (log scale) --
ax0 = fig.add_subplot(gs[0])
ax0.hist(cpt, bins=50, color="#ff7f0e", edgecolor="white", alpha=0.85)
ax0.set_yscale("log")
ax0.axvline(statistics.median(cpt), color="red", linestyle="--", linewidth=1.4,
            label=f"Median: {statistics.median(cpt):.0f}")
ax0.set_title("(a) Columns per Table\n(Spider 2.0, log y-axis)", fontsize=10, fontweight="bold")
ax0.set_xlabel("Number of columns")
ax0.set_ylabel("Count (log scale)")
ax0.legend(fontsize=8)

# -- (b) Tables per DB distribution (log scale) --
ax1 = fig.add_subplot(gs[1])
ax1.hist(tpd, bins=40, color="#4C72B0", edgecolor="white", alpha=0.85)
ax1.set_yscale("log")
ax1.axvline(statistics.median(tpd), color="red", linestyle="--", linewidth=1.4,
            label=f"Median: {statistics.median(tpd):.0f}")
ax1.set_title("(b) Tables per Database\n(Spider 2.0, log y-axis)", fontsize=10, fontweight="bold")
ax1.set_xlabel("Number of tables")
ax1.set_ylabel("Count (log scale)")
ax1.legend(fontsize=8)

# -- (c) Top 12 DBs by table count --
ax2 = fig.add_subplot(gs[2])
top_n = 12
top_labs = [l[:20] for l in labs[:top_n]]
top_vals = tpd[:top_n]
ax2.barh(top_labs[::-1], top_vals[::-1], color="#2ca02c", edgecolor="white", alpha=0.85)
ax2.set_title("(c) Largest Databases\nby Table Count", fontsize=10, fontweight="bold")
ax2.set_xlabel("Number of tables")
ax2.tick_params(axis="y", labelsize=7)
ax2.set_xscale("log")

plt.suptitle("Spider 2.0 Schema Complexity — Graph Statistics", fontsize=12, fontweight="bold")
plt.savefig("graph_stats.png", dpi=180, bbox_inches="tight")
print("Saved graph_stats.png")
