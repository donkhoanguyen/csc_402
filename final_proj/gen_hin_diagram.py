import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch
import numpy as np

fig, ax = plt.subplots(figsize=(10, 6))
ax.set_xlim(0, 10)
ax.set_ylim(0, 6)
ax.axis("off")

# Node positions
nodes = {
    "Database":  (1.5, 5.0),
    "Table":     (4.5, 5.0),
    "Column":    (7.5, 5.0),
    "Metric":    (7.5, 2.5),
    "DbtModel":  (1.5, 2.5),
}

colors = {
    "Database":  "#4C72B0",
    "Table":     "#2ca02c",
    "Column":    "#ff7f0e",
    "Metric":    "#9467bd",
    "DbtModel":  "#8c8c8c",
}

# Draw nodes
for name, (x, y) in nodes.items():
    ax.add_patch(mpatches.FancyBboxPatch(
        (x - 0.9, y - 0.35), 1.8, 0.7,
        boxstyle="round,pad=0.05",
        facecolor=colors[name], edgecolor="white",
        linewidth=1.5, alpha=0.9, zorder=3
    ))
    ax.text(x, y, name, ha="center", va="center",
            fontsize=11, fontweight="bold", color="white", zorder=4)

def arrow(ax, src, dst, label, color="#333333", rad=0.0, lbl_offset=(0, 0.18)):
    x1, y1 = nodes[src]
    x2, y2 = nodes[dst]
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(
                    arrowstyle="-|>",
                    color=color,
                    lw=1.6,
                    connectionstyle=f"arc3,rad={rad}",
                ),
                zorder=2)
    mx = (x1 + x2) / 2 + lbl_offset[0]
    my = (y1 + y2) / 2 + lbl_offset[1]
    ax.text(mx, my, label, ha="center", va="center",
            fontsize=8.5, color=color,
            bbox=dict(facecolor="white", edgecolor="none", pad=1.5))

# Edges
arrow(ax, "Database", "Table",    "HAS",          "#4C72B0")
arrow(ax, "Table",    "Column",   "HAS",          "#2ca02c")
arrow(ax, "Column",   "Metric",   "REFERENCES",   "#ff7f0e", lbl_offset=(0.5, 0))
arrow(ax, "Table",    "Metric",   "REFERENCES",   "#2ca02c", rad=0.3, lbl_offset=(-0.5, 0))
arrow(ax, "DbtModel", "Table",    "READS",        "#8c8c8c", rad=0.25, lbl_offset=(0, 0.25))
arrow(ax, "DbtModel", "Table",    "PRODUCES",     "#8c8c8c", rad=-0.25, lbl_offset=(0, -0.18))

# Self-loop: FK on Column
ax.annotate("", xy=(7.5 + 0.9, 5.0 + 0.1), xytext=(7.5 + 0.9, 5.0 - 0.1),
            arrowprops=dict(
                arrowstyle="-|>", color="#ff7f0e", lw=1.6,
                connectionstyle="arc3,rad=-2.5",
            ), zorder=2)
ax.text(9.3, 5.0, "FK", ha="center", va="center",
        fontsize=8.5, color="#ff7f0e",
        bbox=dict(facecolor="white", edgecolor="none", pad=1.5))

# Self-loop: DEPENDS_ON on DbtModel
ax.annotate("", xy=(1.5 - 0.9, 2.5 + 0.1), xytext=(1.5 - 0.9, 2.5 - 0.1),
            arrowprops=dict(
                arrowstyle="-|>", color="#8c8c8c", lw=1.6,
                connectionstyle="arc3,rad=2.5",
            ), zorder=2)
ax.text(0.1, 2.5, "DEPENDS\nON", ha="center", va="center",
        fontsize=7.5, color="#8c8c8c",
        bbox=dict(facecolor="white", edgecolor="none", pad=1.5))

ax.set_title(
    "Heterogeneous Information Network (HIN) Schema",
    fontsize=13, fontweight="bold", pad=12
)

plt.tight_layout()
plt.savefig("hin_diagram.png", dpi=180, bbox_inches="tight")
print("Saved hin_diagram.png")
