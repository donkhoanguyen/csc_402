# ECE 442 Lab 1 – Questions

All questions below are to be answered and submitted in a single PDF report to Gradescope.

---

## Network analysis

Use the NetworkX or NumPy APIs on the graph `G(V,E)` (full Enron email graph).

1. **Number of directed edges (arcs)**  
   Count the number of unique ordered pairs \((u,v) \in E\) with \(u,v \in V\).

2. **Number of undirected edges**  
   Count the number of unique unordered pairs \((u,v)\) such that at least one of \((u,v) \in E\) or \((v,u) \in E\) (each pair counted once).

3. **Number of mutual arcs**  
   Count the number of pairs \((u,v)\) such that both \((u,v) \in E\) and \((v,u) \in E\).

4. **In-degree zero**  
   How many nodes have \(d_v^{\text{in}} = 0\)? List the corresponding employee names.

5. **Out-degree zero**  
   How many nodes have \(d_v^{\text{out}} = 0\)? List the corresponding employee names.

6. **Contacted by ≥30 employees**  
   How many employees were contacted by 30 or more employees?  
   Produce a graph visualization where: (i) these nodes are colored red; (ii) these nodes are labeled with the corresponding employee names.

7. **Contacted ≥30 employees**  
   How many employees contacted 30 or more employees?  
   Produce a graph visualization where: (i) these nodes are colored red; (ii) these nodes are labeled with the corresponding employee names.

8. **Degree histograms**  
   Plot histograms of vertex degrees, separately for \(d_v^{\text{in}}\) and \(d_v^{\text{out}}\) (e.g. using seaborn’s `histplot`).

---

## Dynamic (temporal) network analysis

Use the weekly graphs already built in the notebook (`graphs`, `weeks`).

9. **Centrality over time**  
   Choose two node centrality measures (e.g. from Ch. 4 of Kolaczyk’s book, the [centrality slides](https://www.hajim.rochester.edu/ece/sites/gmateos/ECE442/Slides/block_3_descriptive_analysis_properties_part_c.pdf), or [NetworkX centrality](https://networkx.org/documentation/stable/reference/algorithms/centrality.html)).  
   For each week, report who was the most central Enron employee for each of the two measures.  
   Compare these results with the most central employee in the “entire” graph (full time horizon).

10. **Graph-level statistics and events**  
    Use a few graph-level summary statistics (e.g. number of nodes, number of edges, average degree, average clustering coefficient, or others of your choice) over time.  
    Use them to identify major events related to the scandal (see e.g. Figure 8 in https://arxiv.org/abs/1403.0989).  
    You should be able to spot at least the launch of Enron Online and Stephen Cooper’s ascent to CEO.

---

## Graph Laplacian (Zachary’s karate club)

Use PyTorch Geometric’s KarateClub dataset and (as needed) NetworkX and `torch.linalg`.

11. **Compute the Laplacian**  
    Compute the graph Laplacian matrix \(\mathbf{L}\) for Zachary’s karate club network (e.g. using [`torch_geometric.utils`](https://pytorch-geometric.readthedocs.io/en/latest/modules/utils.html)).

12. **Zero eigenvalue and ones vector**  
    Check that \(\mathbf{L}\) has an eigenvalue 0 and that the vector of all ones \([1,1,\ldots,1]^\top\) is a corresponding eigenvector (e.g. using [`torch.linalg`](https://pytorch.org/docs/stable/linalg.html)).

13. **Symmetric and positive semidefinite**  
    Verify that \(\mathbf{L}\) is symmetric and positive semidefinite.

14. **Incidence matrix factorization**  
    Form the signed incidence matrix \(\tilde{\mathbf{B}}\) as in Part 2 of the optional exercise below, and verify that \(\mathbf{L} = \tilde{\mathbf{B}}\tilde{\mathbf{B}}^\top\) (e.g. using [NetworkX `incidence_matrix`](https://networkx.org/documentation/stable/reference/generated/networkx.linalg.graphmatrix.incidence_matrix.html)).

---

## Optional (extra credit): Prove Laplacian properties

For an undirected, unweighted graph \(G(V,E)\) with adjacency matrix \(\mathbf{A}\), degree matrix \(\mathbf{D} = \text{diag}(d_1,\ldots,d_{N_v})\), and Laplacian \(\mathbf{L} = \mathbf{D} - \mathbf{A}\):

1. Prove that \(\mathbf{1} := [1,\ldots,1]^\top\) is an eigenvector of \(\mathbf{L}\) with eigenvalue \(0\).
2. Using a signed incidence matrix \(\tilde{\mathbf{B}}\) (with arbitrary orientation per edge), prove \(\mathbf{L} = \tilde{\mathbf{B}}\tilde{\mathbf{B}}^\top\).
3. Show that for any \(\mathbf{x}\), \(\mathbf{x}^\top \mathbf{L}\mathbf{x} = \sum_{(i,j) \in E} (x_i - x_j)^2\), and conclude that \(\mathbf{L}\) is symmetric positive semidefinite.
4. If \(G\) is disconnected, show \(\mathbf{L}\) is block diagonal and that the second smallest eigenvalue is 0 by constructing two linearly independent eigenvectors with eigenvalue 0.