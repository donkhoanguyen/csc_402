# Optional Extra Credit: Properties of the Graph Laplacian

**Setup:** Consider an undirected and unweighted network graph $G(V,E)$, with order $N_v := |V|$, size $N_e := |E|$, and adjacency matrix $\mathbf{A}$. Let $\mathbf{D} = \text{diag}(d_1, \ldots, d_{N_v})$ be the degree matrix and $\mathbf{L} := \mathbf{D} - \mathbf{A}$ the Laplacian of $G$.

---

## Part 1: $\mathbf{1}$ is an eigenvector of $\mathbf{L}$ with eigenvalue $0$

**Claim:** $\mathbf{1} := [1, \ldots, 1]^\top \in \mathbb{R}^{N_v}$ satisfies $\mathbf{L}\mathbf{1} = \mathbf{0}$.

**Proof:**  
For each row $i$ of $\mathbf{L} = \mathbf{D} - \mathbf{A}$:
- The $i$-th row of $\mathbf{D}$ has entry $d_i$ in column $i$ and 0 elsewhere, so $(\mathbf{D}\mathbf{1})_i = d_i$.
- The $i$-th row of $\mathbf{A}$ has a 1 in each column $j$ such that $\{i,j\} \in E$, and 0 elsewhere, so $(\mathbf{A}\mathbf{1})_i = \sum_{j : \{i,j\} \in E} 1 = d_i$.

Therefore
$$
(\mathbf{L}\mathbf{1})_i = (\mathbf{D}\mathbf{1})_i - (\mathbf{A}\mathbf{1})_i = d_i - d_i = 0
$$
for all $i$. Hence $\mathbf{L}\mathbf{1} = \mathbf{0}$, i.e. $\mathbf{1}$ is an eigenvector of $\mathbf{L}$ with eigenvalue $0$. ∎

---

## Part 2: Laplacian factorization $\mathbf{L} = \tilde{\mathbf{B}}\tilde{\mathbf{B}}^\top$

**Setup:** Assign an arbitrary orientation to each edge (head/tail). The *signed incidence matrix* $\tilde{\mathbf{B}} \in \{-1, 0, 1\}^{N_v \times N_e}$ has
$$
\tilde{\mathbf{B}}_{ij} =
\begin{cases}
  \phantom{-}1, & \text{if vertex } i \text{ is the tail of edge } j, \\
  -1, & \text{if vertex } i \text{ is the head of edge } j, \\
  \phantom{-}0, & \text{otherwise}.
\end{cases}
$$

**Claim:** $\mathbf{L} = \tilde{\mathbf{B}}\tilde{\mathbf{B}}^\top$.

**Proof:**  
Compute $(\tilde{\mathbf{B}}\tilde{\mathbf{B}}^\top)_{ik} = \sum_{j=1}^{N_e} \tilde{\mathbf{B}}_{ij} \tilde{\mathbf{B}}_{kj}$.

- **Case $i = k$:**  
  For each edge $j$, vertex $i$ is either tail (+1) or head (-1) or not incident (0). So $\tilde{\mathbf{B}}_{ij}^2 \in \{0, 1\}$ and $\sum_j \tilde{\mathbf{B}}_{ij}^2$ counts edges incident to $i$, i.e. $(\tilde{\mathbf{B}}\tilde{\mathbf{B}}^\top)_{ii} = d_i = (\mathbf{L})_{ii}$.

- **Case $i \neq k$:**  
  $\tilde{\mathbf{B}}_{ij}\tilde{\mathbf{B}}_{kj} \neq 0$ only when edge $j$ is incident to both $i$ and $k$. For such an edge, one of $i,k$ is tail (+1) and the other head (-1), so $\tilde{\mathbf{B}}_{ij}\tilde{\mathbf{B}}_{kj} = -1$. Thus
  $$
  (\tilde{\mathbf{B}}\tilde{\mathbf{B}}^\top)_{ik} = -\#\{\text{edges between } i \text{ and } k\} = -A_{ik} = (\mathbf{L})_{ik}.
  $$

So $\tilde{\mathbf{B}}\tilde{\mathbf{B}}^\top = \mathbf{D} - \mathbf{A} = \mathbf{L}$. ∎

---

## Part 3: Quadratic form $\mathbf{x}^\top \mathbf{L}\mathbf{x}$ and positive semi-definiteness

**Claim:** For any $\mathbf{x} = [x_1, \ldots, x_{N_v}]^\top \in \mathbb{R}^{N_v}$,
$$
\mathbf{x}^\top \mathbf{L}\mathbf{x} = \sum_{(i,j) \in E} (x_i - x_j)^2.
$$
Hence $\mathbf{L}$ is symmetric positive semi-definite.

**Proof:**  
Using $\mathbf{L} = \tilde{\mathbf{B}}\tilde{\mathbf{B}}^\top$,
$$
\mathbf{x}^\top \mathbf{L}\mathbf{x} = \mathbf{x}^\top \tilde{\mathbf{B}}\tilde{\mathbf{B}}^\top \mathbf{x} = \|\tilde{\mathbf{B}}^\top \mathbf{x}\|^2.
$$
The $j$-th entry of $\tilde{\mathbf{B}}^\top \mathbf{x}$ corresponds to edge $j$. If edge $j$ connects vertices $i$ (tail) and $k$ (head), then $(\tilde{\mathbf{B}}^\top \mathbf{x})_j = x_i - x_k$. So
$$
(\tilde{\mathbf{B}}^\top \mathbf{x})_j^2 = (x_i - x_k)^2.
$$
Summing over edges (each undirected edge $(i,j)$ appears once in the sum, with one orientation),
$$
\mathbf{x}^\top \mathbf{L}\mathbf{x} = \sum_{j=1}^{N_e} (\tilde{\mathbf{B}}^\top \mathbf{x})_j^2 = \sum_{(i,j) \in E} (x_i - x_j)^2.
$$

- **Symmetric:** $\mathbf{L} = \mathbf{D} - \mathbf{A}$ and both $\mathbf{D}$ and $\mathbf{A}$ are symmetric, so $\mathbf{L}^\top = \mathbf{L}$.
- **Positive semi-definite:** $\mathbf{x}^\top \mathbf{L}\mathbf{x} = \sum_{(i,j) \in E} (x_i - x_j)^2 \geq 0$ for all $\mathbf{x}$, so $\mathbf{L} \succeq 0$. ∎

---

## Part 4: Disconnected graph and second smallest eigenvalue

**Claim:** If $G$ is disconnected, then $\mathbf{L}$ is block diagonal (with blocks corresponding to connected components), and the second smallest eigenvalue of $\mathbf{L}$ is zero.

**Proof:**

1. **Block structure:** Label vertices so that vertices in the same connected component have consecutive indices. Then there is no edge between different components, so $\mathbf{A}$ (and hence $\mathbf{D}$ and $\mathbf{L}$) has no off-diagonal blocks between components. Thus $\mathbf{L} = \text{blkdiag}(\mathbf{L}_1, \ldots, \mathbf{L}_C)$, where $C \geq 2$ is the number of components and $\mathbf{L}_c$ is the Laplacian of the $c$-th component.

2. **Two independent zero eigenvectors:** For each component $c$, the indicator vector $\mathbf{v}^{(c)} \in \mathbb{R}^{N_v}$ with $v^{(c)}_i = 1$ if vertex $i$ belongs to component $c$ and $v^{(c)}_i = 0$ otherwise satisfies $\mathbf{L}\mathbf{v}^{(c)} = \mathbf{0}$ (within that block, $\mathbf{L}_c \mathbf{1}_c = \mathbf{0}$; outside the block, zeros). So each $\mathbf{v}^{(c)}$ is an eigenvector of $\mathbf{L}$ with eigenvalue $0$.  
   The vectors $\mathbf{v}^{(1)}, \ldots, \mathbf{v}^{(C)}$ are linearly independent (their supports are disjoint and non-empty). So the eigenvalue $0$ has geometric multiplicity at least $C \geq 2$.

3. **Second smallest eigenvalue:** The eigenvalues of $\mathbf{L}$ are the union of the eigenvalues of the $\mathbf{L}_c$. Each $\mathbf{L}_c$ has eigenvalue $0$ with eigenvector $\mathbf{1}_c$. So $\mathbf{L}$ has at least two linearly independent eigenvectors for eigenvalue $0$. Ordering the eigenvalues as $0 = \lambda_1 \leq \lambda_2 \leq \cdots$, we get $\lambda_1 = \lambda_2 = 0$; hence the second smallest eigenvalue of $\mathbf{L}$ is zero. ∎

---

*End of solutions.*
