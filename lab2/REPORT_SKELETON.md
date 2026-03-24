# ECE 442 Network Science Analytics - Laboratory 2 Report
## Descriptive Analysis of Network Graph Characteristics

---

## 1. Introduction
[Brief overview of the laboratory objectives and methods]

---

## 2. Structural Properties of Large-Scale Networks

### 2.1 Degree Distribution

**Question 1:** Write a function that computes the degree distribution of a graph given its degree sequence. The function `numpy.histogram` may be useful to that end.

**Response:**
- [Describe your implementation]
- [Show the function code]
- [Test results on the toy graph]

---

### 2.2 Power-Law Distributions and Scale-Free Networks

**Question 2:** Plot the degree distribution $P(d)$ versus $d$ in log-log scale. Would you say the degree distribution obeys a power law? Discuss.

**Response:**
- [Include log-log plot of degree distribution]
- [Analysis of whether power law is observed]
- [Discussion of findings]

**Question 3:** Plot a histogram for the degrees of the citation network using bins of width $2^n$, for $n=0,1,2, \dots$ (that is, equispaced in logarithmic scale). Do you still stand by your answer to Question 2? Are you more certain now as to whether the citation network can be characterized as scale-free?

**Response:**
- [Include histogram with logarithmic binning]
- [Compare with previous analysis]
- [Discussion of scale-free characterization]
- [Assessment of confidence in conclusions]

---

### 2.3 Pareto Distribution and Estimation of the Power-Law Exponent $\alpha$

**Question 4:** Determine the value of the constant $C$ so that $p(d)$ is a valid pdf.

**Response:**
- [Mathematical derivation]
- [Show that $\int_{d_{\min}}^{\infty} p(d) dd = 1$]
- [Solve for $C$]

**Question 5:** Write a function that implements the aforementioned MLE, given the degree sequence and $d_{\min}$ as inputs. Estimate the power-law exponent for the citation network.

**Response:**
- [Describe MLE implementation]
- [Show function code]
- [Report estimated $\hat{\alpha}$ value (should be around 4)]
- [Discussion of the result]

---

### 2.4 Assortative Mixing and the Modularity Coefficient

**Question 6:** Compute the modularity coefficient for the airport and paper citation networks. You may find the function `networkx.algorithms.community.modularity` handy, in addition to the function `communities_partition` provided.

**Response:**
- [Report modularity coefficient for Cora citation network]
- [Report modularity coefficient for USA airports network]
- [Include relevant code/output]

**Question 7:** What do the respective values tell you about the structure of relational ties established in each of the networks? Do these align with your prior intuitions given the nature and structure of these complex systems?

**Response:**
- [Interpretation of modularity values]
  - Positive values indicate assortative mixing (homophily)
  - Negative values indicate disassortative mixing
- [Discussion for citation network]
  - [Expected behavior: papers cite similar topics]
  - [Whether results match expectations]
- [Discussion for airports network]
  - [Expected behavior: airports connect based on activity levels]
  - [Whether results match expectations]
- [Comparison between the two networks]

---

## 3. Community Detection

### 3.1 Spectral Graph Partitioning

**Question 8:** Implement the spectral graph partitioning algorithm we discussed in class. Your function should partition the vertex set of a given graph $G$ into two groups of given cardinalities $n_1$ and $n_2$, such that the graph cut is (approximately) minimized.

**Response:**
- [Describe algorithm implementation]
- [Show function code]
- [Results on Zachary's Karate Club]
  - [Visualization: ground truth vs. estimated labels]
  - [Adjusted Rand index score]
  - [Fowlkes-Mallows index score]
- [Discussion of performance]

---

### 3.2 Spectral Modularity Maximization

**Question 9:** Implement the spectral modularity maximization algorithm we discussed in class. Your function should partition the vertex set of a given graph $G$ into two groups, such that the modularity of the partition is (approximately) maximized. The function `networkx.modularity_matrix` may be handy to this end.

**Response:**
- [Describe algorithm implementation]
- [Show function code]
- [Results on Zachary's Karate Club]
  - [Visualization: ground truth vs. estimated labels]
  - [Note: should correctly label all vertices except node 8]
  - [Adjusted Rand index score]
  - [Fowlkes-Mallows index score]
- [Comparison with spectral partitioning results]

---

### 3.3 Partitioning a Network of US Political Blogs

**Question 10:** Do you see any qualitative difference when comparing the figures above? Does this examination tell you anything about potential limitations of the spectral modularity maximization approach to network community detection?

**Response:**
- [Results on Political Blogs network]
  - [Visualization: ground truth vs. estimated labels]
  - [Adjusted Rand index score (should be close to 1)]
  - [Fowlkes-Mallows index score (should be close to 1)]
- [Qualitative comparison of visualizations]
- [Discussion of limitations]
  - [When does spectral modularity maximization work well?]
  - [What are its limitations?]
  - [Comparison with spectral partitioning approach]

---

## 4. Optional Exercise (Extra Credit)

### 4.1 Maximum Likelihood Estimator of $\alpha$ in the Pareto Distribution

**Optional Question 1:** Given $n$ independent and identically distributed (i.i.d.) degree observations $d_1,\dots,d_{n}$ from a Pareto distribution, show that the log-likelihood function is given by:
$$\ell_n(\alpha) = n \log (\alpha -1)-n\log d_\text{min} - \alpha \sum_{i=1}^n \log \left(\frac{d_i}{d_\text{min}}\right).$$

**Response:**
- [Mathematical derivation]
- [Step-by-step solution]

**Optional Question 2:** Conclude that the MLE for $\alpha$ is:
$$\hat{\alpha} = 1 + n \left[\sum_{i=1}^n \log \left(\frac{d_i}{d_\text{min}}\right)\right]^{-1}.$$

**Response:**
- [Derivation from log-likelihood]
- [Show maximization process]
- [Final MLE formula]

---

## 5. Conclusion
[Summary of key findings]
[Main insights from the analysis]
[Limitations and future work]

---

## Appendix
[Include all code implementations]
[Additional figures or tables if needed]
