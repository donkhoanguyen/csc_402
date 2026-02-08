![Network graph of emails exchanged during the whole time period](images/network_graph.png)
## Part 1: Network Analysis
1. Number of directed edges (arcs): 3007

2. Number of undirected edges: 2097
3. Number of mutual arcs: 910

4. Number of nodes with d_in = 0: 3
   Employee names: Vince Kaminski      Manager            Risk Management Head, Mary Fischer        Employee, xxx

5. Number of nodes with d_out = 0: 9
   Employee names: xxx, Michelle Lokay         Employee           Administrative Asisstant, Mark Haedicke          Managing Director  Legal Department, Mark Taylor            Employee, Vince Kaminski      Manager            Risk Management Head, xxx, Mary Fischer        Employee, xxx, xxx

6. Employees contacted by ≥30 employees: 13

![Q6: Nodes contacted by ≥30 employees (red)](images/q6_contacted_by_30.png)
7. Employees who contacted ≥30 employees: 24

![Q7: Nodes that contacted ≥30 employees (red)](images/q7_contacted_30.png)
![Q8: Degree histograms](images/q8_degree_histograms.png)
## Part 2: Changes in the Network Graph
9. Centrality over time: two measures (e.g. degree centrality, betweenness)
**Who was most central?**
- **Entire graph:** Most central by degree: **Stephanie Panus        Employee**. Most central by betweenness: **Chris Germany       Employee**.
- **Per week:**
Frequency of individuals as most central by degree over weeks:
- Mark Taylor            Employee: 44 weeks
- Tana Jones             N/A: 30 weeks
- Louise Kitchen      President          Enron Online: 11 weeks
- John Lavorato          CEO                Enron America: 11 weeks
- xxx: 10 weeks
- Mark Haedicke          Managing Director  Legal Department: 8 weeks
- James Steffes          Vice President     Government Affairs: 8 weeks
- Michael Grigsby         Manager: 8 weeks
- Chris Germany       Employee: 6 weeks
- David Delainey         CEO                Enron North America and Enron Enery Services: 5 weeks
- Shelley Corman         Vice President     Regulatory Affairs: 4 weeks
- Jeff Dasovich          Employee           Government Relation Executive: 4 weeks
- Elizabeth Sager        Employee: 3 weeks
- Richard Sanders     Vice President    Enron WholeSale Services: 3 weeks
- Scott Neal              Vice President: 3 weeks
- Sally Beck          Employee           Chief Operating Officer: 2 weeks
- Steven Kean            Vice President     Vice President & Chief of Staff: 2 weeks
- Richard Shapiro        Vice President     Regulatory Affairs: 2 weeks
- Kam Keiser          Employee: 2 weeks
- Kimberly Watson        N/A: 2 weeks
- Michelle Lokay         Employee           Administrative Asisstant: 2 weeks
- Susan Bailey           N/A: 2 weeks
- Chris Dorland       Manager: 1 weeks
- Matthew Lenhart        Employee: 1 weeks
- Vince Kaminski      Manager            Risk Management Head: 1 weeks
- Jeffrey Shankman       President          Enron Global Mkts: 1 weeks
- Kim Ward               N/A: 1 weeks
- Jeffery Skilling    CEO: 1 weeks
- Kenneth Lay         CEO: 1 weeks
- Phillip Love        N/A: 1 weeks
- Kevin Presto            Vice President: 1 weeks
- Lindy Donoho           Employee: 1 weeks
- Susan Scott            N/A: 1 weeks
- Stephanie Panus        Employee: 1 weeks

![Top centrality scores each week (dual axis)](images/most_central_employee_each_week_dual_axis.png)
10. Graph-level statistics over time (identify Enron Online launch, Cooper CEO, etc.)
![Graph-level statistics over time](images/graph_level_statistics_over_time.png)
11. Compute and print the graph Laplacian matrix L for Karate club (L = D - A):
tensor([[16., -1., -1.,  ..., -1.,  0.,  0.],
        [-1.,  9., -1.,  ...,  0.,  0.,  0.],
        [-1., -1., 10.,  ...,  0., -1.,  0.],
        ...,
        [-1.,  0.,  0.,  ...,  6., -1., -1.],
        [ 0.,  0., -1.,  ..., -1., 12., -1.],
        [ 0.,  0.,  0.,  ..., -1., -1., 17.]])
12. Zero eigenvalue and ones vector
   L @ ones ≈ 0: True
   First eigenvector proportional to ones: True
13. Symmetric and positive semidefinite
   is L symmetric: True
   Min eigenvalue ≥ 0: True
14. Signed incidence matrix B_tilde: L = B_tilde @ B_tilde.T
   L = B_tilde @ B_tilde.T: True
