# Lab 2 Environment Setup

## Virtual Environment Created

A virtual environment has been created in `.venv` with all required packages installed.

## Using the Environment

### Option 1: Use in Jupyter Notebook (Recommended)

The kernel has been registered and is available as **"Python (lab2)"**.

1. Open your notebook (`Lab2_ECE442.ipynb`)
2. In Jupyter, go to **Kernel → Change Kernel → Python (lab2)**
3. The notebook will now use the lab2 virtual environment

### Option 2: Activate in Terminal

```bash
cd lab2
source .venv/bin/activate
```

Then run Python scripts or start Jupyter:
```bash
jupyter notebook
# or
jupyter lab
```

### Option 3: Use with VS Code / Cursor

1. Open the `lab2` folder in VS Code/Cursor
2. Select the Python interpreter: `Cmd+Shift+P` → "Python: Select Interpreter"
3. Choose `.venv/bin/python`

## Installed Packages

- networkx
- numpy
- matplotlib
- torch
- torch-geometric
- scikit-learn
- pandas
- jupyter / ipykernel
- graspologic (installed without full dependencies)

## Note on graspologic

The `graspologic` package was installed without full dependencies due to Python 3.12 compatibility issues. The main function used (`remap_labels`) should work, but if you encounter issues, you can use the replacement function in `utils.py`:

```python
# Instead of:
from graspologic.utils import remap_labels

# Use:
from utils import remap_labels
```

## Verifying Installation

Run this in a notebook cell to verify:
```python
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import torch
from torch_geometric.datasets import Planetoid
from sklearn.metrics import adjusted_rand_score, fowlkes_mallows_score

print("All packages imported successfully!")
```
