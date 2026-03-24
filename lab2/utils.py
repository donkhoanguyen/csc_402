"""
Utility functions for Lab 2.
Includes a replacement for graspologic.utils.remap_labels in case of dependency issues.
"""

import numpy as np
from sklearn.metrics import adjusted_rand_score


def remap_labels(true_labels, pred_labels):
    """
    Remap predicted labels to match the ground truth labels as closely as possible.
    
    This function finds the best permutation of predicted labels that maximizes
    agreement with the true labels.
    
    Parameters:
    -----------
    true_labels : array-like
        Ground truth labels
    pred_labels : array-like
        Predicted labels to remap
        
    Returns:
    --------
    remapped_labels : numpy array
        Remapped predicted labels
    """
    true_labels = np.array(true_labels)
    pred_labels = np.array(pred_labels)
    
    # Get unique labels
    true_unique = np.unique(true_labels)
    pred_unique = np.unique(pred_labels)
    
    # If same number of unique labels, try to map them
    if len(true_unique) == len(pred_unique):
        # Try all permutations to find best match
        from itertools import permutations
        
        best_score = -1
        best_mapping = None
        
        for perm in permutations(pred_unique):
            mapping = dict(zip(pred_unique, perm))
            remapped = np.array([mapping[label] for label in pred_labels])
            score = adjusted_rand_score(true_labels, remapped)
            if score > best_score:
                best_score = score
                best_mapping = mapping
        
        if best_mapping:
            return np.array([best_mapping[label] for label in pred_labels])
    
    # Fallback: simple mapping based on label frequency
    # Map most common predicted label to most common true label, etc.
    from collections import Counter
    true_counts = Counter(true_labels)
    pred_counts = Counter(pred_labels)
    
    true_sorted = sorted(true_counts.items(), key=lambda x: x[1], reverse=True)
    pred_sorted = sorted(pred_counts.items(), key=lambda x: x[1], reverse=True)
    
    mapping = {}
    for i, (pred_label, _) in enumerate(pred_sorted):
        if i < len(true_sorted):
            mapping[pred_label] = true_sorted[i][0]
        else:
            # If more predicted labels than true labels, map to first true label
            mapping[pred_label] = true_sorted[0][0]
    
    return np.array([mapping[label] for label in pred_labels])
