# Agentic Relational Reasoning Plan (Nemotron + Graph Memory)

## 1. Project Goal

Build an **Agentic Relational Reasoning System** using NVIDIA Nemotron as the core reasoning engine. The system must solve complex, multi-hop SQL queries from the BIRD and Spider 2.0 benchmarks by leveraging an **External Context Graph** for memory and schema navigation.

---

## 2. Academic & Competitive Context

- **Target 1:** NVIDIA Nemotron Model Reasoning Challenge  
  - Improve benchmark reasoning using open Nemotron models (e.g., Llama-3.1-Nemotron-70B).
- **Target 2:** ECE442 Final Project (Network Science Analytics)  
  - The **"Network"** is a Context Graph (Heterogeneous Information Network) representing schemas, docs, and tool dependencies.

**Primary Benchmarks:**  
- **Spider 2.0:** Complex enterprise workflows, 1,000+ columns, Snowflake/BigQuery dialects.
- **BIRD-INTERACT:** Multi-turn SQL agentic dialog, where top models currently hit ~22% success (Claude-Opus-4.6).

---

## 3. Technical Architecture

### Reasoning Engine
- **Model:** NVIDIA Nemotron-3 / Llama-3.1-Nemotron

### Memory Layer
- **External Context Graph (Relational Knowledge Graph)**
    - **Nodes:** Tables, Columns, Entities, Documentation Snippets
    - **Edges:** Foreign Keys, Semantic Similarity, Join Logic

**Workflow:**
1. **Schema Pruning:** 
    - Use Context Graph to extract only the relevant table "neighborhoods", overcoming huge context window limits.
2. **Reasoning:** 
    - Nemotron generates a Chain-of-Thought (CoT) using the pruned schema as context.
3. **Action:** 
    - Agent utilizes tool-calling to generate and execute SQL.
4. **Self-Correction:** 
    - Use execution feedback (from DB or tool) to update the graph memory and retry.

---

## 4. Engineering Edge – The "Jump-In" Point

- **Problem:** Spider 2.0 baselines mostly rely on RAG with text or vectors, which lose relational structure/topology.
- **Innovation:** Implement a **Relational Foundation Model (RFM)** approach (cf. Kumo AI)—agent treats the database as a *graph* for **logic-aware retrieval**.

---

## 5. Development Strategy (Cloud-Native)

- **Compute:** 
    - Use NVIDIA NIM APIs for all model inference; *no local GPU required*.
- **Database:** 
    - Use managed Neo4j or NetworkX for the Context Graph layer.
- **Evaluation:** 
    - Integrate NeMo Evaluator SDK for Spider and BIRD suite testing and autoscores.

---
