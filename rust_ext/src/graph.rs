//! Graph representation using Compressed Sparse Row (CSR) format.
//!
//! CSR provides cache-friendly traversal for graph algorithms.
//! The graph is undirected — each edge (u,v) is stored in both directions.

use ahash::AHashMap;
use std::collections::HashMap;

/// Compressed Sparse Row graph for cache-friendly neighbor iteration.
///
/// For node `i`, its neighbors are stored in `neighbors[offsets[i]..offsets[i+1]]`
/// with corresponding weights in `weights[offsets[i]..offsets[i+1]]`.
pub struct CsrGraph {
    pub n_nodes: usize,
    /// Length = n_nodes + 1. offsets[i]..offsets[i+1] gives neighbor range for node i.
    pub offsets: Vec<usize>,
    /// Flat array of neighbor indices, parallel to `weights`.
    pub neighbors: Vec<u32>,
    /// Edge weights, parallel to `neighbors`.
    pub weights: Vec<f64>,
    /// Index → person_id string mapping.
    pub node_ids: Vec<String>,
    /// person_id → index mapping (fast lookup).
    pub id_to_idx: AHashMap<String, u32>,
}

impl CsrGraph {
    /// Build CSR graph from a compact edge list.
    ///
    /// - `node_ids`: ordered list of node ID strings (index = node index)
    /// - `edges`: list of (u_idx, v_idx, weight) for undirected edges
    ///   Each edge is stored once here; both directions are added internally.
    ///
    /// Memory: O(V + E) vs O(V + E) for adjacency dict, but with much lower
    /// constant factor — no Python string allocation per neighbor entry.
    pub fn from_edges(node_ids: Vec<String>, edges: Vec<(u32, u32, f64)>) -> Self {
        let n_nodes = node_ids.len();
        let id_to_idx: AHashMap<String, u32> = node_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (id.clone(), i as u32))
            .collect();

        // Count degrees (undirected: each edge contributes to both endpoints)
        let mut degrees = vec![0usize; n_nodes];
        for &(u, v, _) in &edges {
            degrees[u as usize] += 1;
            degrees[v as usize] += 1;
        }

        // Build CSR offsets
        let mut offsets = vec![0usize; n_nodes + 1];
        for i in 0..n_nodes {
            offsets[i + 1] = offsets[i] + degrees[i];
        }

        let total = offsets[n_nodes];
        let mut neighbors = vec![0u32; total];
        let mut weights = vec![0.0f64; total];
        let mut pos = offsets[..n_nodes].to_vec(); // write cursor per node

        for &(u, v, w) in &edges {
            let (u, v) = (u as usize, v as usize);
            neighbors[pos[u]] = v as u32;
            weights[pos[u]] = w;
            pos[u] += 1;
            neighbors[pos[v]] = u as u32;
            weights[pos[v]] = w;
            pos[v] += 1;
        }

        CsrGraph {
            n_nodes,
            offsets,
            neighbors,
            weights,
            node_ids,
            id_to_idx,
        }
    }

    /// Build CSR graph from a Python adjacency dict: `{node_id: {neighbor_id: weight}}`.
    ///
    /// The input represents an undirected graph — each edge appears in both directions
    /// in the adjacency dict (NetworkX convention).
    pub fn from_adjacency(adj: HashMap<String, HashMap<String, f64>>) -> Self {
        let n_nodes = adj.len();

        // Assign stable indices to all nodes
        let mut node_ids: Vec<String> = adj.keys().cloned().collect();
        node_ids.sort(); // deterministic ordering
        let id_to_idx: AHashMap<String, u32> = node_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (id.clone(), i as u32))
            .collect();

        // Build CSR arrays
        let mut offsets = Vec::with_capacity(n_nodes + 1);
        let mut neighbors = Vec::new();
        let mut weights = Vec::new();

        offsets.push(0);

        for node_id in &node_ids {
            if let Some(nbrs) = adj.get(node_id) {
                // Sort neighbors for deterministic output
                let mut nbr_list: Vec<(&String, &f64)> = nbrs.iter().collect();
                nbr_list.sort_by_key(|(k, _)| *k);

                for (nbr_id, &w) in nbr_list {
                    if let Some(&idx) = id_to_idx.get(nbr_id) {
                        neighbors.push(idx);
                        weights.push(w);
                    }
                }
            }
            offsets.push(neighbors.len());
        }

        CsrGraph {
            n_nodes,
            offsets,
            neighbors,
            weights,
            node_ids,
            id_to_idx,
        }
    }

    /// Get degree of node `idx`.
    #[inline]
    pub fn degree(&self, idx: usize) -> usize {
        self.offsets[idx + 1] - self.offsets[idx]
    }

    /// Iterate over (neighbor_idx, weight) for node `idx`.
    #[inline]
    pub fn neighbors(&self, idx: usize) -> &[u32] {
        &self.neighbors[self.offsets[idx]..self.offsets[idx + 1]]
    }

    /// Get weight slice for node `idx`.
    #[inline]
    pub fn neighbor_weights(&self, idx: usize) -> &[f64] {
        &self.weights[self.offsets[idx]..self.offsets[idx + 1]]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csr_from_adjacency() {
        let mut adj: HashMap<String, HashMap<String, f64>> = HashMap::new();

        let mut a_nbrs = HashMap::new();
        a_nbrs.insert("B".to_string(), 1.0);
        a_nbrs.insert("C".to_string(), 2.0);
        adj.insert("A".to_string(), a_nbrs);

        let mut b_nbrs = HashMap::new();
        b_nbrs.insert("A".to_string(), 1.0);
        adj.insert("B".to_string(), b_nbrs);

        let mut c_nbrs = HashMap::new();
        c_nbrs.insert("A".to_string(), 2.0);
        adj.insert("C".to_string(), c_nbrs);

        let g = CsrGraph::from_adjacency(adj);

        assert_eq!(g.n_nodes, 3);
        assert_eq!(g.node_ids, vec!["A", "B", "C"]);

        // Node A has 2 neighbors (B, C)
        let a_idx = g.id_to_idx["A"] as usize;
        assert_eq!(g.degree(a_idx), 2);

        // Node B has 1 neighbor (A)
        let b_idx = g.id_to_idx["B"] as usize;
        assert_eq!(g.degree(b_idx), 1);
    }
}
