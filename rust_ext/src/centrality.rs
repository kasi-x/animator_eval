//! Centrality metrics: betweenness (Brandes'), degree, eigenvector.
//!
//! Betweenness uses weighted Dijkstra-based shortest paths with k-sample
//! approximation and rayon parallelism for massive speedups on large graphs.

use crate::graph::CsrGraph;
use ahash::AHashMap;
use ordered_float::OrderedFloat;
use rayon::prelude::*;
use std::collections::BinaryHeap;

/// Single-source Dijkstra + dependency accumulation (Brandes' algorithm core).
///
/// Returns a vector of dependency values δ[v] for all nodes v,
/// representing v's contribution to shortest paths from source `s`.
fn single_source_dijkstra_brandes(graph: &CsrGraph, s: usize) -> Vec<f64> {
    let n = graph.n_nodes;
    let mut dist = vec![f64::INFINITY; n];
    let mut sigma = vec![0.0_f64; n]; // number of shortest paths
    let mut predecessors: Vec<Vec<u32>> = vec![vec![]; n];
    let mut stack: Vec<u32> = Vec::with_capacity(n);

    dist[s] = 0.0;
    sigma[s] = 1.0;

    // Min-heap: (distance, node_idx)
    let mut heap: BinaryHeap<(std::cmp::Reverse<OrderedFloat<f64>>, u32)> = BinaryHeap::new();
    heap.push((std::cmp::Reverse(OrderedFloat(0.0)), s as u32));

    while let Some((std::cmp::Reverse(OrderedFloat(d_u)), u)) = heap.pop() {
        let u = u as usize;
        if d_u > dist[u] {
            continue; // stale entry
        }
        stack.push(u as u32);

        let nbrs = graph.neighbors(u);
        let wts = graph.neighbor_weights(u);

        for (i, &v) in nbrs.iter().enumerate() {
            let v = v as usize;
            // Use weight directly as distance (matches NetworkX convention)
            let edge_cost = if wts[i] > 0.0 { wts[i] } else { 1.0 };
            let new_dist = dist[u] + edge_cost;

            if new_dist < dist[v] {
                dist[v] = new_dist;
                sigma[v] = sigma[u];
                predecessors[v].clear();
                predecessors[v].push(u as u32);
                heap.push((std::cmp::Reverse(OrderedFloat(new_dist)), v as u32));
            } else if (new_dist - dist[v]).abs() < 1e-12 {
                // Equal-length path
                sigma[v] += sigma[u];
                predecessors[v].push(u as u32);
            }
        }
    }

    // Back-propagation of dependencies
    let mut delta = vec![0.0_f64; n];

    while let Some(w) = stack.pop() {
        let w = w as usize;
        if sigma[w] == 0.0 {
            continue;
        }
        let coeff = (1.0 + delta[w]) / sigma[w];
        for &v in &predecessors[w] {
            let v = v as usize;
            delta[v] += sigma[v] * coeff;
        }
    }

    // Source node has 0 dependency on itself
    delta[s] = 0.0;
    delta
}

/// Approximate betweenness centrality using Brandes' algorithm.
///
/// - `k`: number of source samples (None = exact, all nodes)
/// - `seed`: random seed for reproducible sampling
///
/// Returns: HashMap<String, f64> (node_id → betweenness score)
pub fn betweenness_centrality(
    graph: &CsrGraph,
    k: Option<usize>,
    seed: u64,
) -> AHashMap<String, f64> {
    let n = graph.n_nodes;
    if n == 0 {
        return AHashMap::new();
    }

    // Select source nodes
    let sources: Vec<usize> = match k {
        Some(k) if k < n => {
            // Deterministic sampling using LCG
            let mut rng_state = seed;
            let mut indices: Vec<usize> = (0..n).collect();
            // Fisher-Yates shuffle (first k elements)
            for i in 0..k.min(n) {
                // LCG: state = (a * state + c) mod m
                rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let j = i + (rng_state as usize % (n - i));
                indices.swap(i, j);
            }
            indices.truncate(k);
            indices
        }
        _ => (0..n).collect(),
    };

    let num_sources = sources.len();

    // Parallel computation: each source runs independently
    let per_source_deltas: Vec<Vec<f64>> = sources
        .par_iter()
        .map(|&s| single_source_dijkstra_brandes(graph, s))
        .collect();

    // Sum up all deltas
    let mut betweenness = vec![0.0_f64; n];
    for delta in &per_source_deltas {
        for (i, &d) in delta.iter().enumerate() {
            betweenness[i] += d;
        }
    }

    // Normalization matching NetworkX (normalized=True, undirected):
    //   scale = 1/((n-1)*(n-2)) * (n/k for approximate)
    // Note: no extra /2 — the double-counting in undirected accumulation
    // cancels with the (n-1)(n-2) vs (n-1)(n-2)/2 normalization.
    let norm = if n > 2 {
        1.0 / ((n as f64 - 1.0) * (n as f64 - 2.0))
    } else {
        1.0
    };
    let scale = if num_sources < n {
        norm * (n as f64) / (num_sources as f64)
    } else {
        norm
    };

    let mut result = AHashMap::with_capacity(n);
    for (i, &b) in betweenness.iter().enumerate() {
        result.insert(graph.node_ids[i].clone(), b * scale);
    }

    result
}

/// Degree centrality: degree(v) / (n - 1).
pub fn degree_centrality(graph: &CsrGraph) -> AHashMap<String, f64> {
    let n = graph.n_nodes;
    if n <= 1 {
        return graph
            .node_ids
            .iter()
            .map(|id| (id.clone(), 0.0))
            .collect();
    }

    let denom = (n - 1) as f64;
    graph
        .node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), graph.degree(i) as f64 / denom))
        .collect()
}

/// Eigenvector centrality via power iteration.
///
/// Uses the weighted adjacency matrix. Returns normalized eigenvector
/// corresponding to the largest eigenvalue.
pub fn eigenvector_centrality(
    graph: &CsrGraph,
    max_iter: usize,
    tol: f64,
) -> AHashMap<String, f64> {
    let n = graph.n_nodes;
    if n == 0 {
        return AHashMap::new();
    }

    // Initialize with uniform values
    let init_val = 1.0 / (n as f64).sqrt();
    let mut x = vec![init_val; n];
    let mut x_new = vec![0.0_f64; n];

    for _iter in 0..max_iter {
        // Matrix-vector multiply: x_new[i] = sum(w[i][j] * x[j])
        for i in 0..n {
            let mut sum = 0.0;
            let nbrs = graph.neighbors(i);
            let wts = graph.neighbor_weights(i);
            for (k, &j) in nbrs.iter().enumerate() {
                sum += wts[k] * x[j as usize];
            }
            x_new[i] = sum;
        }

        // Normalize
        let norm: f64 = x_new.iter().map(|v| v * v).sum::<f64>().sqrt();
        if norm == 0.0 {
            break;
        }
        for v in &mut x_new {
            *v /= norm;
        }

        // Check convergence
        let diff: f64 = x
            .iter()
            .zip(x_new.iter())
            .map(|(a, b)| (a - b).abs())
            .sum();

        std::mem::swap(&mut x, &mut x_new);

        if diff < tol {
            break;
        }
    }

    graph
        .node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), x[i]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_triangle() -> CsrGraph {
        // A -- B -- C, A -- C (triangle with uniform weights)
        let mut adj: HashMap<String, HashMap<String, f64>> = HashMap::new();

        let mut a = HashMap::new();
        a.insert("B".to_string(), 1.0);
        a.insert("C".to_string(), 1.0);
        adj.insert("A".to_string(), a);

        let mut b = HashMap::new();
        b.insert("A".to_string(), 1.0);
        b.insert("C".to_string(), 1.0);
        adj.insert("B".to_string(), b);

        let mut c = HashMap::new();
        c.insert("A".to_string(), 1.0);
        c.insert("B".to_string(), 1.0);
        adj.insert("C".to_string(), c);

        CsrGraph::from_adjacency(adj)
    }

    #[test]
    fn test_betweenness_triangle() {
        let g = make_triangle();
        let bc = betweenness_centrality(&g, None, 42);
        // In a triangle, all betweenness = 0.0
        for (_, &v) in bc.iter() {
            assert!(v.abs() < 1e-10, "Triangle betweenness should be 0, got {v}");
        }
    }

    #[test]
    fn test_betweenness_path() {
        // A -- B -- C (path graph, B is bridge)
        let mut adj: HashMap<String, HashMap<String, f64>> = HashMap::new();

        let mut a = HashMap::new();
        a.insert("B".to_string(), 1.0);
        adj.insert("A".to_string(), a);

        let mut b = HashMap::new();
        b.insert("A".to_string(), 1.0);
        b.insert("C".to_string(), 1.0);
        adj.insert("B".to_string(), b);

        let mut c = HashMap::new();
        c.insert("B".to_string(), 1.0);
        adj.insert("C".to_string(), c);

        let g = CsrGraph::from_adjacency(adj);
        let bc = betweenness_centrality(&g, None, 42);

        // B should have highest betweenness (1.0 for 3-node path)
        assert!(bc["B"] > bc["A"]);
        assert!(bc["B"] > bc["C"]);
    }

    #[test]
    fn test_degree_centrality() {
        let g = make_triangle();
        let dc = degree_centrality(&g);
        // All nodes in a triangle have degree 2, centrality = 2/2 = 1.0
        for (_, &v) in dc.iter() {
            assert!((v - 1.0).abs() < 1e-10);
        }
    }

    #[test]
    fn test_eigenvector_centrality_triangle() {
        let g = make_triangle();
        let ec = eigenvector_centrality(&g, 1000, 1e-6);
        // Symmetric graph → all eigenvector values should be equal
        let vals: Vec<f64> = ec.values().copied().collect();
        let first = vals[0];
        for &v in &vals[1..] {
            assert!((v - first).abs() < 1e-6);
        }
    }
}
