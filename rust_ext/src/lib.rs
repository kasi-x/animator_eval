//! animetor_eval_core — Rust extension for graph algorithm acceleration.
//!
//! Provides high-performance implementations of:
//! - Betweenness centrality (Brandes' algorithm with rayon parallelism)
//! - Degree centrality
//! - Eigenvector centrality (power iteration)
//! - Collaboration edge aggregation (parallel per-anime)

mod centrality;
mod collaboration;
mod graph;

use pyo3::prelude::*;
use std::collections::HashMap;

use crate::centrality as cent;
use crate::collaboration as collab;
use crate::graph::CsrGraph;

/// Compute approximate betweenness centrality using Brandes' algorithm.
///
/// Args:
///     adjacency: Dict mapping node_id to {neighbor_id: weight}.
///         This is the standard NetworkX adjacency format for undirected graphs
///         (each edge appears in both directions).
///     k: Number of source samples for approximation (None = exact).
///     seed: Random seed for reproducible sampling.
///
/// Returns:
///     Dict mapping node_id to betweenness centrality score.
#[pyfunction]
#[pyo3(signature = (adjacency, k=None, seed=42))]
fn betweenness_centrality_rs(
    adjacency: HashMap<String, HashMap<String, f64>>,
    k: Option<usize>,
    seed: u64,
) -> HashMap<String, f64> {
    let csr = CsrGraph::from_adjacency(adjacency);
    let result = cent::betweenness_centrality(&csr, k, seed);
    result.into_iter().collect()
}

/// Compute approximate betweenness centrality from a compact edge list.
///
/// Memory-efficient alternative to `betweenness_centrality_rs` for large sparse
/// graphs where building a full adjacency dict would cause OOM.
///
/// Args:
///     node_ids: Ordered list of node ID strings (position = node index).
///     edges: List of (u_idx, v_idx, weight) tuples for undirected edges.
///         Each edge should appear once (not both directions).
///     k: Number of source samples (None = exact).
///     seed: Random seed.
///
/// Returns:
///     Dict mapping node_id to betweenness centrality score.
#[pyfunction]
#[pyo3(signature = (node_ids, edges, k=None, seed=42))]
fn betweenness_centrality_from_edges_rs(
    node_ids: Vec<String>,
    edges: Vec<(u32, u32, f64)>,
    k: Option<usize>,
    seed: u64,
) -> HashMap<String, f64> {
    let csr = CsrGraph::from_edges(node_ids, edges);
    let result = cent::betweenness_centrality(&csr, k, seed);
    result.into_iter().collect()
}

/// Compute degree centrality for all nodes.
///
/// Args:
///     adjacency: Dict mapping node_id to {neighbor_id: weight}.
///
/// Returns:
///     Dict mapping node_id to degree centrality (degree / (n-1)).
#[pyfunction]
fn degree_centrality_rs(
    adjacency: HashMap<String, HashMap<String, f64>>,
) -> HashMap<String, f64> {
    let csr = CsrGraph::from_adjacency(adjacency);
    let result = cent::degree_centrality(&csr);
    result.into_iter().collect()
}

/// Compute eigenvector centrality via power iteration.
///
/// Args:
///     adjacency: Dict mapping node_id to {neighbor_id: weight}.
///     max_iter: Maximum iterations for power method.
///     tol: Convergence tolerance.
///
/// Returns:
///     Dict mapping node_id to eigenvector centrality score.
#[pyfunction]
#[pyo3(signature = (adjacency, max_iter=1000, tol=1e-6))]
fn eigenvector_centrality_rs(
    adjacency: HashMap<String, HashMap<String, f64>>,
    max_iter: usize,
    tol: f64,
) -> HashMap<String, f64> {
    let csr = CsrGraph::from_adjacency(adjacency);
    let result = cent::eigenvector_centrality(&csr, max_iter, tol);
    result.into_iter().collect()
}

/// Build collaboration edges from anime staff lists (parallel).
///
/// Args:
///     anime_staff: List of (anime_id, [(person_id, role_weight), ...]).
///         Each anime has a list of staff members with their role weights.
///
/// Returns:
///     List of (person_a, person_b, total_weight, shared_works_count).
///     Person IDs are canonically ordered (min, max).
#[pyfunction]
fn build_collaboration_edges_rs(
    anime_staff: Vec<(String, Vec<(String, f64)>)>,
) -> Vec<(String, String, f64, u32)> {
    collab::build_collaboration_edges(anime_staff)
}

/// Python module definition.
#[pymodule]
fn animetor_eval_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(betweenness_centrality_rs, m)?)?;
    m.add_function(wrap_pyfunction!(betweenness_centrality_from_edges_rs, m)?)?;
    m.add_function(wrap_pyfunction!(degree_centrality_rs, m)?)?;
    m.add_function(wrap_pyfunction!(eigenvector_centrality_rs, m)?)?;
    m.add_function(wrap_pyfunction!(build_collaboration_edges_rs, m)?)?;
    Ok(())
}
