"""Sparse collaboration graph — memory-efficient alternative to NetworkX.

NetworkX stores each edge as a nested dict (~1.6KB/edge).
For 43.9M edges this costs ~70GB.

This module provides SparseCollaborationGraph which stores adjacency as
scipy.sparse CSR matrices (~16 bytes/edge), reducing memory by ~100x.

The class exposes a NetworkX-compatible subset of methods so it can be
used as a drop-in replacement for collaboration_graph in the pipeline.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterator

import numpy as np
import scipy.sparse as sp
import structlog

logger = structlog.get_logger()


class SparseCollaborationGraph:
    """Memory-efficient collaboration graph using scipy.sparse.

    Supports the NetworkX Graph API subset actually used by the pipeline:
      - nodes(), number_of_nodes()
      - edges(data=True), number_of_edges()
      - neighbors(node), degree(), degree(node)
      - subgraph(nodes)
      - get_edge_data(u, v)
      - __getitem__ for graph[u][v] access
      - Node attributes via .node_attrs[node_id]

    Internal storage:
      - weight_matrix: CSR sparse matrix (float64) for edge weights
      - shared_works_matrix: CSR sparse matrix (int32) for shared work counts
      - node_ids: list[str] mapping matrix index → node_id
      - node_to_idx: dict[str, int] mapping node_id → matrix index
      - node_attrs: dict[str, dict] for node attributes (name, etc.)
    """

    def __init__(
        self,
        edge_data: dict[tuple[str, str], dict[str, float]],
        node_attrs: dict[str, dict[str, Any]] | None = None,
    ):
        """Build sparse graph from edge data.

        Args:
            edge_data: {(pid_a, pid_b): {"weight": float, "shared_works": int}}
            node_attrs: {node_id: {"name": ..., "name_ja": ..., "name_en": ...}}
        """
        # Collect all node IDs
        node_set: set[str] = set()
        if node_attrs:
            node_set.update(node_attrs.keys())
        for a, b in edge_data:
            node_set.add(a)
            node_set.add(b)

        self.node_ids = sorted(node_set)
        self.node_to_idx = {nid: i for i, nid in enumerate(self.node_ids)}
        self.node_attrs: dict[str, dict[str, Any]] = node_attrs or {}
        n = len(self.node_ids)

        if not edge_data:
            self.weight_matrix = sp.csr_matrix((n, n), dtype=np.float64)
            self.shared_works_matrix = sp.csr_matrix((n, n), dtype=np.int32)
            self._n_edges = 0
            return

        # Build COO arrays for sparse construction
        rows = np.empty(len(edge_data) * 2, dtype=np.int32)
        cols = np.empty(len(edge_data) * 2, dtype=np.int32)
        weights = np.empty(len(edge_data) * 2, dtype=np.float64)
        shared = np.empty(len(edge_data) * 2, dtype=np.int32)

        idx = 0
        for (a, b), attrs in edge_data.items():
            ia = self.node_to_idx[a]
            ib = self.node_to_idx[b]
            w = attrs.get("weight", 0.0)
            sw = int(attrs.get("shared_works", 0))
            # Symmetric: both directions
            rows[idx] = ia
            cols[idx] = ib
            weights[idx] = w
            shared[idx] = sw
            idx += 1
            rows[idx] = ib
            cols[idx] = ia
            weights[idx] = w
            shared[idx] = sw
            idx += 1

        self.weight_matrix = sp.csr_matrix(
            (weights[:idx], (rows[:idx], cols[:idx])), shape=(n, n), dtype=np.float64
        )
        self.shared_works_matrix = sp.csr_matrix(
            (shared[:idx], (rows[:idx], cols[:idx])), shape=(n, n), dtype=np.int32
        )
        self._n_edges = len(edge_data)  # undirected edge count

        logger.info(
            "sparse_graph_built",
            nodes=n,
            edges=self._n_edges,
            memory_mb=round(
                (
                    self.weight_matrix.data.nbytes
                    + self.weight_matrix.indices.nbytes
                    + self.weight_matrix.indptr.nbytes
                    + self.shared_works_matrix.data.nbytes
                    + self.shared_works_matrix.indices.nbytes
                    + self.shared_works_matrix.indptr.nbytes
                )
                / 1024
                / 1024,
                1,
            ),
        )

    def __len__(self) -> int:
        return len(self.node_ids)

    def number_of_nodes(self) -> int:
        return len(self.node_ids)

    def number_of_edges(self) -> int:
        return self._n_edges

    def nodes(self) -> list[str]:
        return self.node_ids

    def neighbors(self, node: str) -> list[str]:
        """Return list of neighbor node IDs."""
        idx = self.node_to_idx.get(node)
        if idx is None:
            return []
        row = self.weight_matrix.getrow(idx)
        return [self.node_ids[j] for j in row.indices]

    def degree(self, node: str | None = None) -> dict[str, int] | int:
        """Return degree(s). If node is given, return int. Otherwise dict."""
        if node is not None:
            idx = self.node_to_idx.get(node)
            if idx is None:
                return 0
            return int(self.weight_matrix.getrow(idx).nnz)
        # Return dict for all nodes
        degrees = np.diff(self.weight_matrix.indptr)
        return {self.node_ids[i]: int(degrees[i]) for i in range(len(self.node_ids))}

    def edges(self, data: bool = False) -> Iterator:
        """Iterate over edges. If data=True, yields (u, v, {"weight": ..., "shared_works": ...})."""
        coo = sp.triu(self.weight_matrix, k=1).tocoo()
        sw_coo = sp.triu(self.shared_works_matrix, k=1).tocoo()

        # Build shared_works lookup
        sw_dict: dict[tuple[int, int], int] = {}
        for i, j, v in zip(sw_coo.row, sw_coo.col, sw_coo.data):
            sw_dict[(int(i), int(j))] = int(v)

        for i, j, w in zip(coo.row, coo.col, coo.data):
            u = self.node_ids[int(i)]
            v_node = self.node_ids[int(j)]
            if data:
                yield (
                    u,
                    v_node,
                    {
                        "weight": float(w),
                        "shared_works": sw_dict.get((int(i), int(j)), 0),
                    },
                )
            else:
                yield u, v_node

    def get_edge_data(self, u: str, v: str) -> dict[str, Any] | None:
        """Get edge attributes between u and v."""
        iu = self.node_to_idx.get(u)
        iv = self.node_to_idx.get(v)
        if iu is None or iv is None:
            return None
        w = self.weight_matrix[iu, iv]
        if w == 0:
            return None
        sw = self.shared_works_matrix[iu, iv]
        return {"weight": float(w), "shared_works": int(sw)}

    def subgraph(self, nodes) -> "SparseCollaborationGraph":
        """Return a new SparseCollaborationGraph induced by the given nodes."""
        node_list = sorted(set(nodes) & set(self.node_to_idx.keys()))
        indices = [self.node_to_idx[n] for n in node_list]

        # Extract submatrix
        idx_arr = np.array(indices)
        sub_w = self.weight_matrix[np.ix_(idx_arr, idx_arr)]
        sub_sw = self.shared_works_matrix[np.ix_(idx_arr, idx_arr)]

        # Build new graph directly
        sg = SparseCollaborationGraph.__new__(SparseCollaborationGraph)
        sg.node_ids = node_list
        sg.node_to_idx = {nid: i for i, nid in enumerate(node_list)}
        sg.node_attrs = {n: self.node_attrs.get(n, {}) for n in node_list}
        sg.weight_matrix = sp.csr_matrix(sub_w)
        sg.shared_works_matrix = sp.csr_matrix(sub_sw)
        sg._n_edges = sp.triu(sg.weight_matrix, k=1).nnz
        return sg

    def __getitem__(self, node: str) -> "_NodeView":
        """Support graph[u][v] access pattern."""
        return _NodeView(self, node)

    def __contains__(self, node: str) -> bool:
        return node in self.node_to_idx

    def __iter__(self):
        return iter(self.node_ids)

    def has_edge(self, u: str, v: str) -> bool:
        iu = self.node_to_idx.get(u)
        iv = self.node_to_idx.get(v)
        if iu is None or iv is None:
            return False
        return self.weight_matrix[iu, iv] != 0

    def add_node(self, node: str, **attrs: Any) -> None:
        """Add a node (no-op if already exists)."""
        if node not in self.node_to_idx:
            idx = len(self.node_ids)
            self.node_ids.append(node)
            self.node_to_idx[node] = idx
            # Resize matrices
            n = len(self.node_ids)
            self.weight_matrix.resize((n, n))
            self.shared_works_matrix.resize((n, n))
        if attrs:
            self.node_attrs[node] = attrs

    def community_detection_lpa(self, seed: int = 42) -> dict[str, int]:
        """Label Propagation Algorithm for community detection on sparse matrix.

        Returns: {node_id: community_id}
        """
        rng = np.random.RandomState(seed)
        n = len(self.node_ids)
        labels = np.arange(n, dtype=np.int32)

        for iteration in range(50):  # max iterations
            changed = False
            order = rng.permutation(n)
            for i in order:
                row = self.weight_matrix.getrow(i)
                if row.nnz == 0:
                    continue
                # Count weighted votes per neighbor label
                neighbor_labels = labels[row.indices]
                weights = row.data
                vote: dict[int, float] = {}
                for lbl, w in zip(neighbor_labels, weights):
                    vote[int(lbl)] = vote.get(int(lbl), 0.0) + w
                if vote:
                    best_label = max(vote, key=vote.get)  # type: ignore[arg-type]
                    if labels[i] != best_label:
                        labels[i] = best_label
                        changed = True
            if not changed:
                break

        return {self.node_ids[i]: int(labels[i]) for i in range(n)}


class _NodeView:
    """Helper for graph[u][v] access pattern.

    Supports both graph[u][v] and graph[u].items() for NetworkX compatibility.
    """

    def __init__(self, graph: SparseCollaborationGraph, node: str):
        self._graph = graph
        self._node = node

    def __getitem__(self, other: str) -> dict[str, Any]:
        data = self._graph.get_edge_data(self._node, other)
        if data is None:
            raise KeyError(f"No edge between {self._node} and {other}")
        return data

    def items(self):
        """Yield (neighbor, edge_data) pairs — matches NetworkX adjacency dict."""
        for neighbor in self._graph.neighbors(self._node):
            yield neighbor, self._graph.get_edge_data(self._node, neighbor)
