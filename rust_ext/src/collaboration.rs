//! Parallel collaboration edge aggregation.
//!
//! For each anime, generates all person pairs and accumulates edge weights.
//! Uses rayon for per-anime parallelism and merges results into a single map.

use ahash::AHashMap;
use rayon::prelude::*;

/// A staff entry for one anime: (person_id, role_weight).
pub type StaffEntry = (String, f64);

/// An anime's staff list: (anime_id, vec of staff entries).
pub type AnimeStaff = (String, Vec<StaffEntry>);

/// Output edge: (person_a, person_b, accumulated_weight, shared_works_count).
pub type EdgeResult = (String, String, f64, u32);

/// Compute all collaboration edges from anime staff lists.
///
/// For each anime, generates all person pairs and computes edge weight
/// as (weight_a + weight_b) / 2. Person pairs are canonicalized (min, max)
/// for consistent deduplication.
///
/// Returns: Vec<(person_a, person_b, total_weight, shared_works)>
pub fn build_collaboration_edges(anime_staff: Vec<AnimeStaff>) -> Vec<EdgeResult> {
    // Phase 1: Parallel per-anime pair generation
    let per_anime_edges: Vec<Vec<(String, String, f64)>> = anime_staff
        .par_iter()
        .map(|(_anime_id, staff)| {
            let mut pairs = Vec::new();
            for i in 0..staff.len() {
                let (ref pid_a, w_a) = staff[i];
                for j in (i + 1)..staff.len() {
                    let (ref pid_b, w_b) = staff[j];
                    if pid_a == pid_b {
                        continue;
                    }
                    // Canonical ordering
                    let (a, b) = if pid_a < pid_b {
                        (pid_a.clone(), pid_b.clone())
                    } else {
                        (pid_b.clone(), pid_a.clone())
                    };
                    let edge_weight = (w_a + w_b) / 2.0;
                    pairs.push((a, b, edge_weight));
                }
            }
            pairs
        })
        .collect();

    // Phase 2: Merge all edges into a single map
    let mut edge_map: AHashMap<(String, String), (f64, u32)> = AHashMap::new();

    for pairs in per_anime_edges {
        for (a, b, w) in pairs {
            let entry = edge_map.entry((a, b)).or_insert((0.0, 0));
            entry.0 += w;
            entry.1 += 1;
        }
    }

    // Phase 3: Convert to output format
    edge_map
        .into_iter()
        .map(|((a, b), (w, count))| (a, b, w, count))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_edges_single_anime() {
        let staff = vec![(
            "anime_1".to_string(),
            vec![
                ("A".to_string(), 2.0),
                ("B".to_string(), 1.0),
                ("C".to_string(), 3.0),
            ],
        )];

        let edges = build_collaboration_edges(staff);
        assert_eq!(edges.len(), 3); // 3 pairs: AB, AC, BC

        let edge_map: AHashMap<(String, String), (f64, u32)> = edges
            .into_iter()
            .map(|(a, b, w, c)| ((a, b), (w, c)))
            .collect();

        // A-B: (2.0 + 1.0) / 2 = 1.5
        let ab = &edge_map[&("A".to_string(), "B".to_string())];
        assert!((ab.0 - 1.5).abs() < 1e-10);
        assert_eq!(ab.1, 1);

        // A-C: (2.0 + 3.0) / 2 = 2.5
        let ac = &edge_map[&("A".to_string(), "C".to_string())];
        assert!((ac.0 - 2.5).abs() < 1e-10);
    }

    #[test]
    fn test_build_edges_multiple_anime() {
        let staff = vec![
            (
                "anime_1".to_string(),
                vec![("A".to_string(), 2.0), ("B".to_string(), 1.0)],
            ),
            (
                "anime_2".to_string(),
                vec![("A".to_string(), 3.0), ("B".to_string(), 2.0)],
            ),
        ];

        let edges = build_collaboration_edges(staff);
        assert_eq!(edges.len(), 1); // Only A-B pair

        let (a, b, w, count) = &edges[0];
        assert_eq!(a, "A");
        assert_eq!(b, "B");
        // anime_1: (2+1)/2=1.5, anime_2: (3+2)/2=2.5, total=4.0
        assert!((w - 4.0).abs() < 1e-10);
        assert_eq!(*count, 2);
    }

    #[test]
    fn test_build_edges_empty() {
        let edges = build_collaboration_edges(vec![]);
        assert!(edges.is_empty());
    }

    #[test]
    fn test_same_person_skipped() {
        let staff = vec![(
            "anime_1".to_string(),
            vec![("A".to_string(), 2.0), ("A".to_string(), 1.0)],
        )];

        let edges = build_collaboration_edges(staff);
        assert!(edges.is_empty());
    }
}
