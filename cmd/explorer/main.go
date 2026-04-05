// Animetor Eval Explorer — Go server for interactive person search,
// similarity, ego-graph, and cluster exploration.
// stdlib only, no external dependencies.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

type Centrality struct {
	Degree      float64 `json:"degree"`
	Betweenness float64 `json:"betweenness"`
	Closeness   float64 `json:"closeness"`
	Eigenvector float64 `json:"eigenvector"`
}

type Career struct {
	FirstYear    int      `json:"first_year"`
	LatestYear   int      `json:"latest_year"`
	ActiveYears  int      `json:"active_years"`
	HighestStage int      `json:"highest_stage"`
	HighestRoles []string `json:"highest_roles"`
	PeakYear     int      `json:"peak_year"`
	PeakCredits  int      `json:"peak_credits"`
}

type Network struct {
	Collaborators int     `json:"collaborators"`
	UniqueAnime   int     `json:"unique_anime"`
	HubScore      float64 `json:"hub_score"`
}

type Growth struct {
	Trend         string  `json:"trend"`
	ActivityRatio float64 `json:"activity_ratio"`
	RecentCredits int     `json:"recent_credits"`
}

type Versatility struct {
	Score      float64 `json:"score"`
	Categories int     `json:"categories"`
	Roles      int     `json:"roles"`
}

type Person struct {
	PersonID     string      `json:"person_id"`
	Name         string      `json:"name"`
	NameJA       string      `json:"name_ja"`
	NameEN       string      `json:"name_en"`
	Authority    float64     `json:"authority"`
	Trust        float64     `json:"trust"`
	Skill        float64     `json:"skill"`
	Composite    float64     `json:"composite"`
	TotalCredits int         `json:"total_credits"`
	Centrality   Centrality  `json:"centrality"`
	PrimaryRole  string      `json:"primary_role"`
	Career       Career      `json:"career"`
	Network      Network     `json:"network"`
	Growth       Growth      `json:"growth"`
	Versatility  Versatility `json:"versatility"`
	AuthorityPct float64     `json:"authority_pct"`
	TrustPct     float64     `json:"trust_pct"`
	SkillPct     float64     `json:"skill_pct"`
	CompositePct float64     `json:"composite_pct"`
	Confidence   float64     `json:"confidence"`
	Tags         []string    `json:"tags"`

	// Enriched from ml_clusters.json
	Cluster     int       `json:"cluster"`
	ClusterName string    `json:"cluster_name"`
	PCA2D       [2]float64 `json:"pca_2d"`
	PCA3D       [3]float64 `json:"pca_3d"`

	// Feature vector (for similarity)
	features []float64
	norm     float64
}

type Collaboration struct {
	PersonA       string  `json:"person_a"`
	PersonB       string  `json:"person_b"`
	SharedWorks   int     `json:"shared_works"`
	StrengthScore float64 `json:"strength_score"`
	NameA         string  `json:"name_a"`
	NameB         string  `json:"name_b"`
	FirstYear     int     `json:"first_year"`
	LatestYear    int     `json:"latest_year"`
}

type ClusterProfile struct {
	Cluster int                `json:"cluster"`
	Name    string             `json:"name"`
	Size    int                `json:"size"`
	Profile map[string]float64 `json:"profile"`
}

type MLClusters struct {
	Metadata struct {
		NPersons              int                `json:"n_persons"`
		NClusters             int                `json:"n_clusters"`
		SilhouetteScore       float64            `json:"silhouette_score"`
		ClusterNames          map[string]string  `json:"cluster_names"`
		FeatureNames          []string           `json:"feature_names"`
		ExplainedVarRatio     []float64          `json:"explained_variance_ratio"`
	} `json:"metadata"`
	Persons []struct {
		PersonID    string     `json:"person_id"`
		Cluster     int        `json:"cluster"`
		ClusterName string     `json:"cluster_name"`
		PCA2D       [2]float64 `json:"pca_2d"`
		PCA3D       [3]float64 `json:"pca_3d"`
	} `json:"persons"`
	ClusterProfiles []ClusterProfile `json:"cluster_profiles"`
}

// ---------------------------------------------------------------------------
// Server state (loaded once at startup)
// ---------------------------------------------------------------------------

type Server struct {
	persons    []Person
	byID       map[string]*Person
	adj        map[string][]string // adjacency list from collaborations
	collabs    []Collaboration
	collabMap  map[string][]Collaboration // person_id -> collaborations
	clusters   []ClusterProfile
	metadata   MLClusters
	staticDir  string
	reportsDir string
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

func loadJSON(dir, name string, v interface{}) error {
	path := filepath.Join(dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	return json.Unmarshal(data, v)
}

func (s *Server) load(dataDir string) error {
	start := time.Now()

	// 1. Load explorer_data.json (or fall back to scores.json)
	var persons []Person
	err := loadJSON(dataDir, "explorer_data.json", &persons)
	if err != nil {
		log.Printf("explorer_data.json not found, trying scores.json...")
		if err2 := loadJSON(dataDir, "scores.json", &persons); err2 != nil {
			return fmt.Errorf("load persons: %w", err2)
		}
	}
	s.persons = persons
	s.byID = make(map[string]*Person, len(persons))
	for i := range s.persons {
		s.byID[s.persons[i].PersonID] = &s.persons[i]
	}
	log.Printf("Loaded %d persons (%.1fs)", len(s.persons), time.Since(start).Seconds())

	// 2. Load ml_clusters.json → enrich persons
	var ml MLClusters
	if err := loadJSON(dataDir, "ml_clusters.json", &ml); err == nil {
		s.metadata = ml
		s.clusters = ml.ClusterProfiles
		clusterMap := make(map[string]int)
		nameMap := make(map[string]string)
		pca2dMap := make(map[string][2]float64)
		pca3dMap := make(map[string][3]float64)
		for _, cp := range ml.Persons {
			clusterMap[cp.PersonID] = cp.Cluster
			nameMap[cp.PersonID] = cp.ClusterName
			pca2dMap[cp.PersonID] = cp.PCA2D
			pca3dMap[cp.PersonID] = cp.PCA3D
		}
		for i := range s.persons {
			pid := s.persons[i].PersonID
			if c, ok := clusterMap[pid]; ok {
				s.persons[i].Cluster = c
				s.persons[i].ClusterName = nameMap[pid]
				s.persons[i].PCA2D = pca2dMap[pid]
				s.persons[i].PCA3D = pca3dMap[pid]
			}
		}
		log.Printf("Enriched with %d cluster assignments", len(ml.Persons))
	} else {
		log.Printf("ml_clusters.json not found, skipping cluster enrichment")
	}

	// 3. Load collaborations.json
	var collabs []Collaboration
	if err := loadJSON(dataDir, "collaborations.json", &collabs); err == nil {
		s.collabs = collabs
		s.adj = make(map[string][]string)
		s.collabMap = make(map[string][]Collaboration)
		for _, c := range collabs {
			s.adj[c.PersonA] = append(s.adj[c.PersonA], c.PersonB)
			s.adj[c.PersonB] = append(s.adj[c.PersonB], c.PersonA)
			s.collabMap[c.PersonA] = append(s.collabMap[c.PersonA], c)
			s.collabMap[c.PersonB] = append(s.collabMap[c.PersonB], c)
		}
		log.Printf("Loaded %d collaborations", len(collabs))
	}

	// 4. Build feature vectors for similarity
	s.buildFeatures()

	log.Printf("Server ready in %.2fs", time.Since(start).Seconds())
	return nil
}

func (s *Server) buildFeatures() {
	for i := range s.persons {
		p := &s.persons[i]
		f := []float64{
			p.Authority, p.Trust, p.Skill, p.Composite,
			float64(p.TotalCredits),
			p.Centrality.Degree, p.Centrality.Betweenness, p.Centrality.Eigenvector,
			float64(p.Career.ActiveYears), float64(p.Career.HighestStage), float64(p.Career.PeakCredits),
			float64(p.Network.Collaborators), float64(p.Network.UniqueAnime), p.Network.HubScore,
			p.Growth.ActivityRatio, float64(p.Growth.RecentCredits),
			p.Versatility.Score, float64(p.Versatility.Categories), float64(p.Versatility.Roles),
			p.Confidence,
		}
		p.features = f
		var sumSq float64
		for _, v := range f {
			sumSq += v * v
		}
		p.norm = math.Sqrt(sumSq)
	}
}

func cosineSimilarity(a, b *Person) float64 {
	if a.norm == 0 || b.norm == 0 {
		return 0
	}
	var dot float64
	for i := range a.features {
		dot += a.features[i] * b.features[i]
	}
	return dot / (a.norm * b.norm)
}

// ---------------------------------------------------------------------------
// JSON response helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// GET /api/search?q=&role=&cluster=&limit=20&offset=0
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	roleFilter := r.URL.Query().Get("role")
	clusterFilter := r.URL.Query().Get("cluster")
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")
	limit := 20
	if n, err := strconv.Atoi(limitStr); err == nil && n > 0 && n <= 200 {
		limit = n
	}
	offset := 0
	if n, err := strconv.Atoi(offsetStr); err == nil && n >= 0 {
		offset = n
	}

	var matched []*Person
	for i := range s.persons {
		p := &s.persons[i]
		if q != "" {
			name := strings.ToLower(p.Name + " " + p.NameJA + " " + p.NameEN)
			if !strings.Contains(name, q) {
				continue
			}
		}
		if roleFilter != "" && p.PrimaryRole != roleFilter {
			continue
		}
		if clusterFilter != "" {
			if c, err := strconv.Atoi(clusterFilter); err == nil && p.Cluster != c {
				continue
			}
		}
		matched = append(matched, p)
	}

	// Sort by composite descending
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].Composite > matched[j].Composite
	})

	total := len(matched)
	if offset < len(matched) {
		matched = matched[offset:]
	} else {
		matched = nil
	}
	if len(matched) > limit {
		matched = matched[:limit]
	}

	type SearchResult struct {
		PersonID     string   `json:"person_id"`
		Name         string   `json:"name"`
		PrimaryRole  string   `json:"primary_role"`
		Composite    float64  `json:"composite"`
		Cluster      int      `json:"cluster"`
		ClusterName  string   `json:"cluster_name"`
		TotalCredits int      `json:"total_credits"`
		Tags         []string `json:"tags"`
	}
	results := make([]SearchResult, len(matched))
	for i, p := range matched {
		results[i] = SearchResult{
			PersonID:     p.PersonID,
			Name:         p.Name,
			PrimaryRole:  p.PrimaryRole,
			Composite:    p.Composite,
			Cluster:      p.Cluster,
			ClusterName:  p.ClusterName,
			TotalCredits: p.TotalCredits,
			Tags:         p.Tags,
		}
	}

	writeJSON(w, map[string]interface{}{
		"results": results,
		"total":   total,
		"offset":  offset,
		"limit":   limit,
	})
}

// GET /api/roles
func (s *Server) handleRoles(w http.ResponseWriter, r *http.Request) {
	counts := make(map[string]int)
	for _, p := range s.persons {
		counts[p.PrimaryRole]++
	}
	type RoleSummary struct {
		Role  string `json:"role"`
		Count int    `json:"count"`
	}
	roles := make([]RoleSummary, 0, len(counts))
	for role, cnt := range counts {
		roles = append(roles, RoleSummary{role, cnt})
	}
	sort.Slice(roles, func(i, j int) bool {
		return roles[i].Count > roles[j].Count
	})
	writeJSON(w, map[string]interface{}{"roles": roles})
}

// GET /api/stats
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	var sumComp, maxComp float64
	for _, p := range s.persons {
		sumComp += p.Composite
		if p.Composite > maxComp {
			maxComp = p.Composite
		}
	}
	avg := 0.0
	if len(s.persons) > 0 {
		avg = sumComp / float64(len(s.persons))
	}
	resp := map[string]interface{}{
		"total_persons":  len(s.persons),
		"total_clusters": s.metadata.Metadata.NClusters,
		"avg_composite":  math.Round(avg*10) / 10,
		"max_composite":  math.Round(maxComp*10) / 10,
		"silhouette":     s.metadata.Metadata.SilhouetteScore,
	}
	writeJSON(w, resp)
}

// GET /api/persons/{id}
func (s *Server) handlePerson(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/persons/")
	// Strip trailing sub-paths
	if idx := strings.Index(id, "/"); idx != -1 {
		id = id[:idx]
	}
	p, ok := s.byID[id]
	if !ok {
		writeError(w, 404, "person not found")
		return
	}
	writeJSON(w, p)
}

// GET /api/persons/{id}/graph
func (s *Server) handleGraph(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/persons/"), "/")
	if len(parts) < 2 {
		writeError(w, 400, "invalid path")
		return
	}
	id := parts[0]
	p, ok := s.byID[id]
	if !ok {
		writeError(w, 404, "person not found")
		return
	}

	type GraphNode struct {
		PersonID  string  `json:"person_id"`
		Name      string  `json:"name"`
		Composite float64 `json:"composite"`
		Role      string  `json:"primary_role"`
		Cluster   int     `json:"cluster"`
	}
	type GraphEdge struct {
		Source        string  `json:"source"`
		Target        string  `json:"target"`
		SharedWorks   int     `json:"shared_works"`
		StrengthScore float64 `json:"strength_score"`
	}

	nodes := []GraphNode{{
		PersonID: p.PersonID, Name: p.Name, Composite: p.Composite,
		Role: p.PrimaryRole, Cluster: p.Cluster,
	}}
	var edges []GraphEdge
	seen := map[string]bool{id: true}

	for _, c := range s.collabMap[id] {
		other := c.PersonB
		if other == id {
			other = c.PersonA
		}
		if seen[other] {
			continue
		}
		seen[other] = true
		if op, ok := s.byID[other]; ok {
			nodes = append(nodes, GraphNode{
				PersonID: op.PersonID, Name: op.Name, Composite: op.Composite,
				Role: op.PrimaryRole, Cluster: op.Cluster,
			})
		}
		edges = append(edges, GraphEdge{
			Source: id, Target: other,
			SharedWorks: c.SharedWorks, StrengthScore: c.StrengthScore,
		})
	}

	writeJSON(w, map[string]interface{}{"nodes": nodes, "edges": edges})
}

// GET /api/persons/{id}/similar?n=10
func (s *Server) handleSimilar(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/persons/"), "/")
	if len(parts) < 2 {
		writeError(w, 400, "invalid path")
		return
	}
	id := parts[0]
	p, ok := s.byID[id]
	if !ok {
		writeError(w, 404, "person not found")
		return
	}

	n := 10
	if nStr := r.URL.Query().Get("n"); nStr != "" {
		if nn, err := strconv.Atoi(nStr); err == nil && nn > 0 && nn <= 50 {
			n = nn
		}
	}

	type SimilarResult struct {
		Person     Person  `json:"person"`
		Similarity float64 `json:"similarity"`
	}

	var results []SimilarResult
	for i := range s.persons {
		if s.persons[i].PersonID == id {
			continue
		}
		sim := cosineSimilarity(p, &s.persons[i])
		results = append(results, SimilarResult{Person: s.persons[i], Similarity: math.Round(sim*10000) / 10000})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Similarity > results[j].Similarity
	})
	if len(results) > n {
		results = results[:n]
	}

	writeJSON(w, map[string]interface{}{"similar": results})
}

// GET /api/persons/{id}/recommendations
func (s *Server) handleRecommendations(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/persons/"), "/")
	if len(parts) < 2 {
		writeError(w, 400, "invalid path")
		return
	}
	id := parts[0]
	p, ok := s.byID[id]
	if !ok {
		writeError(w, 404, "person not found")
		return
	}

	firstYear := p.Career.FirstYear
	role := p.PrimaryRole
	var recs []Person
	for _, other := range s.persons {
		if other.PersonID == id {
			continue
		}
		yearDiff := other.Career.FirstYear - firstYear
		if yearDiff < -5 || yearDiff > 5 {
			continue
		}
		if role != "" && other.PrimaryRole != role {
			continue
		}
		recs = append(recs, other)
	}
	sort.Slice(recs, func(i, j int) bool {
		return recs[i].Composite > recs[j].Composite
	})
	if len(recs) > 20 {
		recs = recs[:20]
	}

	writeJSON(w, map[string]interface{}{
		"target_year": firstYear,
		"target_role": role,
		"recommendations": recs,
	})
}

// GET /api/clusters
func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]interface{}{
		"n_clusters":       s.metadata.Metadata.NClusters,
		"silhouette_score": s.metadata.Metadata.SilhouetteScore,
		"clusters":         s.clusters,
	})
}

// GET /api/clusters/{id}
func (s *Server) handleClusterDetail(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimPrefix(r.URL.Path, "/api/clusters/")
	cid, err := strconv.Atoi(idStr)
	if err != nil {
		writeError(w, 400, "invalid cluster id")
		return
	}

	var profile *ClusterProfile
	for i := range s.clusters {
		if s.clusters[i].Cluster == cid {
			profile = &s.clusters[i]
			break
		}
	}
	if profile == nil {
		writeError(w, 404, "cluster not found")
		return
	}

	var members []Person
	for _, p := range s.persons {
		if p.Cluster == cid {
			members = append(members, p)
		}
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].Composite > members[j].Composite
	})

	writeJSON(w, map[string]interface{}{
		"profile": profile,
		"members": members,
	})
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	switch {
	case path == "/api/search":
		s.handleSearch(w, r)
	case path == "/api/roles":
		s.handleRoles(w, r)
	case path == "/api/stats":
		s.handleStats(w, r)
	case path == "/api/clusters":
		s.handleClusters(w, r)
	case strings.HasPrefix(path, "/api/clusters/"):
		s.handleClusterDetail(w, r)
	case strings.HasSuffix(path, "/graph") && strings.HasPrefix(path, "/api/persons/"):
		s.handleGraph(w, r)
	case strings.HasSuffix(path, "/similar") && strings.HasPrefix(path, "/api/persons/"):
		s.handleSimilar(w, r)
	case strings.HasSuffix(path, "/recommendations") && strings.HasPrefix(path, "/api/persons/"):
		s.handleRecommendations(w, r)
	case strings.HasPrefix(path, "/api/persons/"):
		s.handlePerson(w, r)
	case strings.HasPrefix(path, "/reports/") || path == "/reports":
		// Serve report HTML files
		if s.reportsDir != "" {
			if path == "/reports" || path == "/reports/" {
				http.ServeFile(w, r, filepath.Join(s.reportsDir, "index.html"))
				return
			}
			sub := strings.TrimPrefix(path, "/reports/")
			fpath := filepath.Join(s.reportsDir, sub)
			if _, err := os.Stat(fpath); err == nil {
				http.ServeFile(w, r, fpath)
				return
			}
			writeError(w, 404, "report not found")
		} else {
			writeError(w, 404, "reports dir not configured")
		}
	default:
		// Serve static files
		if s.staticDir != "" {
			fs := http.FileServer(http.Dir(s.staticDir))
			// For SPA: serve index.html for non-file paths
			if path == "/" || path == "" {
				http.ServeFile(w, r, filepath.Join(s.staticDir, "index.html"))
				return
			}
			fpath := filepath.Join(s.staticDir, path)
			if _, err := os.Stat(fpath); err == nil {
				fs.ServeHTTP(w, r)
				return
			}
			// SPA fallback
			http.ServeFile(w, r, filepath.Join(s.staticDir, "index.html"))
		} else {
			writeError(w, 404, "not found")
		}
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	dataDir := flag.String("data", "./result/json", "Path to JSON data directory")
	staticDir := flag.String("static", "./static/explorer", "Path to static files directory")
	reportsDir := flag.String("reports", "./result/reports", "Path to generated reports directory")
	port := flag.Int("port", 3000, "HTTP port")
	flag.Parse()

	srv := &Server{staticDir: *staticDir, reportsDir: *reportsDir}
	if err := srv.load(*dataDir); err != nil {
		log.Fatalf("Failed to load data: %v", err)
	}

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Explorer server listening on http://localhost%s", addr)
	log.Printf("  Data: %s (%d persons)", *dataDir, len(srv.persons))
	log.Printf("  Static: %s", *staticDir)
	log.Printf("  Reports: %s", *reportsDir)

	httpSrv := &http.Server{
		Addr:         addr,
		Handler:      srv,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	if err := httpSrv.ListenAndServe(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
