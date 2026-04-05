// Animetor Eval — Person History Page Generator
// Generates self-contained HTML pages with embedded Plotly charts
// for individual person profiles. stdlib only, no external dependencies.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Data structures — match the JSON schemas from pipeline output
// ---------------------------------------------------------------------------

type Centrality struct {
	Degree      float64 `json:"degree"`
	Betweenness float64 `json:"betweenness"`
	Closeness   float64 `json:"closeness"`
	Eigenvector float64 `json:"eigenvector"`
}

type Career struct {
	FirstYear    int `json:"first_year"`
	LatestYear   int `json:"latest_year"`
	ActiveYears  int `json:"active_years"`
	HighestStage int `json:"highest_stage"`
	PeakYear     int `json:"peak_year"`
	PeakCredits  int `json:"peak_credits"`
}

type Network struct {
	Collaborators int     `json:"collaborators"`
	UniqueAnime   int     `json:"unique_anime"`
	HubScore      float64 `json:"hub_score"`
}

type GrowthInline struct {
	Trend         string  `json:"trend"`
	ActivityRatio float64 `json:"activity_ratio"`
	RecentCredits int     `json:"recent_credits"`
}

type Versatility struct {
	Score      float64 `json:"score"`
	Categories int     `json:"categories"`
	Roles      int     `json:"roles"`
}

type ScoreRange struct {
	IVScore   [2]float64 `json:"iv_score"`
	BiRank    [2]float64 `json:"birank"`
	Patronage [2]float64 `json:"patronage"`
	PersonFE  [2]float64 `json:"person_fe"`
}

type Person struct {
	PersonID         string       `json:"person_id"`
	Name             string       `json:"name"`
	NameJA           string       `json:"name_ja"`
	NameEN           string       `json:"name_en"`
	IVScore          float64      `json:"iv_score"`
	PersonFE         float64      `json:"person_fe"`
	StudioFEExposure float64      `json:"studio_fe_exposure"`
	BiRank           float64      `json:"birank"`
	Patronage        float64      `json:"patronage"`
	Dormancy         float64      `json:"dormancy"`
	AWCC             float64      `json:"awcc"`
	NDI              float64      `json:"ndi"`
	CareerFriction   float64      `json:"career_friction"`
	PeerBoost        float64      `json:"peer_boost"`
	Centrality       Centrality   `json:"centrality"`
	PrimaryRole      string       `json:"primary_role"`
	TotalCredits     int          `json:"total_credits"`
	Career           Career       `json:"career"`
	Network          Network      `json:"network"`
	Growth           GrowthInline `json:"growth"`
	Versatility      Versatility  `json:"versatility"`
	IVScorePct       float64      `json:"iv_score_pct"`
	PersonFEPct      float64      `json:"person_fe_pct"`
	BiRankPct        float64      `json:"birank_pct"`
	PatronagePct     float64      `json:"patronage_pct"`
	AWCCPct          float64      `json:"awcc_pct"`
	DormancyPct      float64      `json:"dormancy_pct"`
	Confidence       float64      `json:"confidence"`
	ScoreRange       *ScoreRange  `json:"score_range"`
	Tags             []string     `json:"tags"`

	// Optional fields (may not exist in current data)
	PersonFESE   *float64 `json:"person_fe_se"`
	PersonFENObs *int     `json:"person_fe_n_obs"`
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

type IndividualProfile struct {
	PersonID            string  `json:"person_id"`
	PeerPercentile      float64 `json:"peer_percentile"`
	OpportunityResidual float64 `json:"opportunity_residual"`
	Consistency         float64 `json:"consistency"`
	IndependentValue    float64 `json:"independent_value"`
	ClusterPercentile   float64 `json:"cluster_percentile"`
	ClusterID           int     `json:"cluster_id"`
	ClusterSize         int     `json:"cluster_size"`
}

type ProfilesFile struct {
	Profiles map[string]IndividualProfile `json:"profiles"`
}

type Milestone struct {
	Type        string `json:"type"`
	Year        int    `json:"year"`
	AnimeID     string `json:"anime_id"`
	AnimeTitle  string `json:"anime_title"`
	Description string `json:"description"`
	Role        string `json:"role"`
	FromStage   int    `json:"from_stage"`
	ToStage     int    `json:"to_stage"`
}

type GrowthPerson struct {
	YearlyCredits map[string]int `json:"yearly_credits"`
	Trend         string         `json:"trend"`
	TotalCredits  int            `json:"total_credits"`
	RecentCredits int            `json:"recent_credits"`
	TotalYears    int            `json:"total_years"`
	CareerSpan    int            `json:"career_span"`
	ActivityRatio float64        `json:"activity_ratio"`
	Name          string         `json:"name"`
}

type GrowthFile struct {
	TrendSummary map[string]int          `json:"trend_summary"`
	TotalPersons int                     `json:"total_persons"`
	Persons      map[string]GrowthPerson `json:"persons"`
}

type IVWeightsFile struct {
	LambdaWeights map[string]float64 `json:"lambda_weights"`
}

// ---------------------------------------------------------------------------
// Histogram pre-computation
// ---------------------------------------------------------------------------

type Histogram struct {
	BinEdges []float64 // len = numBins+1
	Counts   []int     // len = numBins
	Min      float64
	Max      float64
}

func computeHistogram(values []float64, numBins int) Histogram {
	if len(values) == 0 {
		return Histogram{BinEdges: make([]float64, numBins+1), Counts: make([]int, numBins)}
	}

	minVal, maxVal := values[0], values[0]
	for _, v := range values {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	// Avoid zero-width bins
	if maxVal == minVal {
		maxVal = minVal + 1
	}

	binWidth := (maxVal - minVal) / float64(numBins)
	edges := make([]float64, numBins+1)
	for i := 0; i <= numBins; i++ {
		edges[i] = minVal + float64(i)*binWidth
	}
	counts := make([]int, numBins)

	for _, v := range values {
		idx := int((v - minVal) / binWidth)
		if idx >= numBins {
			idx = numBins - 1
		}
		if idx < 0 {
			idx = 0
		}
		counts[idx]++
	}

	return Histogram{BinEdges: edges, Counts: counts, Min: minVal, Max: maxVal}
}

// ---------------------------------------------------------------------------
// Loading helpers
// ---------------------------------------------------------------------------

func loadJSON(dir, name string, v interface{}) error {
	path := filepath.Join(dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	return json.Unmarshal(data, v)
}

// ---------------------------------------------------------------------------
// Filename sanitization
// ---------------------------------------------------------------------------

func sanitizeFilename(id string) string {
	r := strings.NewReplacer(":", "_", "/", "_", "\\", "_", " ", "_", "?", "_", "*", "_")
	return r.Replace(id)
}

// ---------------------------------------------------------------------------
// Display helpers
// ---------------------------------------------------------------------------

func stageName(stage int) string {
	names := map[int]string{
		0: "未分類", 1: "新人", 2: "中堅", 3: "上級",
		4: "ベテラン", 5: "マスター", 6: "レジェンド",
	}
	if n, ok := names[stage]; ok {
		return n
	}
	return fmt.Sprintf("Stage %d", stage)
}

func trendNameJA(trend string) string {
	names := map[string]string{
		"rising": "上昇", "stable": "安定", "declining": "下降",
		"inactive": "休止", "new": "新規",
	}
	if n, ok := names[trend]; ok {
		return n
	}
	return trend
}

func roleNameJA(role string) string {
	names := map[string]string{
		"director": "監督", "key_animator": "原画",
		"animation_director": "作画監督", "episode_director": "演出",
		"producer": "プロデューサー", "production": "制作",
		"sound_director": "音響監督", "character_designer": "キャラクターデザイン",
		"art_director": "美術監督", "color_designer": "色彩設計",
		"music": "音楽", "in_between": "動画", "script": "脚本",
		"storyboard": "絵コンテ", "other": "その他", "unknown": "不明",
	}
	if n, ok := names[role]; ok {
		return n
	}
	return role
}

func fmtF(v float64, prec int) string {
	return fmt.Sprintf("%.*f", prec, v)
}

// ---------------------------------------------------------------------------
// JS array serialization for embedding in HTML
// ---------------------------------------------------------------------------

func jsFloatArray(vals []float64) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			parts[i] = "0"
		} else {
			parts[i] = fmtF(v, 4)
		}
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func jsIntArray(vals []int) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = fmt.Sprintf("%d", v)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func jsStringArray(vals []string) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = fmt.Sprintf("%q", v)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// ---------------------------------------------------------------------------
// CSS constant — shared by person pages and index
// ---------------------------------------------------------------------------

const pageCSS = `*{margin:0;padding:0;box-sizing:border-box}
body{
  font-family:'Segoe UI',system-ui,-apple-system,sans-serif;
  background:linear-gradient(135deg,#0f0c29,#302b63,#24243e);
  color:#e0e0f0;min-height:100vh;line-height:1.6;
}
.container{max-width:1200px;margin:0 auto;padding:24px 20px}
h1{font-size:2rem;font-weight:700;margin-bottom:4px}
h2{font-size:1.3rem;font-weight:600;margin:28px 0 14px;color:#f093fb;
   border-bottom:1px solid rgba(240,147,251,0.3);padding-bottom:6px}
h3{font-size:1.05rem;font-weight:600;margin:18px 0 10px;color:#a0d2db}
.subtitle{color:#a0a0c0;font-size:0.95rem;margin-bottom:16px}
.card{
  background:rgba(255,255,255,0.05);backdrop-filter:blur(12px);
  border:1px solid rgba(255,255,255,0.08);border-radius:12px;
  padding:20px;margin-bottom:18px;
}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:18px}
.grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:18px}
.grid4{display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:14px}
@media(max-width:900px){.grid2,.grid3,.grid4{grid-template-columns:1fr}}
.stat-box{
  background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);
  border-radius:8px;padding:14px;text-align:center;
}
.stat-box .label{font-size:0.78rem;color:#a0a0c0;margin-bottom:4px}
.stat-box .value{font-size:1.5rem;font-weight:700}
.stat-box .sub{font-size:0.75rem;color:#a0a0c0;margin-top:2px}
.accent-pink{color:#f093fb}
.accent-green{color:#06D6A0}
.accent-blue{color:#667eea}
.accent-yellow{color:#FFD166}
.accent-teal{color:#a0d2db}
.tag{
  display:inline-block;background:rgba(102,126,234,0.2);
  border:1px solid rgba(102,126,234,0.3);border-radius:20px;
  padding:3px 12px;font-size:0.8rem;margin:3px 4px 3px 0;color:#a0d2db;
}
table{width:100%;border-collapse:collapse;font-size:0.88rem}
thead th{text-align:left;padding:8px 10px;border-bottom:1px solid rgba(255,255,255,0.1);
  color:#a0a0c0;font-weight:600}
tbody td{padding:7px 10px;border-bottom:1px solid rgba(255,255,255,0.04)}
tbody tr:hover{background:rgba(255,255,255,0.03)}
.chart-container{margin:10px 0;min-height:320px}
.back-link{display:inline-block;color:#667eea;text-decoration:none;font-size:0.9rem;margin-bottom:16px}
.back-link:hover{text-decoration:underline}
.layer-row{display:flex;align-items:center;padding:6px 0;border-bottom:1px solid rgba(255,255,255,0.04)}
.layer-label{width:180px;font-size:0.85rem;color:#a0a0c0}
.layer-value{font-size:1rem;font-weight:600;min-width:80px}
.layer-bar{flex:1;height:8px;background:rgba(255,255,255,0.05);border-radius:4px;margin-left:12px;overflow:hidden}
.layer-bar-fill{height:100%;border-radius:4px}
.disclaimer{
  margin-top:32px;padding:14px;background:rgba(255,255,255,0.03);
  border:1px solid rgba(255,255,255,0.06);border-radius:8px;
  font-size:0.75rem;color:#808098;line-height:1.5;
}
.milestone-item{
  display:flex;align-items:flex-start;padding:8px 0;
  border-left:2px solid rgba(240,147,251,0.3);padding-left:14px;
  margin-left:6px;margin-bottom:4px;
}
.milestone-year{font-weight:700;color:#f093fb;min-width:50px;font-size:0.9rem}
.milestone-desc{font-size:0.85rem;color:#c0c0d0}
a{color:#a0d2db;text-decoration:none}
a:hover{text-decoration:underline}`

// ---------------------------------------------------------------------------
// Person page generation
// ---------------------------------------------------------------------------

type personPageData struct {
	P             Person
	Collabs       []Collaboration
	Profile       *IndividualProfile
	Milestones    []Milestone
	GrowthData    *GrowthPerson
	IVWeights     map[string]float64
	HistPersonFE  Histogram
	HistBiRank    Histogram
	HistPatronage Histogram
	HistIVScore   Histogram
	StudioExpPct  float64
}

func generatePersonPage(d personPageData) string {
	p := d.P
	var sb strings.Builder

	displayName := p.Name
	if displayName == "" {
		displayName = p.NameEN
	}
	nameEN := p.NameEN
	if nameEN == "" {
		nameEN = p.NameJA
	}

	careerSpan := ""
	if p.Career.FirstYear > 0 {
		careerSpan = fmt.Sprintf("%d — %d (%d年)", p.Career.FirstYear, p.Career.LatestYear, p.Career.ActiveYears)
	}

	escapedName := html.EscapeString(displayName)

	// --- HTML head ---
	sb.WriteString("<!DOCTYPE html>\n<html lang=\"ja\">\n<head>\n<meta charset=\"UTF-8\">\n")
	sb.WriteString("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n")
	sb.WriteString("<title>")
	sb.WriteString(escapedName)
	sb.WriteString(" — Person History | Animetor Eval</title>\n")
	sb.WriteString("<script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>\n")
	sb.WriteString("<style>\n")
	sb.WriteString(pageCSS)
	sb.WriteString("\n</style>\n</head>\n<body>\n<div class=\"container\">\n")

	// --- Back link ---
	sb.WriteString("<a class=\"back-link\" href=\"index.html\">&#8592; 一覧に戻る</a>\n")

	// --- Header ---
	sb.WriteString("<h1>")
	sb.WriteString(escapedName)
	sb.WriteString("</h1>\n")
	if nameEN != displayName && nameEN != "" {
		sb.WriteString("<div class=\"subtitle\">")
		sb.WriteString(html.EscapeString(nameEN))
		sb.WriteString("</div>\n")
	}

	// --- Key stats ---
	sb.WriteString("<div class=\"grid4\" style=\"margin-top:14px;margin-bottom:20px\">\n")

	writeStatBox(&sb, "IV Score", fmtF(p.IVScore, 2), "accent-pink",
		fmt.Sprintf("P%s", fmtF(p.IVScorePct, 1)))
	writeStatBox(&sb, "主要職種", html.EscapeString(roleNameJA(p.PrimaryRole)), "accent-blue",
		html.EscapeString(p.PrimaryRole))

	careerVal := "—"
	if careerSpan != "" {
		careerVal = html.EscapeString(careerSpan)
	}
	sb.WriteString("<div class=\"stat-box\"><div class=\"label\">キャリア</div>")
	sb.WriteString(fmt.Sprintf("<div class=\"value accent-green\" style=\"font-size:1.1rem\">%s</div>", careerVal))
	sb.WriteString(fmt.Sprintf("<div class=\"sub\">%s</div></div>\n", stageName(p.Career.HighestStage)))

	writeStatBox(&sb, "信頼度", fmtF(p.Confidence*100, 1)+"%", "accent-yellow",
		fmt.Sprintf("%d credits / %d works", p.TotalCredits, p.Network.UniqueAnime))
	sb.WriteString("</div>\n")

	// --- Tags ---
	if len(p.Tags) > 0 {
		sb.WriteString("<div style=\"margin-bottom:18px\">")
		for _, t := range p.Tags {
			sb.WriteString("<span class=\"tag\">")
			sb.WriteString(html.EscapeString(t))
			sb.WriteString("</span>")
		}
		sb.WriteString("</div>\n")
	}

	// --- Score Radar ---
	sb.WriteString("<h2>Score Radar</h2>\n<div class=\"card\">\n")
	sb.WriteString("<div class=\"chart-container\" id=\"radar-chart\"></div>\n</div>\n")

	radarAxes := []string{"Person FE", "BiRank", "Patronage", "AWCC", "Studio Exp.", "Dormancy"}
	personPcts := []float64{p.PersonFEPct, p.BiRankPct, p.PatronagePct, p.AWCCPct, d.StudioExpPct, p.DormancyPct}
	medianPcts := []float64{50, 50, 50, 50, 50, 50}

	sb.WriteString("<script>\n(function(){\n")
	sb.WriteString("var axes=")
	sb.WriteString(jsStringArray(radarAxes))
	sb.WriteString(";\nvar pv=")
	sb.WriteString(jsFloatArray(personPcts))
	sb.WriteString(";\nvar mv=")
	sb.WriteString(jsFloatArray(medianPcts))
	sb.WriteString(";\n")
	sb.WriteString("axes.push(axes[0]);pv.push(pv[0]);mv.push(mv[0]);\n")
	sb.WriteString("Plotly.newPlot('radar-chart',[")
	sb.WriteString("{type:'scatterpolar',r:pv,theta:axes,fill:'toself',name:")
	sb.WriteString(fmt.Sprintf("%q", displayName))
	sb.WriteString(",fillcolor:'rgba(240,147,251,0.15)',line:{color:'#f093fb',width:2},marker:{size:6,color:'#f093fb'}},")
	sb.WriteString("{type:'scatterpolar',r:mv,theta:axes,fill:'toself',name:'Population Median',")
	sb.WriteString("fillcolor:'rgba(102,126,234,0.08)',line:{color:'#667eea',width:1.5,dash:'dot'},marker:{size:4,color:'#667eea'}}")
	sb.WriteString("],{polar:{bgcolor:'rgba(0,0,0,0)',")
	sb.WriteString("radialaxis:{visible:true,range:[0,100],tickfont:{color:'#808098',size:10},gridcolor:'rgba(255,255,255,0.08)'},")
	sb.WriteString("angularaxis:{tickfont:{color:'#a0a0c0',size:11},gridcolor:'rgba(255,255,255,0.08)'}},")
	sb.WriteString("showlegend:true,legend:{font:{color:'#a0a0c0',size:11},x:0.5,y:-0.15,xanchor:'center',orientation:'h'},")
	sb.WriteString("paper_bgcolor:'rgba(0,0,0,0)',plot_bgcolor:'rgba(0,0,0,0)',margin:{t:30,b:60,l:60,r:60},height:400")
	sb.WriteString("},{responsive:true});\n})();\n</script>\n")

	// --- Distribution Positioning ---
	sb.WriteString("<h2>Distribution Positioning</h2>\n<div class=\"grid2\">\n")

	distCharts := []struct {
		id, title string
		value     float64
		pct       float64
		hist      Histogram
		color     string
	}{
		{"hist-pfe", "Person FE (個人固定効果)", p.PersonFE, p.PersonFEPct, d.HistPersonFE, "#f093fb"},
		{"hist-br", "BiRank (構造的評価)", p.BiRank, p.BiRankPct, d.HistBiRank, "#06D6A0"},
		{"hist-pat", "Patronage (指名率)", p.Patronage, p.PatronagePct, d.HistPatronage, "#667eea"},
		{"hist-iv", "IV Score (統合価値)", p.IVScore, p.IVScorePct, d.HistIVScore, "#FFD166"},
	}

	for _, dc := range distCharts {
		sb.WriteString(fmt.Sprintf("<div class=\"card\"><div class=\"chart-container\" id=\"%s\"></div></div>\n", dc.id))
	}
	sb.WriteString("</div>\n")

	for _, dc := range distCharts {
		binCenters := make([]float64, len(dc.hist.Counts))
		for i := range dc.hist.Counts {
			binCenters[i] = (dc.hist.BinEdges[i] + dc.hist.BinEdges[i+1]) / 2
		}
		sb.WriteString("<script>\n(function(){\n")
		sb.WriteString("var x=")
		sb.WriteString(jsFloatArray(binCenters))
		sb.WriteString(";\nvar y=")
		sb.WriteString(jsIntArray(dc.hist.Counts))
		sb.WriteString(";\nvar pv=")
		sb.WriteString(fmtF(dc.value, 4))
		sb.WriteString(";\nvar pp=")
		sb.WriteString(fmtF(dc.pct, 1))
		sb.WriteString(";\n")
		sb.WriteString(fmt.Sprintf("Plotly.newPlot('%s',[{type:'bar',x:x,y:y,", dc.id))
		sb.WriteString("marker:{color:'rgba(255,255,255,0.12)',line:{color:'rgba(255,255,255,0.2)',width:0.5}},")
		sb.WriteString("hovertemplate:'Value: %{x:.3f}<br>Count: %{y}<extra></extra>'}],{")
		sb.WriteString(fmt.Sprintf("title:{text:'%s',font:{color:'#e0e0f0',size:13}},", dc.title))
		sb.WriteString(fmt.Sprintf("shapes:[{type:'line',x0:pv,x1:pv,y0:0,y1:1,yref:'paper',line:{color:'%s',width:2.5}}],", dc.color))
		sb.WriteString(fmt.Sprintf("annotations:[{x:pv,y:1,yref:'paper',text:'P'+pp.toFixed(1),showarrow:true,arrowhead:0,arrowcolor:'%s',font:{color:'%s',size:12,family:'monospace'},ax:30,ay:-20}],", dc.color, dc.color))
		sb.WriteString("paper_bgcolor:'rgba(0,0,0,0)',plot_bgcolor:'rgba(0,0,0,0)',")
		sb.WriteString("xaxis:{gridcolor:'rgba(255,255,255,0.05)',tickfont:{color:'#808098',size:10},title:{text:'Value',font:{color:'#a0a0c0',size:10}}},")
		sb.WriteString("yaxis:{gridcolor:'rgba(255,255,255,0.05)',tickfont:{color:'#808098',size:10},title:{text:'Count',font:{color:'#a0a0c0',size:10}}},")
		sb.WriteString("margin:{t:40,b:45,l:50,r:20},height:280,bargap:0.02")
		sb.WriteString("},{responsive:true});\n})();\n</script>\n")
	}

	// --- Career Timeline ---
	sb.WriteString("<h2>Career Timeline</h2>\n<div class=\"card\">\n")

	if d.GrowthData != nil && len(d.GrowthData.YearlyCredits) > 0 {
		type yc struct {
			Year, Count int
		}
		var ycs []yc
		for yStr, cnt := range d.GrowthData.YearlyCredits {
			var y int
			if _, err := fmt.Sscanf(yStr, "%d", &y); err == nil && y > 0 {
				ycs = append(ycs, yc{y, cnt})
			}
		}
		sort.Slice(ycs, func(i, j int) bool { return ycs[i].Year < ycs[j].Year })

		years := make([]int, len(ycs))
		counts := make([]int, len(ycs))
		for i, v := range ycs {
			years[i] = v.Year
			counts[i] = v.Count
		}

		sb.WriteString("<div class=\"chart-container\" id=\"career-timeline\"></div>\n")

		// Milestone annotations for promotions and career starts
		var annots []string
		for _, ms := range d.Milestones {
			if ms.Type != "promotion" && ms.Type != "career_start" {
				continue
			}
			color := "#f093fb"
			if ms.Type == "career_start" {
				color = "#06D6A0"
			}
			label := ms.Description
			runes := []rune(label)
			if len(runes) > 25 {
				label = string(runes[:25]) + "..."
			}
			annots = append(annots, fmt.Sprintf(
				"{x:%d,y:0,yref:'paper',text:%q,showarrow:true,arrowhead:2,arrowcolor:'%s',font:{color:'%s',size:9},ax:0,ay:-25}",
				ms.Year, label, color, color))
		}

		sb.WriteString("<script>\n(function(){\n")
		sb.WriteString("var yrs=")
		sb.WriteString(jsIntArray(years))
		sb.WriteString(";\nvar cts=")
		sb.WriteString(jsIntArray(counts))
		sb.WriteString(";\nvar ann=[")
		sb.WriteString(strings.Join(annots, ","))
		sb.WriteString("];\n")
		sb.WriteString("Plotly.newPlot('career-timeline',[{type:'bar',x:yrs,y:cts,")
		sb.WriteString("marker:{color:'rgba(240,147,251,0.5)',line:{color:'#f093fb',width:1}},")
		sb.WriteString("hovertemplate:'%{x}: %{y} credits<extra></extra>',name:'Credits/Year'}],{")
		sb.WriteString("title:{text:'年別クレジット数',font:{color:'#e0e0f0',size:13}},")
		sb.WriteString("paper_bgcolor:'rgba(0,0,0,0)',plot_bgcolor:'rgba(0,0,0,0)',")
		sb.WriteString("xaxis:{gridcolor:'rgba(255,255,255,0.05)',tickfont:{color:'#808098',size:10},title:{text:'Year',font:{color:'#a0a0c0',size:10}}},")
		sb.WriteString("yaxis:{gridcolor:'rgba(255,255,255,0.05)',tickfont:{color:'#808098',size:10},title:{text:'Credits',font:{color:'#a0a0c0',size:10}}},")
		sb.WriteString("annotations:ann,margin:{t:40,b:45,l:50,r:20},height:320")
		sb.WriteString("},{responsive:true});\n})();\n</script>\n")
	} else {
		sb.WriteString(fmt.Sprintf("<div style=\"padding:20px;text-align:center;color:#a0a0c0\">"+
			"キャリア期間: %d — %d (%d年間活動)<br>ピーク: %d年 (%d credits)</div>\n",
			p.Career.FirstYear, p.Career.LatestYear, p.Career.ActiveYears,
			p.Career.PeakYear, p.Career.PeakCredits))
	}
	sb.WriteString("</div>\n")

	// --- Score Layers Summary ---
	sb.WriteString("<h2>Score Layers Summary</h2>\n<div class=\"card\">\n")

	type layerEntry struct {
		label, color, note string
		value, pct         float64
	}
	type layerGroup struct {
		title   string
		entries []layerEntry
	}

	seNote := ""
	if p.PersonFESE != nil {
		seNote = fmt.Sprintf("SE: %s", fmtF(*p.PersonFESE, 4))
	}

	groups := []layerGroup{
		{"Causal (因果的評価)", []layerEntry{
			{"Person FE (個人固定効果)", "#f093fb", seNote, p.PersonFE, p.PersonFEPct},
		}},
		{"Structural (構造的評価)", []layerEntry{
			{"BiRank", "#06D6A0", "", p.BiRank, p.BiRankPct},
			{"AWCC", "#a0d2db", "", p.AWCC, p.AWCCPct},
			{"NDI", "#667eea", "", p.NDI, 0},
		}},
		{"Collaboration (協業評価)", []layerEntry{
			{"Patronage (指名率)", "#FFD166", "", p.Patronage, p.PatronagePct},
			{"Studio Exposure", "#667eea", "", p.StudioFEExposure, d.StudioExpPct},
		}},
		{"Combined (統合)", []layerEntry{
			{"IV Score", "#f093fb", "", p.IVScore, p.IVScorePct},
			{"Dormancy", "#a0d2db", fmt.Sprintf("(multiplier: %s)", fmtF(p.Dormancy, 3)), p.Dormancy, p.DormancyPct},
		}},
	}

	for _, g := range groups {
		sb.WriteString("<h3>")
		sb.WriteString(html.EscapeString(g.title))
		sb.WriteString("</h3>\n")
		for _, e := range g.entries {
			pw := e.pct
			if pw < 0 {
				pw = 0
			}
			if pw > 100 {
				pw = 100
			}
			sb.WriteString("<div class=\"layer-row\">")
			sb.WriteString(fmt.Sprintf("<div class=\"layer-label\">%s</div>", html.EscapeString(e.label)))
			sb.WriteString(fmt.Sprintf("<div class=\"layer-value\" style=\"color:%s\">%s</div>", e.color, fmtF(e.value, 4)))
			sb.WriteString(fmt.Sprintf("<div class=\"layer-bar\"><div class=\"layer-bar-fill\" style=\"width:%.1f%%;background:%s\"></div></div>", pw, e.color))
			if e.note != "" {
				sb.WriteString(fmt.Sprintf("<div style=\"font-size:0.78rem;color:#808098;margin-left:8px;white-space:nowrap\">%s</div>", html.EscapeString(e.note)))
			}
			sb.WriteString("</div>\n")
		}
	}

	// IV weight composition
	if len(d.IVWeights) > 0 {
		sb.WriteString("<h3>IV Weight Composition</h3>\n")
		sb.WriteString("<div style=\"display:flex;gap:8px;flex-wrap:wrap;margin-top:6px\">\n")
		for _, w := range []struct{ key, label, color string }{
			{"person_fe", "Person FE", "#f093fb"},
			{"birank", "BiRank", "#06D6A0"},
			{"patronage", "Patronage", "#FFD166"},
			{"awcc", "AWCC", "#a0d2db"},
			{"studio_exposure", "Studio Exp.", "#667eea"},
		} {
			if val, ok := d.IVWeights[w.key]; ok {
				sb.WriteString(fmt.Sprintf("<div class=\"stat-box\" style=\"flex:1;min-width:120px\">"+
					"<div class=\"label\">%s</div>"+
					"<div class=\"value\" style=\"font-size:1.1rem;color:%s\">&lambda;=%s</div></div>\n",
					html.EscapeString(w.label), w.color, fmtF(val, 2)))
			}
		}
		sb.WriteString("</div>\n")
	}
	sb.WriteString("</div>\n")

	// --- Individual Profile ---
	if d.Profile != nil {
		sb.WriteString("<h2>Individual Contribution Profile</h2>\n<div class=\"card\"><div class=\"grid4\">\n")
		for _, m := range []struct{ label, value, color, sub string }{
			{"Peer Percentile", fmtF(d.Profile.PeerPercentile, 1), "#f093fb", "同コホート内順位"},
			{"Opportunity Residual", fmtF(d.Profile.OpportunityResidual, 3), "#06D6A0", "機会要因控除後"},
			{"Consistency", fmtF(d.Profile.Consistency, 3), "#667eea", "スコア安定性"},
			{"Independent Value", fmtF(d.Profile.IndependentValue, 3), "#FFD166", "協業者効果除外後"},
		} {
			sb.WriteString(fmt.Sprintf("<div class=\"stat-box\"><div class=\"label\">%s</div>"+
				"<div class=\"value\" style=\"font-size:1.1rem;color:%s\">%s</div>"+
				"<div class=\"sub\">%s</div></div>\n",
				html.EscapeString(m.label), m.color, m.value, html.EscapeString(m.sub)))
		}
		sb.WriteString("</div></div>\n")
	}

	// --- Milestones ---
	if len(d.Milestones) > 0 {
		sb.WriteString("<h2>Milestones</h2>\n<div class=\"card\">\n")
		shown := d.Milestones
		if len(shown) > 30 {
			shown = shown[:30]
		}
		for _, ms := range shown {
			desc := ms.Description
			if desc == "" {
				desc = ms.Type
			}
			sb.WriteString(fmt.Sprintf("<div class=\"milestone-item\">"+
				"<div class=\"milestone-year\">%d</div>"+
				"<div class=\"milestone-desc\">%s</div></div>\n",
				ms.Year, html.EscapeString(desc)))
		}
		if len(d.Milestones) > 30 {
			sb.WriteString(fmt.Sprintf("<div style=\"color:#808098;font-size:0.8rem;padding:8px 0 0 20px\">... and %d more milestones</div>\n", len(d.Milestones)-30))
		}
		sb.WriteString("</div>\n")
	}

	// --- Top Collaborators ---
	if len(d.Collabs) > 0 {
		sb.WriteString("<h2>Top Collaborators</h2>\n<div class=\"card\">\n")
		sb.WriteString("<table>\n<thead><tr><th>Name</th><th>Shared Works</th><th>Strength</th><th>Period</th></tr></thead>\n<tbody>\n")
		shown := d.Collabs
		if len(shown) > 20 {
			shown = shown[:20]
		}
		for _, c := range shown {
			partnerName := c.NameB
			partnerID := c.PersonB
			if c.PersonB == p.PersonID {
				partnerName = c.NameA
				partnerID = c.PersonA
			}
			if partnerName == "" {
				partnerName = partnerID
			}
			sb.WriteString(fmt.Sprintf("<tr><td><a href=\"%s.html\">%s</a></td><td>%d</td><td>%s</td><td>%d — %d</td></tr>\n",
				sanitizeFilename(partnerID), html.EscapeString(partnerName),
				c.SharedWorks, fmtF(c.StrengthScore, 3), c.FirstYear, c.LatestYear))
		}
		sb.WriteString("</tbody></table>\n</div>\n")
	}

	// --- Network & Growth ---
	sb.WriteString("<h2>Network &amp; Growth</h2>\n<div class=\"card\">\n<div class=\"grid3\">\n")
	writeStatBox(&sb, "Collaborators", fmt.Sprintf("%d", p.Network.Collaborators), "accent-teal", "")
	writeStatBox(&sb, "Unique Anime", fmt.Sprintf("%d", p.Network.UniqueAnime), "accent-blue", "")
	writeStatBox(&sb, "Hub Score", fmtF(p.Network.HubScore, 4), "accent-green", "")
	sb.WriteString("</div>\n<div class=\"grid4\" style=\"margin-top:14px\">\n")

	sb.WriteString(fmt.Sprintf("<div class=\"stat-box\"><div class=\"label\">Trend</div>"+
		"<div class=\"value accent-yellow\">%s</div><div class=\"sub\">%s</div></div>\n",
		html.EscapeString(trendNameJA(p.Growth.Trend)), html.EscapeString(p.Growth.Trend)))
	writeStatBox(&sb, "Activity Ratio", fmtF(p.Growth.ActivityRatio, 3), "accent-green", "")
	writeStatBox(&sb, "Recent Credits", fmt.Sprintf("%d", p.Growth.RecentCredits), "accent-pink", "")
	sb.WriteString(fmt.Sprintf("<div class=\"stat-box\"><div class=\"label\">Versatility</div>"+
		"<div class=\"value accent-teal\">%s</div><div class=\"sub\">%d categories / %d roles</div></div>\n",
		fmtF(p.Versatility.Score, 2), p.Versatility.Categories, p.Versatility.Roles))

	sb.WriteString("</div>\n</div>\n")

	// --- Additional Metrics ---
	sb.WriteString("<h2>Additional Metrics</h2>\n<div class=\"card\"><div class=\"grid4\">\n")
	for _, m := range []struct{ label, value string }{
		{"Career Friction", fmtF(p.CareerFriction, 4)},
		{"Peer Boost", fmtF(p.PeerBoost, 4)},
		{"Degree Centrality", fmtF(p.Centrality.Degree, 4)},
		{"Betweenness", fmtF(p.Centrality.Betweenness, 4)},
	} {
		sb.WriteString(fmt.Sprintf("<div class=\"stat-box\"><div class=\"label\">%s</div>"+
			"<div class=\"value\" style=\"color:#e0e0f0\">%s</div></div>\n",
			html.EscapeString(m.label), m.value))
	}
	sb.WriteString("</div></div>\n")

	// --- Disclaimer ---
	sb.WriteString("<div class=\"disclaimer\">\n")
	sb.WriteString("<strong>免責事項 / Disclaimer</strong><br>\n")
	sb.WriteString("本レポートは公開されたクレジットデータに基づく構造的分析であり、個人の能力を主観的に評価するものではありません。")
	sb.WriteString("スコアはネットワーク上の位置と協業密度を反映しており、「能力の高低」を示すものではありません。<br><br>\n")
	sb.WriteString("This report is a structural analysis based on publicly available credit data and does not constitute a subjective assessment of individual ability. ")
	sb.WriteString("Scores reflect network position and collaboration density, not ability rankings. ")
	sb.WriteString("Data source: publicly released anime credit records.\n</div>\n")

	// --- Footer ---
	sb.WriteString(fmt.Sprintf("<div style=\"text-align:center;color:#606078;font-size:0.75rem;margin-top:24px;padding-bottom:20px\">"+
		"Generated by Animetor Eval — Person History | %s</div>\n", time.Now().Format("2006-01-02")))
	sb.WriteString("</div>\n</body>\n</html>")

	return sb.String()
}

// writeStatBox is a helper that writes a standard stat-box div.
func writeStatBox(sb *strings.Builder, label, value, colorClass, sub string) {
	sb.WriteString(fmt.Sprintf("<div class=\"stat-box\"><div class=\"label\">%s</div>"+
		"<div class=\"value %s\">%s</div>", html.EscapeString(label), colorClass, value))
	if sub != "" {
		sb.WriteString(fmt.Sprintf("<div class=\"sub\">%s</div>", sub))
	}
	sb.WriteString("</div>\n")
}

// ---------------------------------------------------------------------------
// Index page generation
// ---------------------------------------------------------------------------

func generateIndexPage(persons []Person) string {
	var sb strings.Builder

	sb.WriteString("<!DOCTYPE html>\n<html lang=\"ja\">\n<head>\n<meta charset=\"UTF-8\">\n")
	sb.WriteString("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n")
	sb.WriteString("<title>Person History Index | Animetor Eval</title>\n")
	sb.WriteString("<style>\n")
	sb.WriteString(pageCSS)
	sb.WriteString("\n.search-box{width:100%;padding:10px 14px;background:rgba(255,255,255,0.06);")
	sb.WriteString("border:1px solid rgba(255,255,255,0.1);border-radius:8px;color:#e0e0f0;")
	sb.WriteString("font-size:0.95rem;margin-bottom:16px;outline:none}")
	sb.WriteString("\n.search-box:focus{border-color:#667eea}")
	sb.WriteString("\n.search-box::placeholder{color:#606078}")
	sb.WriteString("\n.count{color:#a0a0c0;font-size:0.85rem;margin-bottom:10px}")
	sb.WriteString("\nthead th{cursor:pointer;user-select:none;white-space:nowrap}")
	sb.WriteString("\nthead th:hover{color:#f093fb}")
	sb.WriteString("\nthead th .arrow{font-size:0.7rem;margin-left:4px;color:#f093fb}")
	sb.WriteString("\n</style>\n</head>\n<body>\n<div class=\"container\">\n")

	sb.WriteString("<h1 style=\"color:#f093fb\">Person History Index</h1>\n")
	sb.WriteString(fmt.Sprintf("<div class=\"subtitle\">個人プロフィールページ一覧 — Generated %s</div>\n", time.Now().Format("2006-01-02")))

	totalStr := fmt.Sprintf("%d", len(persons))
	sb.WriteString("<input type=\"text\" class=\"search-box\" id=\"search\" placeholder=\"Search by name...\" oninput=\"filterTable()\">\n")
	sb.WriteString(fmt.Sprintf("<div class=\"count\" id=\"count\">%s persons</div>\n", totalStr))

	// Table header
	sb.WriteString("<table id=\"ptable\">\n<thead><tr>\n")
	headers := []struct{ idx int; label string }{
		{0, "Rank"}, {1, "Name"}, {2, "Role"}, {3, "IV Score"},
		{4, "Person FE"}, {5, "BiRank"}, {6, "Career"}, {7, "Credits"}, {-1, "Tags"},
	}
	for _, h := range headers {
		if h.idx >= 0 {
			sb.WriteString(fmt.Sprintf("<th onclick=\"sortTable(%d)\">%s <span class=\"arrow\" id=\"a%d\">%s</span></th>",
				h.idx, h.label, h.idx, func() string {
					if h.idx == 0 {
						return "&#9660;"
					}
					return ""
				}()))
		} else {
			sb.WriteString(fmt.Sprintf("<th>%s</th>", h.label))
		}
	}
	sb.WriteString("\n</tr></thead>\n<tbody>\n")

	// Table body
	for i, p := range persons {
		displayName := p.Name
		if displayName == "" {
			displayName = p.NameEN
		}
		safeName := sanitizeFilename(p.PersonID)
		careerStr := ""
		if p.Career.FirstYear > 0 {
			careerStr = fmt.Sprintf("%d-%d", p.Career.FirstYear, p.Career.LatestYear)
		}
		searchData := strings.ToLower(p.Name + " " + p.NameJA + " " + p.NameEN)

		sb.WriteString(fmt.Sprintf("<tr data-s=\"%s\">", html.EscapeString(searchData)))
		sb.WriteString(fmt.Sprintf("<td>%d</td>", i+1))
		sb.WriteString(fmt.Sprintf("<td><a href=\"%s.html\">%s</a></td>", safeName, html.EscapeString(displayName)))
		sb.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(roleNameJA(p.PrimaryRole))))
		sb.WriteString(fmt.Sprintf("<td>%s</td>", fmtF(p.IVScore, 3)))
		sb.WriteString(fmt.Sprintf("<td>%s</td>", fmtF(p.PersonFE, 3)))
		sb.WriteString(fmt.Sprintf("<td>%s</td>", fmtF(p.BiRank, 3)))
		sb.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(careerStr)))
		sb.WriteString(fmt.Sprintf("<td>%d</td>", p.TotalCredits))
		sb.WriteString("<td>")
		for _, t := range p.Tags {
			sb.WriteString(fmt.Sprintf("<span class=\"tag\">%s</span>", html.EscapeString(t)))
		}
		sb.WriteString("</td></tr>\n")
	}

	sb.WriteString("</tbody>\n</table>\n")

	// Sort and filter script — uses textContent for safe DOM access
	sb.WriteString("<script>\n")
	sb.WriteString("var cs=0,asc=false;\n")
	sb.WriteString("function sortTable(c){\n")
	sb.WriteString("  var tb=document.getElementById('ptable').tBodies[0];\n")
	sb.WriteString("  var rows=Array.from(tb.rows);\n")
	sb.WriteString("  for(var i=0;i<9;i++){var a=document.getElementById('a'+i);if(a)a.textContent='';}\n")
	sb.WriteString("  if(cs===c){asc=!asc}else{cs=c;asc=(c<=2||c===6)}\n")
	sb.WriteString("  var ar=document.getElementById('a'+c);\n")
	sb.WriteString("  if(ar)ar.textContent=asc?'\\u25B2':'\\u25BC';\n")
	sb.WriteString("  rows.sort(function(a,b){\n")
	sb.WriteString("    var av=a.cells[c].textContent.trim(),bv=b.cells[c].textContent.trim();\n")
	sb.WriteString("    var an=parseFloat(av),bn=parseFloat(bv),r;\n")
	sb.WriteString("    if(!isNaN(an)&&!isNaN(bn)){r=an-bn}else{r=av.localeCompare(bv,'ja')}\n")
	sb.WriteString("    return asc?r:-r;\n")
	sb.WriteString("  });\n")
	sb.WriteString("  rows.forEach(function(r){tb.appendChild(r)});\n")
	sb.WriteString("}\n")
	sb.WriteString("function filterTable(){\n")
	sb.WriteString("  var q=document.getElementById('search').value.toLowerCase();\n")
	sb.WriteString("  var rows=document.querySelectorAll('#ptable tbody tr'),vis=0;\n")
	sb.WriteString("  rows.forEach(function(r){\n")
	sb.WriteString("    var d=r.getAttribute('data-s')||'';\n")
	sb.WriteString("    if(d.indexOf(q)>=0){r.style.display='';vis++}else{r.style.display='none'}\n")
	sb.WriteString("  });\n")
	sb.WriteString(fmt.Sprintf("  document.getElementById('count').textContent=vis+' / %s persons';\n", totalStr))
	sb.WriteString("}\n")
	sb.WriteString("</script>\n")

	// Disclaimer
	sb.WriteString("<div class=\"disclaimer\">\n")
	sb.WriteString("<strong>免責事項 / Disclaimer</strong><br>\n")
	sb.WriteString("本データは公開クレジット情報に基づく構造分析です。個人の能力を主観的に評価するものではありません。<br>\n")
	sb.WriteString("Scores reflect network position and collaboration density based on publicly available credit data, not ability rankings.\n</div>\n")

	sb.WriteString(fmt.Sprintf("<div style=\"text-align:center;color:#606078;font-size:0.75rem;margin-top:16px;padding-bottom:20px\">"+
		"Generated by Animetor Eval — Person History | %s</div>\n", time.Now().Format("2006-01-02")))
	sb.WriteString("</div>\n</body>\n</html>")

	return sb.String()
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	dataDir := flag.String("data", "result/json", "Path to JSON data directory")
	outDir := flag.String("out", "result/reports/persons", "Output directory for person HTML pages")
	topN := flag.Int("top", 200, "Generate pages for top N persons by IV score")
	idsFlag := flag.String("ids", "", "Comma-separated person IDs to generate (overrides -top)")
	flag.Parse()

	start := time.Now()
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[person_history] ")

	// -----------------------------------------------------------------------
	// 1. Load all data
	// -----------------------------------------------------------------------
	log.Println("Loading data...")

	var persons []Person
	if err := loadJSON(*dataDir, "scores.json", &persons); err != nil {
		log.Fatalf("Failed to load scores.json: %v", err)
	}
	log.Printf("  scores.json: %d persons", len(persons))

	personByID := make(map[string]*Person, len(persons))
	for i := range persons {
		personByID[persons[i].PersonID] = &persons[i]
	}
	_ = personByID

	// Collaborations
	var allCollabs []Collaboration
	if err := loadJSON(*dataDir, "collaborations.json", &allCollabs); err != nil {
		log.Printf("  collaborations.json: not found (skipping)")
	} else {
		log.Printf("  collaborations.json: %d pairs", len(allCollabs))
	}
	collabMap := make(map[string][]Collaboration)
	for _, c := range allCollabs {
		collabMap[c.PersonA] = append(collabMap[c.PersonA], c)
		collabMap[c.PersonB] = append(collabMap[c.PersonB], c)
	}

	// Individual profiles
	var profilesFile ProfilesFile
	if err := loadJSON(*dataDir, "individual_profiles.json", &profilesFile); err != nil {
		log.Printf("  individual_profiles.json: not found (skipping)")
	} else {
		log.Printf("  individual_profiles.json: %d profiles", len(profilesFile.Profiles))
	}

	// Milestones
	var milestonesMap map[string][]Milestone
	if err := loadJSON(*dataDir, "milestones.json", &milestonesMap); err != nil {
		log.Printf("  milestones.json: not found (skipping)")
	} else {
		log.Printf("  milestones.json: %d persons", len(milestonesMap))
	}

	// Growth
	var growthFile GrowthFile
	if err := loadJSON(*dataDir, "growth.json", &growthFile); err != nil {
		log.Printf("  growth.json: not found (skipping)")
	} else {
		log.Printf("  growth.json: %d persons", len(growthFile.Persons))
	}

	// IV weights
	var ivWeightsFile IVWeightsFile
	if err := loadJSON(*dataDir, "iv_weights.json", &ivWeightsFile); err != nil {
		log.Printf("  iv_weights.json: not found (skipping)")
	} else {
		log.Printf("  iv_weights.json: %d weights", len(ivWeightsFile.LambdaWeights))
	}

	log.Printf("Data loaded in %.2fs", time.Since(start).Seconds())

	// -----------------------------------------------------------------------
	// 2. Determine which persons to generate
	// -----------------------------------------------------------------------
	sort.Slice(persons, func(i, j int) bool {
		return persons[i].IVScore > persons[j].IVScore
	})

	var selectedIDs map[string]bool
	if *idsFlag != "" {
		ids := strings.Split(*idsFlag, ",")
		selectedIDs = make(map[string]bool, len(ids))
		for _, id := range ids {
			id = strings.TrimSpace(id)
			if id != "" {
				selectedIDs[id] = true
			}
		}
		log.Printf("Generating pages for %d specified person IDs", len(selectedIDs))
	} else {
		n := *topN
		if n > len(persons) {
			n = len(persons)
		}
		selectedIDs = make(map[string]bool, n)
		for i := 0; i < n; i++ {
			selectedIDs[persons[i].PersonID] = true
		}
		log.Printf("Generating pages for top %d persons by IV score", n)
	}

	var selected []Person
	for _, p := range persons {
		if selectedIDs[p.PersonID] {
			selected = append(selected, p)
		}
	}

	if len(selected) == 0 {
		log.Println("No persons selected. Exiting.")
		return
	}

	// -----------------------------------------------------------------------
	// 3. Pre-compute population histograms (50 bins)
	// -----------------------------------------------------------------------
	log.Println("Pre-computing population histograms...")

	allPersonFE := make([]float64, len(persons))
	allBiRank := make([]float64, len(persons))
	allPatronage := make([]float64, len(persons))
	allIVScore := make([]float64, len(persons))
	allStudioExp := make([]float64, len(persons))
	for i, p := range persons {
		allPersonFE[i] = p.PersonFE
		allBiRank[i] = p.BiRank
		allPatronage[i] = p.Patronage
		allIVScore[i] = p.IVScore
		allStudioExp[i] = p.StudioFEExposure
	}

	const numBins = 50
	histPersonFE := computeHistogram(allPersonFE, numBins)
	histBiRank := computeHistogram(allBiRank, numBins)
	histPatronage := computeHistogram(allPatronage, numBins)
	histIVScore := computeHistogram(allIVScore, numBins)

	// Compute studio_fe_exposure percentile for each person
	sortedStudioExp := make([]float64, len(allStudioExp))
	copy(sortedStudioExp, allStudioExp)
	sort.Float64s(sortedStudioExp)

	studioExpPctMap := make(map[string]float64, len(persons))
	n := len(sortedStudioExp)
	for _, p := range persons {
		rank := sort.SearchFloat64s(sortedStudioExp, p.StudioFEExposure)
		pct := float64(rank) / float64(n) * 100
		if pct > 100 {
			pct = 100
		}
		studioExpPctMap[p.PersonID] = pct
	}

	log.Println("Histograms computed.")

	// -----------------------------------------------------------------------
	// 4. Create output directory
	// -----------------------------------------------------------------------
	if err := os.MkdirAll(*outDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// -----------------------------------------------------------------------
	// 5. Generate pages in parallel
	// -----------------------------------------------------------------------
	log.Printf("Generating %d person pages...", len(selected))
	genStart := time.Now()

	workers := runtime.NumCPU()
	if workers < 1 {
		workers = 1
	}
	if workers > 16 {
		workers = 16
	}

	var completed int64
	total := int64(len(selected))

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var errCount int64

	for _, p := range selected {
		wg.Add(1)
		go func(p Person) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Gather per-person data
			pCollabs := collabMap[p.PersonID]
			sort.Slice(pCollabs, func(i, j int) bool {
				return pCollabs[i].StrengthScore > pCollabs[j].StrengthScore
			})

			var pProfile *IndividualProfile
			if prof, ok := profilesFile.Profiles[p.PersonID]; ok {
				pProfile = &prof
			}

			pd := personPageData{
				P:             p,
				Collabs:       pCollabs,
				Profile:       pProfile,
				Milestones:    milestonesMap[p.PersonID],
				IVWeights:     ivWeightsFile.LambdaWeights,
				HistPersonFE:  histPersonFE,
				HistBiRank:    histBiRank,
				HistPatronage: histPatronage,
				HistIVScore:   histIVScore,
				StudioExpPct:  studioExpPctMap[p.PersonID],
			}
			if gp, ok := growthFile.Persons[p.PersonID]; ok {
				pd.GrowthData = &gp
			}

			pageHTML := generatePersonPage(pd)

			filename := filepath.Join(*outDir, sanitizeFilename(p.PersonID)+".html")
			if err := os.WriteFile(filename, []byte(pageHTML), 0644); err != nil {
				log.Printf("ERROR writing %s: %v", filename, err)
				atomic.AddInt64(&errCount, 1)
				return
			}

			done := atomic.AddInt64(&completed, 1)
			if done%50 == 0 || done == total {
				log.Printf("Generated %d/%d person pages...", done, total)
			}
		}(p)
	}

	wg.Wait()

	log.Printf("All %d pages generated in %.2fs (%d errors)",
		len(selected), time.Since(genStart).Seconds(), errCount)

	// -----------------------------------------------------------------------
	// 6. Generate index page
	// -----------------------------------------------------------------------
	log.Println("Generating index page...")
	indexHTML := generateIndexPage(selected)
	indexPath := filepath.Join(*outDir, "index.html")
	if err := os.WriteFile(indexPath, []byte(indexHTML), 0644); err != nil {
		log.Fatalf("Failed to write index: %v", err)
	}
	log.Printf("Index page: %s", indexPath)

	log.Printf("Done. Total time: %.2fs", time.Since(start).Seconds())
	log.Printf("Output: %s/ (%d pages + index)", *outDir, len(selected))
}
