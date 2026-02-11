/**
 * Animetor Eval — Portfolio SPA
 *
 * Vanilla JS frontend for person search, profile view, and ranking.
 * All DOM manipulation uses safe methods (createElement, textContent).
 * No innerHTML with untrusted data.
 */

// ========================================================================
// i18n
// ========================================================================
const TRANSLATIONS = {
    en: {
        nav_search: "Search", nav_ranking: "Ranking",
        search_title: "Animetor Eval",
        search_subtitle: "Search anime industry professionals by name",
        search_placeholder: "Enter name (EN or JA)...",
        search_button: "Search", search_no_results: "No results found",
        ranking_title: "Ranking", ranking_all_roles: "All Roles",
        rank_col: "#", name_col: "Name", role_col: "Role",
        composite_col: "Composite", authority_col: "Authority",
        trust_col: "Trust", skill_col: "Skill",
        profile_network: "Network Profile",
        profile_contribution: "Individual Contribution",
        profile_similar: "Similar Persons",
        profile_explanation: "Score Explanation",
        score_composite: "Composite", score_authority: "Authority",
        score_trust: "Trust", score_skill: "Skill",
        metric_percentile: "Peer Percentile",
        metric_residual: "Opportunity Residual",
        metric_consistency: "Consistency",
        metric_independent: "Independent Value",
        desc_percentile: "Ranking within same role and career stage cohort",
        desc_residual: "Contribution beyond what opportunity factors predict",
        desc_consistency: "Score stability across different works",
        desc_independent: "Value independent of collaborator quality",
        similarity: "Similarity",
        disclaimer_title: "Disclaimer",
        disclaimer_text: "Scores reflect network position and collaboration density, not individual ability. This data is derived from publicly available credit information of released works.",
        loading: "Loading...",
    },
    ja: {
        nav_search: "\u691C\u7D22", nav_ranking: "\u30E9\u30F3\u30AD\u30F3\u30B0",
        search_title: "Animetor Eval",
        search_subtitle: "\u30A2\u30CB\u30E1\u696D\u754C\u306E\u30D7\u30ED\u30D5\u30A7\u30C3\u30B7\u30E7\u30CA\u30EB\u3092\u540D\u524D\u3067\u691C\u7D22",
        search_placeholder: "\u540D\u524D\u3092\u5165\u529B\uFF08\u65E5\u672C\u8A9E\u30FB\u82F1\u8A9E\uFF09...",
        search_button: "\u691C\u7D22",
        search_no_results: "\u7D50\u679C\u304C\u898B\u3064\u304B\u308A\u307E\u305B\u3093",
        ranking_title: "\u30E9\u30F3\u30AD\u30F3\u30B0",
        ranking_all_roles: "\u5168\u5F79\u8077",
        rank_col: "#", name_col: "\u6C0F\u540D", role_col: "\u5F79\u8077",
        composite_col: "\u7DCF\u5408", authority_col: "\u6A29\u5A01\u6027",
        trust_col: "\u4FE1\u983C\u6027", skill_col: "\u6280\u8853\u529B",
        profile_network: "\u30CD\u30C3\u30C8\u30EF\u30FC\u30AF\u30D7\u30ED\u30D5\u30A1\u30A4\u30EB",
        profile_contribution: "\u500B\u4EBA\u8CA2\u732E\u30D7\u30ED\u30D5\u30A1\u30A4\u30EB",
        profile_similar: "\u985E\u4F3C\u4EBA\u7269",
        profile_explanation: "\u30B9\u30B3\u30A2\u89E3\u8AAC",
        score_composite: "\u7DCF\u5408", score_authority: "\u6A29\u5A01\u6027",
        score_trust: "\u4FE1\u983C\u6027", score_skill: "\u6280\u8853\u529B",
        metric_percentile: "\u30D4\u30A2\u30D1\u30FC\u30BB\u30F3\u30BF\u30A4\u30EB",
        metric_residual: "\u6A5F\u4F1A\u7D71\u5236\u6B8B\u5DEE",
        metric_consistency: "\u4E00\u8CAB\u6027",
        metric_independent: "\u72EC\u7ACB\u8CA2\u732E\u5EA6",
        desc_percentile: "\u540C\u4E00\u5F79\u8077\u30FB\u540C\u4E00\u30AD\u30E3\u30EA\u30A2\u30B9\u30C6\u30FC\u30B8\u306E\u30B3\u30DB\u30FC\u30C8\u5185\u3067\u306E\u9806\u4F4D",
        desc_residual: "\u6A5F\u4F1A\u8981\u56E0\u3092\u7D71\u5236\u3057\u305F\u4E0A\u3067\u306E\u72EC\u81EA\u306E\u8CA2\u732E",
        desc_consistency: "\u4F5C\u54C1\u9593\u3067\u306E\u30B9\u30B3\u30A2\u5B89\u5B9A\u6027",
        desc_independent: "\u30B3\u30E9\u30DC\u30EC\u30FC\u30BF\u30FC\u306E\u8CEA\u306B\u4F9D\u5B58\u3057\u306A\u3044\u72EC\u81EA\u306E\u4FA1\u5024",
        similarity: "\u985E\u4F3C\u5EA6",
        disclaimer_title: "\u514D\u8CAC\u4E8B\u9805",
        disclaimer_text: "\u30B9\u30B3\u30A2\u306F\u30CD\u30C3\u30C8\u30EF\u30FC\u30AF\u4E0A\u306E\u4F4D\u7F6E\u3068\u5354\u529B\u5BC6\u5EA6\u3092\u53CD\u6620\u3059\u308B\u3082\u306E\u3067\u3042\u308A\u3001\u500B\u4EBA\u306E\u80FD\u529B\u3092\u793A\u3059\u3082\u306E\u3067\u306F\u3042\u308A\u307E\u305B\u3093\u3002\u672C\u30C7\u30FC\u30BF\u306F\u516C\u958B\u3055\u308C\u305F\u30AF\u30EC\u30B8\u30C3\u30C8\u60C5\u5831\u306B\u57FA\u3065\u3044\u3066\u3044\u307E\u3059\u3002",
        loading: "\u8AAD\u307F\u8FBC\u307F\u4E2D...",
    },
};

let currentLang = localStorage.getItem("animetor_lang") || "ja";

function t(key) {
    return (TRANSLATIONS[currentLang] || TRANSLATIONS.ja)[key] || key;
}

function applyI18n() {
    document.querySelectorAll("[data-i18n]").forEach(function (el) {
        el.textContent = t(el.getAttribute("data-i18n"));
    });
    document.querySelectorAll("[data-i18n-placeholder]").forEach(function (el) {
        el.placeholder = t(el.getAttribute("data-i18n-placeholder"));
    });
    document.querySelectorAll(".lang-btn").forEach(function (btn) {
        btn.classList.toggle("active", btn.getAttribute("data-lang") === currentLang);
    });
}

document.querySelectorAll(".lang-btn").forEach(function (btn) {
    btn.addEventListener("click", function () {
        currentLang = btn.getAttribute("data-lang");
        localStorage.setItem("animetor_lang", currentLang);
        applyI18n();
        var hash = location.hash || "#search";
        if (hash.startsWith("#profile/")) loadProfile(hash.substring(9));
        if (hash === "#ranking") loadRanking();
    });
});

// ========================================================================
// Router
// ========================================================================
function navigate(hash) { location.hash = hash; }

function handleRoute() {
    var hash = location.hash || "#search";
    document.querySelectorAll(".page").forEach(function (p) { p.classList.remove("active"); });
    document.querySelectorAll(".nav-link").forEach(function (a) { a.classList.remove("active"); });

    if (hash.startsWith("#profile/")) {
        document.getElementById("page-profile").classList.add("active");
        loadProfile(decodeURIComponent(hash.substring(9)));
    } else if (hash === "#ranking") {
        document.getElementById("page-ranking").classList.add("active");
        document.querySelector('[data-page="ranking"]').classList.add("active");
        loadRanking();
    } else {
        document.getElementById("page-search").classList.add("active");
        document.querySelector('[data-page="search"]').classList.add("active");
    }
}

window.addEventListener("hashchange", handleRoute);

// ========================================================================
// API
// ========================================================================
var API_BASE = "/api";

function api(path) {
    return fetch(API_BASE + path).then(function (resp) {
        if (!resp.ok) throw new Error(resp.statusText);
        return resp.json();
    });
}

function fmtScore(v) {
    if (v == null) return "\u2014";
    return (typeof v === "number") ? v.toFixed(2) : String(v);
}

function clearChildren(el) {
    while (el.firstChild) el.removeChild(el.firstChild);
}

function showLoading(container) {
    clearChildren(container);
    var div = document.createElement("div");
    div.className = "loading";
    var sp = document.createElement("div");
    sp.className = "spinner";
    div.appendChild(sp);
    var p = document.createElement("p");
    p.textContent = t("loading");
    div.appendChild(p);
    container.appendChild(div);
}

// ========================================================================
// Search
// ========================================================================
var searchInput = document.getElementById("searchInput");
var searchBtn = document.getElementById("searchBtn");
var searchResults = document.getElementById("searchResults");

searchBtn.addEventListener("click", doSearch);
searchInput.addEventListener("keydown", function (e) { if (e.key === "Enter") doSearch(); });

function doSearch() {
    var q = searchInput.value.trim();
    if (!q) return;
    showLoading(searchResults);

    api("/persons/search?q=" + encodeURIComponent(q) + "&limit=20").then(function (data) {
        clearChildren(searchResults);
        if (!data.results || data.results.length === 0) {
            searchResults.textContent = t("search_no_results");
            return;
        }
        var card = document.createElement("div");
        card.className = "card";
        data.results.forEach(function (r) {
            var item = document.createElement("div");
            item.className = "search-item";
            var nameDiv = document.createElement("div");
            nameDiv.className = "name";
            nameDiv.textContent = r.name_ja || r.name_en || r.id;
            if (r.name_en && r.name_ja) {
                var small = document.createElement("small");
                small.textContent = r.name_en;
                nameDiv.appendChild(small);
            }
            item.appendChild(nameDiv);
            if (r.composite != null) {
                var sc = document.createElement("div");
                sc.className = "score";
                sc.textContent = fmtScore(r.composite);
                item.appendChild(sc);
            }
            item.addEventListener("click", function () { navigate("#profile/" + encodeURIComponent(r.id)); });
            card.appendChild(item);
        });
        searchResults.appendChild(card);
    }).catch(function (err) {
        clearChildren(searchResults);
        searchResults.textContent = "Error: " + err.message;
    });
}

// ========================================================================
// Profile
// ========================================================================
function loadProfile(personId) {
    var content = document.getElementById("profileContent");
    showLoading(content);

    Promise.allSettled([
        api("/persons/" + encodeURIComponent(personId) + "/profile"),
        api("/persons/" + encodeURIComponent(personId) + "/similar?top_n=5"),
    ]).then(function (results) {
        var profile = results[0].status === "fulfilled" ? results[0].value : null;
        var similar = results[1].status === "fulfilled" ? results[1].value : null;

        clearChildren(content);

        if (!profile) {
            content.textContent = "Person not found: " + personId;
            return;
        }

        var np = profile.network_profile || {};

        // Header card
        var headerCard = document.createElement("div");
        headerCard.className = "card";

        var header = document.createElement("div");
        header.className = "profile-header";
        var info = document.createElement("div");
        info.className = "info";

        var h1 = document.createElement("h1");
        h1.textContent = np.name_ja || np.name_en || personId;
        info.appendChild(h1);

        if (np.name_en && np.name_ja) {
            var sub = document.createElement("div");
            sub.className = "subtitle";
            sub.textContent = np.name_en;
            info.appendChild(sub);
        }

        var badge = document.createElement("span");
        badge.className = "id-badge";
        badge.textContent = personId;
        info.appendChild(badge);

        if (np.primary_role) {
            var roleBadge = document.createElement("span");
            roleBadge.className = "id-badge";
            roleBadge.style.marginLeft = "8px";
            roleBadge.textContent = np.primary_role;
            info.appendChild(roleBadge);
        }

        header.appendChild(info);
        headerCard.appendChild(header);

        // Score grid
        var scoreSection = document.createElement("div");
        scoreSection.style.marginTop = "20px";
        var scoreGrid = document.createElement("div");
        scoreGrid.className = "score-grid";

        [
            { key: "composite", label: t("score_composite"), cls: "composite" },
            { key: "authority", label: t("score_authority"), cls: "" },
            { key: "trust", label: t("score_trust"), cls: "" },
            { key: "skill", label: t("score_skill"), cls: "" },
        ].forEach(function (ax) {
            var cell = document.createElement("div");
            cell.className = "score-cell" + (ax.cls ? " " + ax.cls : "");
            var lbl = document.createElement("div");
            lbl.className = "label";
            lbl.textContent = ax.label;
            var val = document.createElement("div");
            val.className = "value";
            val.textContent = fmtScore(np[ax.key]);
            cell.appendChild(lbl);
            cell.appendChild(val);
            scoreGrid.appendChild(cell);
        });
        scoreSection.appendChild(scoreGrid);
        headerCard.appendChild(scoreSection);

        // Bar chart
        var barSection = document.createElement("div");
        barSection.style.marginTop = "20px";
        var barChart = document.createElement("div");
        barChart.className = "bar-chart";

        [
            { key: "authority", label: t("score_authority"), cls: "authority" },
            { key: "trust", label: t("score_trust"), cls: "trust" },
            { key: "skill", label: t("score_skill"), cls: "skill" },
            { key: "composite", label: t("score_composite"), cls: "composite" },
        ].forEach(function (ax) {
            var row = document.createElement("div");
            row.className = "bar-row";
            var label = document.createElement("div");
            label.className = "bar-label";
            label.textContent = ax.label;
            var track = document.createElement("div");
            track.className = "bar-track";
            var fill = document.createElement("div");
            fill.className = "bar-fill " + ax.cls;
            var rawVal = np[ax.key] || 0;
            fill.style.width = Math.min(rawVal, 100) + "%";
            track.appendChild(fill);
            var valDiv = document.createElement("div");
            valDiv.className = "bar-value";
            valDiv.textContent = fmtScore(rawVal);
            row.appendChild(label);
            row.appendChild(track);
            row.appendChild(valDiv);
            barChart.appendChild(row);
        });
        barSection.appendChild(barChart);
        headerCard.appendChild(barSection);
        content.appendChild(headerCard);

        // Individual Contribution Profile
        var ip = profile.individual_profile;
        if (ip) {
            var contribCard = document.createElement("div");
            contribCard.className = "card";
            var h2c = document.createElement("h2");
            h2c.textContent = t("profile_contribution");
            contribCard.appendChild(h2c);

            var grid = document.createElement("div");
            grid.className = "contrib-grid";

            [
                { key: "peer_percentile", label: t("metric_percentile"), desc: t("desc_percentile"), fmt: fmtScore },
                { key: "opportunity_residual", label: t("metric_residual"), desc: t("desc_residual"), fmt: function (v) { return (v > 0 ? "+" : "") + fmtScore(v); } },
                { key: "consistency", label: t("metric_consistency"), desc: t("desc_consistency"), fmt: fmtScore },
                { key: "independent_value", label: t("metric_independent"), desc: t("desc_independent"), fmt: fmtScore },
            ].forEach(function (m) {
                var mc = document.createElement("div");
                mc.className = "contrib-card";
                var mn = document.createElement("div");
                mn.className = "metric-name";
                mn.textContent = m.label;
                var mv = document.createElement("div");
                mv.className = "metric-value";
                mv.textContent = ip[m.key] != null ? m.fmt(ip[m.key]) : "\u2014";
                var md = document.createElement("div");
                md.className = "metric-desc";
                md.textContent = m.desc;
                mc.appendChild(mn);
                mc.appendChild(mv);
                mc.appendChild(md);
                grid.appendChild(mc);
            });
            contribCard.appendChild(grid);
            content.appendChild(contribCard);
        }

        // Similar Persons
        if (similar && similar.similar && similar.similar.length > 0) {
            var simCard = document.createElement("div");
            simCard.className = "card";
            var h2s = document.createElement("h2");
            h2s.textContent = t("profile_similar");
            simCard.appendChild(h2s);

            var list = document.createElement("div");
            list.className = "similar-list";
            similar.similar.forEach(function (s) {
                var item = document.createElement("div");
                item.className = "similar-item";
                var name = document.createElement("div");
                name.className = "name";
                name.textContent = s.name_ja || s.name_en || s.person_id;
                var sim = document.createElement("div");
                sim.className = "similarity";
                sim.textContent = t("similarity") + ": " + fmtScore(s.similarity);
                item.appendChild(name);
                item.appendChild(sim);
                item.addEventListener("click", function () { navigate("#profile/" + encodeURIComponent(s.person_id)); });
                list.appendChild(item);
            });
            simCard.appendChild(list);
            content.appendChild(simCard);
        }

        // Explanation
        if (profile.explanation) {
            var explCard = document.createElement("div");
            explCard.className = "card";
            var h2e = document.createElement("h2");
            h2e.textContent = t("profile_explanation");
            explCard.appendChild(h2e);

            var explDiv = document.createElement("div");
            explDiv.style.fontSize = "14px";
            explDiv.style.lineHeight = "1.8";

            var expl = profile.explanation;
            if (Array.isArray(expl)) {
                expl.forEach(function (e) {
                    var p = document.createElement("p");
                    p.style.marginBottom = "8px";
                    p.textContent = e.text || e.description || JSON.stringify(e);
                    explDiv.appendChild(p);
                });
            } else if (typeof expl === "object") {
                Object.entries(expl).forEach(function (pair) {
                    var p = document.createElement("p");
                    p.style.marginBottom = "8px";
                    var strong = document.createElement("strong");
                    strong.textContent = pair[0] + ": ";
                    p.appendChild(strong);
                    p.appendChild(document.createTextNode(typeof pair[1] === "string" ? pair[1] : JSON.stringify(pair[1])));
                    explDiv.appendChild(p);
                });
            }
            explCard.appendChild(explDiv);
            content.appendChild(explCard);
        }

        // Disclaimer
        var disclaimer = document.createElement("div");
        disclaimer.className = "disclaimer";
        var dt = document.createElement("strong");
        dt.textContent = t("disclaimer_title") + ": ";
        disclaimer.appendChild(dt);
        disclaimer.appendChild(document.createTextNode(t("disclaimer_text")));
        content.appendChild(disclaimer);
    }).catch(function (err) {
        clearChildren(content);
        content.textContent = "Error loading profile: " + err.message;
    });
}

// ========================================================================
// Ranking
// ========================================================================
function loadRanking() {
    var container = document.getElementById("rankingTable");
    var sortBy = document.getElementById("rankSort").value;
    var roleFilter = document.getElementById("rankRole").value;

    showLoading(container);

    var url = "/ranking?sort=" + sortBy + "&per_page=100";
    if (roleFilter) url += "&role=" + roleFilter;

    api(url).then(function (data) {
        clearChildren(container);
        var items = data.items || data.results || data;
        if (!Array.isArray(items) || items.length === 0) {
            container.textContent = t("search_no_results");
            return;
        }

        var table = document.createElement("table");
        var thead = document.createElement("thead");
        var headerRow = document.createElement("tr");
        [t("rank_col"), t("name_col"), t("role_col"), t("composite_col"), t("authority_col"), t("trust_col"), t("skill_col")].forEach(function (col) {
            var th = document.createElement("th");
            th.textContent = col;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        var tbody = document.createElement("tbody");
        items.forEach(function (entry, i) {
            var tr = document.createElement("tr");
            tr.className = "clickable";
            tr.addEventListener("click", function () { navigate("#profile/" + encodeURIComponent(entry.person_id)); });

            var td1 = document.createElement("td");
            td1.textContent = i + 1;
            tr.appendChild(td1);

            var td2 = document.createElement("td");
            td2.className = "person-name";
            td2.textContent = entry.name_ja || entry.name_en || entry.person_id;
            tr.appendChild(td2);

            var td3 = document.createElement("td");
            td3.textContent = entry.primary_role || "\u2014";
            tr.appendChild(td3);

            [entry.composite, entry.authority, entry.trust, entry.skill].forEach(function (v) {
                var td = document.createElement("td");
                td.textContent = fmtScore(v);
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        container.appendChild(table);
    }).catch(function (err) {
        clearChildren(container);
        container.textContent = "Error: " + err.message;
    });
}

document.getElementById("rankSort").addEventListener("change", loadRanking);
document.getElementById("rankRole").addEventListener("change", loadRanking);

// ========================================================================
// Init
// ========================================================================
applyI18n();
handleRoute();
