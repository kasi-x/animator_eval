/**
 * ANIMETOR — Netflix-Inspired Portfolio SPA
 *
 * Vanilla JS frontend. All DOM manipulation uses safe methods
 * (createElement, textContent). No innerHTML with untrusted data.
 */

// ========================================================================
// i18n
// ========================================================================
var TRANSLATIONS = {
    en: {
        nav_home: "Discover",
        nav_ranking: "Ranking",
        hero_title: "Discover Anime Professionals",
        hero_subtitle: "Explore the anime industry's collaboration network",
        search_placeholder: "Search by name (EN or JA)...",
        row_top_rated: "Top Rated",
        row_collaborative: "Most Collaborative",
        row_rising: "Rising Careers",
        search_no_results: "No results found",
        search_results_for: "Results for",
        profile_network: "Network Profile",
        profile_contribution: "Individual Contribution",
        profile_similar: "Similar Persons",
        profile_radar: "Radar Chart",
        profile_ego: "Ego Graph",
        profile_explanation: "Score Explanation",
        score_iv: "IV Score",
        score_birank: "BiRank",
        score_patronage: "Patronage",
        score_person_fe: "Person FE",
        metric_percentile: "Peer Percentile",
        metric_residual: "Opportunity Residual",
        metric_consistency: "Consistency",
        metric_independent: "Independent Value",
        desc_percentile: "Ranking within same role and career stage cohort",
        desc_residual: "Contribution beyond what opportunity factors predict",
        desc_consistency: "Score stability across different works",
        desc_independent: "Value independent of collaborator quality",
        rank_col: "#",
        name_col: "Name",
        role_col: "Role",
        ranking_all_roles: "All Roles",
        similarity: "Similarity",
        disclaimer_title: "Disclaimer",
        disclaimer_text: "Scores reflect network position and collaboration density, not individual ability. This data is derived from publicly available credit information of released works.",
        loading: "Loading...",
        back: "Back",
        not_found: "Person not found",
        hub_score: "Hub",
        versatility: "Versatility",
        confidence: "Confidence",
        credits: "Credits",
        nav_reports: "Reports",
        reports_title: "Analysis Reports",
        reports_subtitle: "19 deep-dive reports generated from pipeline data",
        row_reports: "Analysis Reports",
    },
    ja: {
        nav_home: "\u63A2\u3059",
        nav_ranking: "\u30E9\u30F3\u30AD\u30F3\u30B0",
        hero_title: "\u30A2\u30CB\u30E1\u696D\u754C\u306E\u30D7\u30ED\u30D5\u30A7\u30C3\u30B7\u30E7\u30CA\u30EB\u3092\u767A\u898B",
        hero_subtitle: "\u30B3\u30E9\u30DC\u30EC\u30FC\u30B7\u30E7\u30F3\u30CD\u30C3\u30C8\u30EF\u30FC\u30AF\u3092\u63A2\u7D22",
        search_placeholder: "\u540D\u524D\u3092\u5165\u529B\uFF08\u65E5\u672C\u8A9E\u30FB\u82F1\u8A9E\uFF09...",
        row_top_rated: "\u30C8\u30C3\u30D7\u30EC\u30FC\u30C6\u30A3\u30F3\u30B0",
        row_collaborative: "\u6700\u591A\u30B3\u30E9\u30DC\u30EC\u30FC\u30B7\u30E7\u30F3",
        row_rising: "\u30AD\u30E3\u30EA\u30A2\u4E0A\u6607\u4E2D",
        search_no_results: "\u7D50\u679C\u304C\u898B\u3064\u304B\u308A\u307E\u305B\u3093",
        search_results_for: "\u691C\u7D22\u7D50\u679C",
        profile_network: "\u30CD\u30C3\u30C8\u30EF\u30FC\u30AF\u30D7\u30ED\u30D5\u30A1\u30A4\u30EB",
        profile_contribution: "\u500B\u4EBA\u8CA2\u732E\u30D7\u30ED\u30D5\u30A1\u30A4\u30EB",
        profile_similar: "\u985E\u4F3C\u4EBA\u7269",
        profile_radar: "\u30EC\u30FC\u30C0\u30FC\u30C1\u30E3\u30FC\u30C8",
        profile_ego: "\u30A8\u30B4\u30B0\u30E9\u30D5",
        profile_explanation: "\u30B9\u30B3\u30A2\u89E3\u8AAC",
        score_iv: "IV\u30B9\u30B3\u30A2",
        score_birank: "BiRank",
        score_patronage: "Patronage",
        score_person_fe: "\u500B\u4EBAFE",
        metric_percentile: "\u30D4\u30A2\u30D1\u30FC\u30BB\u30F3\u30BF\u30A4\u30EB",
        metric_residual: "\u6A5F\u4F1A\u7D71\u5236\u6B8B\u5DEE",
        metric_consistency: "\u4E00\u8CAB\u6027",
        metric_independent: "\u72EC\u7ACB\u8CA2\u732E\u5EA6",
        desc_percentile: "\u540C\u4E00\u5F79\u8077\u30FB\u540C\u4E00\u30AD\u30E3\u30EA\u30A2\u30B9\u30C6\u30FC\u30B8\u306E\u30B3\u30DB\u30FC\u30C8\u5185\u3067\u306E\u9806\u4F4D",
        desc_residual: "\u6A5F\u4F1A\u8981\u56E0\u3092\u7D71\u5236\u3057\u305F\u4E0A\u3067\u306E\u72EC\u81EA\u306E\u8CA2\u732E",
        desc_consistency: "\u4F5C\u54C1\u9593\u3067\u306E\u30B9\u30B3\u30A2\u5B89\u5B9A\u6027",
        desc_independent: "\u30B3\u30E9\u30DC\u30EC\u30FC\u30BF\u30FC\u306E\u8CEA\u306B\u4F9D\u5B58\u3057\u306A\u3044\u72EC\u81EA\u306E\u4FA1\u5024",
        rank_col: "#",
        name_col: "\u6C0F\u540D",
        role_col: "\u5F79\u8077",
        ranking_all_roles: "\u5168\u5F79\u8077",
        similarity: "\u985E\u4F3C\u5EA6",
        disclaimer_title: "\u514D\u8CAC\u4E8B\u9805",
        disclaimer_text: "\u30B9\u30B3\u30A2\u306F\u30CD\u30C3\u30C8\u30EF\u30FC\u30AF\u4E0A\u306E\u4F4D\u7F6E\u3068\u5354\u529B\u5BC6\u5EA6\u3092\u53CD\u6620\u3059\u308B\u3082\u306E\u3067\u3042\u308A\u3001\u500B\u4EBA\u306E\u80FD\u529B\u3092\u793A\u3059\u3082\u306E\u3067\u306F\u3042\u308A\u307E\u305B\u3093\u3002\u672C\u30C7\u30FC\u30BF\u306F\u516C\u958B\u3055\u308C\u305F\u30AF\u30EC\u30B8\u30C3\u30C8\u60C5\u5831\u306B\u57FA\u3065\u3044\u3066\u3044\u307E\u3059\u3002",
        loading: "\u8AAD\u307F\u8FBC\u307F\u4E2D...",
        back: "\u623B\u308B",
        not_found: "\u4EBA\u7269\u304C\u898B\u3064\u304B\u308A\u307E\u305B\u3093",
        hub_score: "\u30CF\u30D6",
        versatility: "\u591A\u69D8\u6027",
        confidence: "\u78BA\u4FE1\u5EA6",
        credits: "\u30AF\u30EC\u30B8\u30C3\u30C8",
        nav_reports: "\u30EC\u30DD\u30FC\u30C8",
        reports_title: "\u5206\u6790\u30EC\u30DD\u30FC\u30C8",
        reports_subtitle: "\u30D1\u30A4\u30D7\u30E9\u30A4\u30F3\u30C7\u30FC\u30BF\u304B\u3089\u751F\u6210\u3055\u308C\u305F19\u306E\u6DF1\u6398\u308A\u30EC\u30DD\u30FC\u30C8",
        row_reports: "\u5206\u6790\u30EC\u30DD\u30FC\u30C8",
    },
};

var currentLang = localStorage.getItem("animetor_lang") || "ja";

function t(key) {
    return (TRANSLATIONS[currentLang] || TRANSLATIONS.ja)[key] || key;
}

// ========================================================================
// Utilities
// ========================================================================
function fmtScore(v, digits) {
    if (v == null) return "\u2014";
    if (typeof v !== "number") return String(v);
    if (digits != null) return v.toFixed(digits);
    // Smart formatting: more decimals for small values
    var abs = Math.abs(v);
    if (abs >= 10) return v.toFixed(1);
    if (abs >= 1) return v.toFixed(2);
    if (abs >= 0.01) return v.toFixed(3);
    return v.toFixed(4);
}

function clearChildren(node) {
    while (node.firstChild) node.removeChild(node.firstChild);
}

function enc(str) {
    return encodeURIComponent(str);
}

function debounce(fn, ms) {
    var timer;
    return function () {
        var args = arguments;
        var ctx = this;
        clearTimeout(timer);
        timer = setTimeout(function () { fn.apply(ctx, args); }, ms);
    };
}

// ========================================================================
// API
// ========================================================================
var API_BASE = "/api";

function api(path) {
    return fetch(API_BASE + path).then(function (resp) {
        if (!resp.ok) throw new Error(resp.status + " " + resp.statusText);
        return resp.json();
    });
}

// ========================================================================
// DOM Helpers (safe — no innerHTML)
// ========================================================================
function el(tag, attrs) {
    var e = document.createElement(tag);
    if (attrs) {
        for (var k in attrs) {
            if (!attrs.hasOwnProperty(k)) continue;
            var v = attrs[k];
            if (k === "className") e.className = v;
            else if (k === "onclick") e.addEventListener("click", v);
            else if (k === "oninput") e.addEventListener("input", v);
            else if (k === "onkeydown") e.addEventListener("keydown", v);
            else e.setAttribute(k, v);
        }
    }
    for (var i = 2; i < arguments.length; i++) {
        var c = arguments[i];
        if (c == null) continue;
        if (typeof c === "string") e.appendChild(document.createTextNode(c));
        else e.appendChild(c);
    }
    return e;
}

function svgSearchIcon() {
    var svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("class", "search-icon");
    svg.setAttribute("width", "20");
    svg.setAttribute("height", "20");
    svg.setAttribute("viewBox", "0 0 24 24");
    svg.setAttribute("fill", "none");
    svg.setAttribute("stroke", "currentColor");
    svg.setAttribute("stroke-width", "2");
    var circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("cx", "11");
    circle.setAttribute("cy", "11");
    circle.setAttribute("r", "8");
    var line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", "21");
    line.setAttribute("y1", "21");
    line.setAttribute("x2", "16.65");
    line.setAttribute("y2", "16.65");
    svg.appendChild(circle);
    svg.appendChild(line);
    return svg;
}

function navigate(hash) {
    location.hash = hash;
}

// ========================================================================
// Components
// ========================================================================

function createCardSkeleton() {
    return el("div", { className: "skeleton skeleton-card" });
}

function createCardSkeletonLg() {
    return el("div", { className: "skeleton skeleton-card-lg" });
}

function createPersonCard(person, size) {
    var pid = person.person_id || person.id;
    var nameJa = person.name_ja || person.name || pid;
    var nameEn = person.name_en || "";
    var role = person.primary_role || "";
    var iv = person.iv_score;

    if (size === "small") {
        var card = el("div", {
            className: "person-card-sm",
            onclick: function () { navigate("#profile/" + enc(pid)); },
        });
        card.appendChild(el("div", { className: "card-name" }, nameJa));
        if (role) card.appendChild(el("div", { className: "card-role" }, role));
        if (iv != null) {
            card.appendChild(
                el("span", { className: "card-score-pill pill-iv" }, "IV " + fmtScore(iv))
            );
        }
        return card;
    }

    // Large card for search results
    var cardLg = el("div", {
        className: "person-card-lg",
        onclick: function () { navigate("#profile/" + enc(pid)); },
    });
    cardLg.appendChild(el("div", { className: "card-name-lg" }, nameJa));
    if (nameEn && nameJa !== nameEn) {
        cardLg.appendChild(el("div", { className: "card-name-sub" }, nameEn));
    }
    if (role) cardLg.appendChild(el("div", { className: "card-role" }, role));
    var scores = el("div", { className: "card-scores" });
    if (iv != null) scores.appendChild(el("span", { className: "card-score-pill pill-iv" }, "IV " + fmtScore(iv)));
    if (person.birank != null) scores.appendChild(el("span", { className: "card-score-pill pill-birank" }, "BR " + fmtScore(person.birank, 4)));
    if (person.patronage != null) scores.appendChild(el("span", { className: "card-score-pill pill-patronage" }, "PT " + fmtScore(person.patronage, 4)));
    if (person.person_fe != null) scores.appendChild(el("span", { className: "card-score-pill pill-person-fe" }, "FE " + fmtScore(person.person_fe, 4)));
    cardLg.appendChild(scores);
    return cardLg;
}

function scrollRow(track, direction) {
    var cardWidth = 212; // 200px card + 12px gap
    track.scrollBy({ left: direction * cardWidth * 4, behavior: "smooth" });
}

function setupTrackScrollState(wrap, track) {
    function update() {
        wrap.classList.toggle("has-scroll-left", track.scrollLeft > 10);
    }
    track.addEventListener("scroll", update);
    update();
}

// ========================================================================
// Page: Home
// ========================================================================
function renderHome() {
    var container = document.getElementById("page-home");
    clearChildren(container);

    // Hero
    var hero = el("section", { className: "hero" });
    hero.appendChild(el("h1", { className: "hero-title" }, t("hero_title")));
    hero.appendChild(el("p", { className: "hero-subtitle" }, t("hero_subtitle")));
    var searchWrap = el("div", { className: "hero-search-wrap" });
    searchWrap.appendChild(svgSearchIcon());
    var searchInput = el("input", {
        className: "hero-search",
        type: "text",
        placeholder: t("search_placeholder"),
        onkeydown: function (e) {
            if (e.key === "Enter") {
                var q = searchInput.value.trim();
                if (q) navigate("#search/" + enc(q));
            }
        },
    });
    searchWrap.appendChild(searchInput);
    hero.appendChild(searchWrap);
    container.appendChild(hero);

    // Category rows
    var rows = [
        { title: t("row_top_rated"), sort: "iv_score" },
        { title: t("row_collaborative"), sort: "patronage" },
        { title: t("row_rising"), sort: "person_fe" },
    ];

    rows.forEach(function (rowDef) {
        var section = el("section", { className: "category-section" });
        var header = el("div", { className: "row-header" });
        header.appendChild(el("h2", { className: "row-title" }, rowDef.title));

        var track = el("div", { className: "row-track" });
        var trackWrap = el("div", { className: "row-track-wrap" });

        var arrows = el("div", { className: "row-arrows" });
        arrows.appendChild(el("button", {
            className: "row-arrow",
            onclick: function () { scrollRow(track, -1); },
        }, "\u2039"));
        arrows.appendChild(el("button", {
            className: "row-arrow",
            onclick: function () { scrollRow(track, 1); },
        }, "\u203A"));
        header.appendChild(arrows);
        section.appendChild(header);

        // Skeletons
        for (var i = 0; i < 8; i++) {
            track.appendChild(createCardSkeleton());
        }
        trackWrap.appendChild(track);
        section.appendChild(trackWrap);
        container.appendChild(section);

        setupTrackScrollState(trackWrap, track);

        // Load data
        api("/ranking?sort=" + rowDef.sort + "&limit=20").then(function (data) {
            clearChildren(track);
            var items = data.items || data.results || data;
            if (!Array.isArray(items)) items = [];
            items.forEach(function (p) {
                track.appendChild(createPersonCard(p, "small"));
            });
        }).catch(function () {
            clearChildren(track);
        });
    });

    // Reports row
    var isJa = currentLang === "ja";
    var reportSection = el("section", { className: "category-section" });
    var reportHeader = el("div", { className: "row-header" });
    var reportTitleLink = el("a", {
        className: "row-title-link",
        href: "#reports",
    }, t("row_reports"));
    reportHeader.appendChild(reportTitleLink);

    var reportTrack = el("div", { className: "row-track" });
    var reportTrackWrap = el("div", { className: "row-track-wrap" });

    var reportArrows = el("div", { className: "row-arrows" });
    reportArrows.appendChild(el("button", {
        className: "row-arrow",
        onclick: function () { scrollRow(reportTrack, -1); },
    }, "\u2039"));
    reportArrows.appendChild(el("button", {
        className: "row-arrow",
        onclick: function () { scrollRow(reportTrack, 1); },
    }, "\u203A"));
    reportHeader.appendChild(reportArrows);
    reportSection.appendChild(reportHeader);

    REPORTS.forEach(function (r) {
        var card = el("a", {
            className: "report-card-sm",
            href: "/reports/" + r.file,
        });
        card.setAttribute("target", "_blank");
        card.setAttribute("rel", "noopener");
        card.appendChild(el("div", { className: "report-num" }, "REPORT " + r.num));
        card.appendChild(el("h4", null, isJa ? r.title : r.titleEn));
        card.appendChild(el("div", { className: "report-subtitle" }, isJa ? r.subtitle : r.subtitleEn));
        card.appendChild(el("div", { className: "report-desc" }, isJa ? r.desc : r.descEn));
        reportTrack.appendChild(card);
    });

    reportTrackWrap.appendChild(reportTrack);
    reportSection.appendChild(reportTrackWrap);
    container.appendChild(reportSection);
    setupTrackScrollState(reportTrackWrap, reportTrack);
}

// ========================================================================
// Page: Search Results
// ========================================================================
function renderSearch(query) {
    var container = document.getElementById("page-search");
    clearChildren(container);

    // Search bar at top
    var headerDiv = el("div", { className: "search-page-header" });
    var inputWrap = el("div", { className: "search-input-wrap" });
    inputWrap.appendChild(svgSearchIcon());
    var searchInput = el("input", {
        className: "search-page-input",
        type: "text",
        placeholder: t("search_placeholder"),
        value: query || "",
        onkeydown: function (e) {
            if (e.key === "Enter") {
                doFetch();
            }
        },
    });
    inputWrap.appendChild(searchInput);
    headerDiv.appendChild(inputWrap);
    container.appendChild(headerDiv);

    var countDiv = el("div", { className: "search-count" });
    container.appendChild(countDiv);

    var resultsGrid = el("div", { className: "search-results-grid" });
    container.appendChild(resultsGrid);

    function showSkeletons() {
        clearChildren(resultsGrid);
        for (var i = 0; i < 8; i++) {
            resultsGrid.appendChild(el("div", { className: "skeleton skeleton-card-lg" }));
        }
    }

    function doFetch() {
        var q = searchInput.value.trim();
        if (!q) {
            clearChildren(resultsGrid);
            countDiv.textContent = "";
            return;
        }

        // Update hash without re-rendering
        history.replaceState(null, "", "#search/" + enc(q));

        showSkeletons();
        countDiv.textContent = "";

        api("/persons/search?q=" + enc(q) + "&limit=40").then(function (data) {
            clearChildren(resultsGrid);
            var items = data.results || data.persons || [];
            if (items.length === 0) {
                countDiv.textContent = "";
                resultsGrid.appendChild(
                    el("div", { className: "empty-state" },
                        el("div", { className: "empty-state-title" }, t("search_no_results")),
                        el("div", { className: "empty-state-text" }, q)
                    )
                );
                return;
            }
            countDiv.textContent = t("search_results_for") + " \"" + q + "\" \u2014 " + items.length;
            items.forEach(function (p) {
                resultsGrid.appendChild(createPersonCard(p, "large"));
            });
        }).catch(function (err) {
            clearChildren(resultsGrid);
            countDiv.textContent = "";
            resultsGrid.appendChild(
                el("div", { className: "empty-state" },
                    el("div", { className: "empty-state-title" }, "Error"),
                    el("div", { className: "empty-state-text" }, err.message)
                )
            );
        });
    }

    var debouncedFetch = debounce(doFetch, 300);
    searchInput.addEventListener("input", debouncedFetch);

    // Initial search if query provided
    if (query) {
        doFetch();
    }

    // Focus the input
    setTimeout(function () { searchInput.focus(); }, 50);
}

// ========================================================================
// Page: Profile
// ========================================================================
function renderProfile(personId) {
    var container = document.getElementById("page-profile");
    clearChildren(container);

    // Loading skeletons
    var loadingDiv = el("div", { className: "profile-hero" });
    loadingDiv.appendChild(el("div", { className: "skeleton skeleton-text w40", style: "height:20px;margin-bottom:16px" }));
    loadingDiv.appendChild(el("div", { className: "skeleton skeleton-text w60", style: "height:36px;margin-bottom:8px" }));
    loadingDiv.appendChild(el("div", { className: "skeleton skeleton-text w40", style: "height:16px;margin-bottom:32px" }));
    var scoreSkeletons = el("div", { className: "score-cards" });
    for (var i = 0; i < 4; i++) scoreSkeletons.appendChild(el("div", { className: "skeleton skeleton-score" }));
    loadingDiv.appendChild(scoreSkeletons);
    container.appendChild(loadingDiv);

    // Fetch all data
    var pid = enc(personId);
    Promise.allSettled([
        api("/persons/" + pid + "/profile"),
        api("/persons/" + pid),
        api("/persons/" + pid + "/graph").catch(function () {
            return api("/persons/" + pid + "/network");
        }).catch(function () {
            return { nodes: [], edges: [] };
        }),
        api("/persons/" + pid + "/similar?top_n=8"),
    ]).then(function (results) {
        var profile = results[0].status === "fulfilled" ? results[0].value : null;
        var personFull = results[1].status === "fulfilled" ? results[1].value : null;
        var graphData = results[2].status === "fulfilled" ? results[2].value : { nodes: [], edges: [] };
        var similar = results[3].status === "fulfilled" ? results[3].value : null;

        clearChildren(container);

        // Determine data sources
        var np = (profile && profile.network_profile) || {};
        var ip = profile && profile.individual_profile;
        var pf = personFull || {};

        // Merge: prefer network_profile fields, fall back to personFull
        var name_ja = np.name_ja || pf.name_ja || pf.name || personId;
        var name_en = np.name_en || pf.name_en || "";
        var role = np.primary_role || pf.primary_role || "";
        var iv = np.iv_score != null ? np.iv_score : pf.iv_score;
        var birank = np.birank != null ? np.birank : pf.birank;
        var patronage = np.patronage != null ? np.patronage : pf.patronage;
        var person_fe = np.person_fe != null ? np.person_fe : pf.person_fe;

        if (!profile && !personFull) {
            container.appendChild(
                el("div", { className: "empty-state", style: "padding-top:120px" },
                    el("div", { className: "empty-state-title" }, t("not_found")),
                    el("div", { className: "empty-state-text" }, personId)
                )
            );
            return;
        }

        // 1. Profile Header
        var heroSection = el("div", { className: "profile-hero" });
        heroSection.appendChild(el("a", {
            className: "profile-back",
            onclick: function () { history.back(); },
        }, "\u2190 " + t("back")));

        heroSection.appendChild(el("h1", { className: "profile-name" }, name_ja));
        if (name_en && name_en !== name_ja) {
            heroSection.appendChild(el("div", { className: "profile-name-en" }, name_en));
        }

        var badges = el("div", { className: "profile-badges" });
        if (role) badges.appendChild(el("span", { className: "badge-pill role" }, role));
        badges.appendChild(el("span", { className: "badge-pill id" }, personId));
        heroSection.appendChild(badges);

        // 2. Score Cards
        var scoreCards = el("div", { className: "score-cards" });
        var scoreItems = [
            { value: iv, label: t("score_iv"), highlight: true },
            { value: birank, label: t("score_birank") },
            { value: patronage, label: t("score_patronage") },
            { value: person_fe, label: t("score_person_fe") },
        ];
        scoreItems.forEach(function (item) {
            var card = el("div", { className: "score-card" + (item.highlight ? " highlight" : "") });
            card.appendChild(el("div", { className: "score-value" }, fmtScore(item.value)));
            card.appendChild(el("div", { className: "score-label" }, item.label));
            scoreCards.appendChild(card);
        });
        heroSection.appendChild(scoreCards);
        container.appendChild(heroSection);

        // 3. Score Bars
        var barSection = el("div", { className: "profile-section" });
        barSection.appendChild(el("h2", { className: "section-title" }, t("profile_network")));
        var bars = el("div", { className: "score-bars" });
        // Per-metric ranges: each metric has its own typical scale
        var barItems = [
            { key: "iv-score", value: iv, label: t("score_iv"), range: 0.001 },
            { key: "birank", value: birank, label: t("score_birank"), range: 0.001 },
            { key: "patronage", value: patronage, label: t("score_patronage"), range: 1.0 },
            { key: "person-fe", value: person_fe, label: t("score_person_fe"), range: 10.0 },
        ];
        barItems.forEach(function (item) {
            var row = el("div", { className: "bar-row" });
            row.appendChild(el("div", { className: "bar-label" }, item.label));
            var track = el("div", { className: "bar-track" });
            var fill = el("div", { className: "bar-fill bar-" + item.key });
            track.appendChild(fill);
            row.appendChild(track);
            row.appendChild(el("div", { className: "bar-value" }, fmtScore(item.value)));
            bars.appendChild(row);
            // Animate bar fill — normalize to each metric's own range
            var pct = Math.min(Math.abs(item.value || 0) / item.range * 100, 100);
            setTimeout(function () {
                fill.style.width = pct + "%";
            }, 50);
        });
        barSection.appendChild(bars);
        container.appendChild(barSection);

        // 4. Individual Contribution
        if (ip) {
            var contribSection = el("div", { className: "profile-section" });
            contribSection.appendChild(el("h2", { className: "section-title" }, t("profile_contribution")));
            var grid = el("div", { className: "contrib-grid" });
            var metrics = [
                { key: "peer_percentile", label: t("metric_percentile"), desc: t("desc_percentile"), fmt: fmtScore },
                { key: "opportunity_residual", label: t("metric_residual"), desc: t("desc_residual"), fmt: function (v) { return (v > 0 ? "+" : "") + fmtScore(v); } },
                { key: "consistency", label: t("metric_consistency"), desc: t("desc_consistency"), fmt: fmtScore },
                { key: "independent_value", label: t("metric_independent"), desc: t("desc_independent"), fmt: fmtScore },
            ];
            metrics.forEach(function (m) {
                var card = el("div", { className: "contrib-card" });
                card.appendChild(el("div", { className: "contrib-name" }, m.label));
                card.appendChild(el("div", { className: "contrib-value" }, ip[m.key] != null ? m.fmt(ip[m.key]) : "\u2014"));
                card.appendChild(el("div", { className: "contrib-desc" }, m.desc));
                grid.appendChild(card);
            });
            contribSection.appendChild(grid);
            container.appendChild(contribSection);
        }

        // 5. Radar Chart + Career (two-column)
        var twoColSection = el("div", { className: "profile-section" });
        var twoCol = el("div", { className: "profile-grid" });

        // Radar
        var radarCol = el("div");
        radarCol.appendChild(el("h2", { className: "section-title" }, t("profile_radar")));
        var radarContainer = el("div", { className: "chart-container", id: "radarChart" });
        radarCol.appendChild(radarContainer);
        twoCol.appendChild(radarCol);

        // Score breakdown or career
        var infoCol = el("div");
        infoCol.appendChild(el("h2", { className: "section-title" }, t("profile_explanation")));
        var infoBox = el("div", { className: "chart-container" });

        // Show explanation from profile or career from personFull
        if (profile && profile.explanation) {
            renderExplanation(infoBox, profile.explanation);
        } else if (profile && profile.interpretation) {
            renderInterpretation(infoBox, profile.interpretation);
        } else if (pf.career) {
            renderCareerInfo(infoBox, pf);
        } else {
            infoBox.appendChild(el("div", { style: "padding:24px;color:var(--text-muted)" }, "\u2014"));
        }
        infoCol.appendChild(infoBox);
        twoCol.appendChild(infoCol);
        twoColSection.appendChild(twoCol);
        container.appendChild(twoColSection);

        // Render Plotly radar
        renderRadarChart(radarContainer, np, pf);

        // 6. Ego Graph
        if (graphData && graphData.nodes && graphData.nodes.length > 1) {
            var graphSection = el("div", { className: "profile-section" });
            graphSection.appendChild(el("h2", { className: "section-title" }, t("profile_ego")));
            var graphContainer = el("div", { className: "chart-container", id: "egoGraph" });
            graphSection.appendChild(graphContainer);
            container.appendChild(graphSection);
            renderEgoGraph(graphContainer, personId, graphData);
        }

        // 7. Similar Persons (horizontal scroll row)
        var simItems = similar && (similar.similar || []);
        if (simItems && simItems.length > 0) {
            var simSection = el("div", { className: "profile-section" });
            var simHeader = el("div", { className: "row-header" });
            simHeader.appendChild(el("h2", { className: "section-title", style: "margin-bottom:0" }, t("profile_similar")));

            var simTrack = el("div", { className: "row-track" });
            var simTrackWrap = el("div", { className: "row-track-wrap" });

            var simArrows = el("div", { className: "row-arrows" });
            simArrows.appendChild(el("button", {
                className: "row-arrow",
                onclick: function () { scrollRow(simTrack, -1); },
            }, "\u2039"));
            simArrows.appendChild(el("button", {
                className: "row-arrow",
                onclick: function () { scrollRow(simTrack, 1); },
            }, "\u203A"));
            simHeader.appendChild(simArrows);
            simSection.appendChild(simHeader);

            simItems.forEach(function (s) {
                var pdata = s.person || s;
                pdata.person_id = pdata.person_id || s.person_id;
                var card = createPersonCard(pdata, "small");
                var simBadge = el("div", { className: "card-similarity" },
                    t("similarity") + ": " + fmtScore(s.similarity || s.similarity_score, 4)
                );
                card.appendChild(simBadge);
                simTrack.appendChild(card);
            });
            simTrackWrap.appendChild(simTrack);
            simSection.appendChild(simTrackWrap);
            container.appendChild(simSection);
            setupTrackScrollState(simTrackWrap, simTrack);
        }

        // 8. Disclaimer
        var disclaimerSection = el("div", { className: "profile-section", style: "padding-bottom:48px" });
        var disclaimer = el("div", { className: "disclaimer" });
        var dt = el("strong", null, t("disclaimer_title") + ": ");
        disclaimer.appendChild(dt);
        disclaimer.appendChild(document.createTextNode(t("disclaimer_text")));
        disclaimerSection.appendChild(disclaimer);
        container.appendChild(disclaimerSection);
    });
}

function renderRadarChart(container, np, pf) {
    // Build radar data from available sources
    var iv = np.iv_score || pf.iv_score || 0;
    var birank = np.birank || pf.birank || 0;
    var patronage = np.patronage || pf.patronage || 0;
    var person_fe = np.person_fe || pf.person_fe || 0;
    var hub = (pf.network && pf.network.hub_score) || 0;
    var vers = (pf.versatility && pf.versatility.score) || 0;
    var conf = (pf.confidence || 0) * 100;

    // Auto-scale: normalize all values to 0-100 range
    var values = [iv, birank, patronage, person_fe, hub, vers, conf];
    var labels = [
        t("score_iv"), t("score_birank"), t("score_patronage"),
        t("score_person_fe"), t("hub_score"), t("versatility"), t("confidence"),
    ];

    // Use max of all values, minimum 100 for display
    var maxVal = Math.max.apply(null, values.concat([1]));
    var scale = maxVal > 100 ? 100 / maxVal : (maxVal < 1 ? 100 : 1);
    var scaledValues = values.map(function (v) { return v * scale; });

    // Close the polygon
    var plotValues = scaledValues.concat([scaledValues[0]]);
    var plotLabels = labels.concat([labels[0]]);

    try {
        Plotly.newPlot(container, [{
            type: "scatterpolar",
            r: plotValues,
            theta: plotLabels,
            fill: "toself",
            fillcolor: "rgba(240,147,251,0.12)",
            line: { color: "#f093fb", width: 2 },
            marker: { size: 5, color: "#f093fb" },
        }], {
            polar: {
                radialaxis: {
                    visible: true,
                    range: [0, 100],
                    color: "#444",
                    gridcolor: "rgba(255,255,255,0.06)",
                    tickfont: { size: 10, color: "#666" },
                },
                angularaxis: { color: "#888", tickfont: { size: 11 } },
                bgcolor: "rgba(0,0,0,0)",
            },
            paper_bgcolor: "rgba(0,0,0,0)",
            plot_bgcolor: "rgba(0,0,0,0)",
            font: { color: "#999" },
            margin: { l: 60, r: 60, t: 30, b: 30 },
            showlegend: false,
            height: 320,
        }, { responsive: true, displayModeBar: false });
    } catch (e) {
        container.appendChild(el("div", { style: "padding:24px;color:var(--text-muted)" }, "Chart unavailable"));
    }
}

function renderEgoGraph(container, centerId, graphData) {
    var nodes = graphData.nodes || [];
    var edges = graphData.edges || [];
    var others = nodes.filter(function (n) { return (n.person_id || n.id) !== centerId; });

    // Layout: center at origin, others in circle
    var positions = {};
    positions[centerId] = [0, 0];
    others.forEach(function (n, i) {
        var angle = (2 * Math.PI * i) / others.length;
        positions[n.person_id || n.id] = [Math.cos(angle), Math.sin(angle)];
    });

    var edgeX = [], edgeY = [];
    edges.forEach(function (e) {
        var s = positions[e.source] || [0, 0];
        var tgt = positions[e.target] || [0, 0];
        edgeX.push(s[0], tgt[0], null);
        edgeY.push(s[1], tgt[1], null);
    });

    var centerNode = nodes.find(function (n) { return (n.person_id || n.id) === centerId; }) || nodes[0];
    var clusterColors = ["#f093fb", "#06D6A0", "#667eea", "#fda085", "#FFD166", "#a0d2db", "#EF476F", "#96E6A1", "#DDA0DD", "#87CEEB"];

    var traces = [];
    // Edges
    traces.push({
        x: edgeX, y: edgeY, mode: "lines",
        line: { width: 1, color: "rgba(150,150,200,0.3)" },
        hoverinfo: "none", showlegend: false,
    });
    // Center node
    traces.push({
        x: [0], y: [0], mode: "markers+text",
        marker: { size: 20, color: "#f093fb", line: { width: 2, color: "white" } },
        text: [centerNode.name || centerNode.name_ja || centerId],
        textposition: "top center",
        textfont: { color: "#f093fb", size: 11 },
        hovertemplate: (centerNode.name || centerId) + "<br>IV: " + fmtScore(centerNode.iv_score) + "<extra></extra>",
        showlegend: false,
    });
    // Other nodes
    traces.push({
        x: others.map(function (n) { return positions[n.person_id || n.id][0]; }),
        y: others.map(function (n) { return positions[n.person_id || n.id][1]; }),
        mode: "markers+text",
        marker: {
            size: others.map(function (n) { return Math.max(8, Math.min(18, (n.iv_score || 10) / 5)); }),
            color: others.map(function (n) { return clusterColors[(n.cluster || 0) % clusterColors.length]; }),
            line: { width: 1, color: "rgba(255,255,255,0.4)" },
        },
        text: others.map(function (n) { return n.name || n.name_ja || (n.person_id || n.id); }),
        textposition: "top center",
        textfont: { color: "#888", size: 9 },
        customdata: others.map(function (n) { return n.person_id || n.id; }),
        hovertemplate: "%{text}<extra></extra>",
        showlegend: false,
    });

    try {
        Plotly.newPlot(container, traces, {
            xaxis: { showgrid: false, zeroline: false, showticklabels: false },
            yaxis: { showgrid: false, zeroline: false, showticklabels: false, scaleanchor: "x" },
            paper_bgcolor: "rgba(0,0,0,0)",
            plot_bgcolor: "rgba(0,0,0,0)",
            font: { color: "#888" },
            margin: { l: 20, r: 20, t: 20, b: 20 },
            height: 420,
            showlegend: false,
        }, { responsive: true, displayModeBar: false });

        // Click navigation
        container.on("plotly_click", function (data) {
            var cd = data.points[0].customdata;
            if (cd) navigate("#profile/" + enc(cd));
        });
    } catch (e) {
        container.appendChild(el("div", { style: "padding:24px;color:var(--text-muted)" }, "Graph unavailable"));
    }
}

function renderExplanation(container, expl) {
    var div = el("div", { style: "padding:16px;font-size:14px;line-height:1.8;color:var(--text-secondary)" });
    if (Array.isArray(expl)) {
        expl.forEach(function (e) {
            var p = el("p", { style: "margin-bottom:8px" }, e.text || e.description || JSON.stringify(e));
            div.appendChild(p);
        });
    } else if (typeof expl === "object") {
        Object.keys(expl).forEach(function (key) {
            var p = el("p", { style: "margin-bottom:8px" });
            p.appendChild(el("strong", { style: "color:var(--text-primary)" }, key + ": "));
            p.appendChild(document.createTextNode(typeof expl[key] === "string" ? expl[key] : JSON.stringify(expl[key])));
            div.appendChild(p);
        });
    }
    container.appendChild(div);
}

function renderInterpretation(container, interp) {
    var div = el("div", { style: "padding:16px;font-size:14px;line-height:1.8;color:var(--text-secondary)" });
    if (interp.narrative) {
        div.appendChild(el("p", { style: "margin-bottom:12px" }, interp.narrative));
    }
    if (interp.key_metrics && interp.key_metrics.length) {
        interp.key_metrics.forEach(function (m) {
            div.appendChild(el("p", { style: "margin-bottom:4px;color:var(--text-primary)" }, "\u2022 " + m));
        });
    }
    container.appendChild(div);
}

function renderCareerInfo(container, pf) {
    var career = pf.career || {};
    var growth = pf.growth || {};
    var versatility = pf.versatility || {};
    var network = pf.network || {};

    var table = el("table", { style: "width:100%;border-collapse:collapse" });
    var tbody = el("tbody");
    var rows = [
        [t("credits"), String(pf.total_credits || 0)],
        ["\u6D3B\u52D5\u671F\u9593", (career.first_year || "?") + " \u2013 " + (career.latest_year || "?")],
        ["\u30D4\u30FC\u30AF", (career.peak_year || "?") + " (" + (career.peak_credits || 0) + " credits)"],
        [t("versatility"), fmtScore(versatility.score, 1) + " (" + (versatility.roles || 0) + " roles)"],
        [t("hub_score"), fmtScore(network.hub_score, 1)],
        ["Trend", growth.trend || "\u2014"],
    ];
    rows.forEach(function (r) {
        var tr = el("tr");
        tr.appendChild(el("td", { style: "padding:8px 12px;color:var(--score-pink);font-weight:600;font-size:13px;white-space:nowrap" }, r[0]));
        tr.appendChild(el("td", { style: "padding:8px 12px;color:var(--text-secondary);font-size:14px" }, r[1]));
        tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    container.appendChild(table);
}

// ========================================================================
// Page: Ranking
// ========================================================================
function renderRanking() {
    var container = document.getElementById("page-ranking");
    clearChildren(container);

    var wrapper = el("div", { className: "ranking-container" });
    wrapper.appendChild(el("h1", { className: "page-title" }, t("nav_ranking")));

    // State
    var currentSort = "iv_score";
    var currentRole = "";

    // Filter bar
    var filterBar = el("div", { className: "filter-bar" });

    // Sort pills
    var sortGroup = el("div", { className: "filter-group" });
    var sortOptions = [
        { value: "iv_score", label: t("score_iv") },
        { value: "birank", label: t("score_birank") },
        { value: "patronage", label: t("score_patronage") },
        { value: "person_fe", label: t("score_person_fe") },
    ];
    var sortPills = [];
    sortOptions.forEach(function (opt) {
        var pill = el("button", {
            className: "filter-pill" + (opt.value === currentSort ? " active" : ""),
            onclick: function () {
                currentSort = opt.value;
                sortPills.forEach(function (p) { p.classList.remove("active"); });
                pill.classList.add("active");
                doFetch();
            },
        }, opt.label);
        sortPills.push(pill);
        sortGroup.appendChild(pill);
    });
    filterBar.appendChild(sortGroup);

    // Role pills
    var roleGroup = el("div", { className: "filter-group" });
    var roleOptions = [
        { value: "", label: t("ranking_all_roles") },
        { value: "director", label: "Director" },
        { value: "animator", label: "Animator" },
        { value: "designer", label: "Designer" },
    ];
    var rolePills = [];
    roleOptions.forEach(function (opt) {
        var pill = el("button", {
            className: "filter-pill" + (opt.value === currentRole ? " active" : ""),
            onclick: function () {
                currentRole = opt.value;
                rolePills.forEach(function (p) { p.classList.remove("active"); });
                pill.classList.add("active");
                doFetch();
            },
        }, opt.label);
        rolePills.push(pill);
        roleGroup.appendChild(pill);
    });
    filterBar.appendChild(roleGroup);
    wrapper.appendChild(filterBar);

    var tableContainer = el("div");
    wrapper.appendChild(tableContainer);
    container.appendChild(wrapper);

    function showTableSkeletons() {
        clearChildren(tableContainer);
        var skeletonGroup = el("div", { className: "skeleton-row-group" });
        for (var i = 0; i < 10; i++) {
            skeletonGroup.appendChild(el("div", { className: "skeleton skeleton-bar", style: "height:44px" }));
        }
        tableContainer.appendChild(skeletonGroup);
    }

    function doFetch() {
        showTableSkeletons();
        var url = "/ranking?sort=" + currentSort + "&limit=100";
        if (currentRole) url += "&role=" + enc(currentRole);

        api(url).then(function (data) {
            clearChildren(tableContainer);
            var items = data.items || data.results || data;
            if (!Array.isArray(items) || items.length === 0) {
                tableContainer.appendChild(
                    el("div", { className: "empty-state" },
                        el("div", { className: "empty-state-title" }, t("search_no_results"))
                    )
                );
                return;
            }

            var table = el("table", { className: "rank-table" });
            var thead = el("thead");
            var headerRow = el("tr");
            var cols = [t("rank_col"), t("name_col"), t("role_col"), t("score_iv"), t("score_birank"), t("score_patronage"), t("score_person_fe")];
            cols.forEach(function (col) {
                headerRow.appendChild(el("th", null, col));
            });
            thead.appendChild(headerRow);
            table.appendChild(thead);

            var tbody = el("tbody");
            items.forEach(function (entry, i) {
                var tr = el("tr", {
                    onclick: function () { navigate("#profile/" + enc(entry.person_id)); },
                });
                tr.appendChild(el("td", { className: "rank-num" }, String(i + 1)));
                var nameCell = el("td", { className: "person-name" }, entry.name_ja || entry.name_en || entry.person_id);
                tr.appendChild(nameCell);
                tr.appendChild(el("td", null, entry.primary_role || "\u2014"));
                tr.appendChild(el("td", { className: "score-col" }, fmtScore(entry.iv_score)));
                tr.appendChild(el("td", { className: "score-col" }, fmtScore(entry.birank, 4)));
                tr.appendChild(el("td", { className: "score-col" }, fmtScore(entry.patronage, 4)));
                tr.appendChild(el("td", { className: "score-col" }, fmtScore(entry.person_fe, 4)));
                tbody.appendChild(tr);
            });
            table.appendChild(tbody);
            tableContainer.appendChild(table);
        }).catch(function (err) {
            clearChildren(tableContainer);
            tableContainer.appendChild(
                el("div", { className: "empty-state" },
                    el("div", { className: "empty-state-title" }, "Error"),
                    el("div", { className: "empty-state-text" }, err.message)
                )
            );
        });
    }

    doFetch();
}

// ========================================================================
// Page: Reports
// ========================================================================
var REPORTS = [
    { num: "01", file: "industry_analysis.html", title: "\u696D\u754C\u4FEF\u77B0\u30C0\u30C3\u30B7\u30E5\u30DC\u30FC\u30C9", titleEn: "Industry Overview", subtitle: "\u30DE\u30AF\u30ED\u30C8\u30EC\u30F3\u30C9", subtitleEn: "Macro trends across 100+ years", desc: "\u6642\u7CFB\u5217\u63A8\u79FB\u3001\u5B63\u7BC0\u30D1\u30BF\u30FC\u30F3\u3001\u5E74\u4EE3\u6BD4\u8F03\u3001\u6210\u9577\u5206\u6790\u3002", descEn: "Time series, seasonal patterns, decade comparison, growth analysis.", sources: "summary, time_series, decades" },
    { num: "02", file: "bridge_analysis.html", title: "\u30CD\u30C3\u30C8\u30EF\u30FC\u30AF\u30D6\u30EA\u30C3\u30B8\u5206\u6790", titleEn: "Network Bridge Analysis", subtitle: "\u8D8A\u5883\u4EBA\u6750\u3068\u63A5\u7D9A\u6027", subtitleEn: "Cross-community bridge persons", desc: "\u5206\u96E2\u3057\u305F\u30B3\u30DF\u30E5\u30CB\u30C6\u30A3\u3092\u63A5\u7D9A\u3059\u308B\u4EBA\u7269\u3002\u30D6\u30EA\u30C3\u30B8\u30B9\u30B3\u30A2\u3001\u30AF\u30ED\u30B9\u30B3\u30DF\u30E5\u30CB\u30C6\u30A3\u30A8\u30C3\u30B8\u3002", descEn: "Persons connecting isolated communities. Bridge scores, cross-community edges.", sources: "bridges" },
    { num: "03", file: "team_analysis.html", title: "\u30C1\u30FC\u30E0\u69CB\u6210\u5206\u6790", titleEn: "Team Composition", subtitle: "\u30B9\u30BF\u30C3\u30D5\u69CB\u6210\u30D1\u30BF\u30FC\u30F3", subtitleEn: "Staff composition patterns", desc: "\u30C1\u30FC\u30E0\u69CB\u9020\u3001\u5F79\u8077\u7D44\u307F\u5408\u308F\u305B\u3001\u63A8\u85A6\u30B3\u30E9\u30DC\u30DA\u30A2\u3002", descEn: "Team structure, role combinations, recommended collab pairs.", sources: "teams" },
    { num: "04", file: "career_transitions.html", title: "\u30AD\u30E3\u30EA\u30A2\u9077\u79FB\u5206\u6790", titleEn: "Career Transitions", subtitle: "\u30AD\u30E3\u30EA\u30A2\u30B9\u30C6\u30FC\u30B8\u9032\u884C", subtitleEn: "Career stage progression", desc: "\u9077\u79FB\u884C\u5217\u3001\u30B5\u30F3\u30AD\u30FC\u30C0\u30A4\u30A2\u30B0\u30E9\u30E0\u3001\u4E00\u822C\u7684\u306A\u30AD\u30E3\u30EA\u30A2\u30D1\u30B9\u3002", descEn: "Transition matrices, Sankey diagrams, common career paths.", sources: "transitions, role_flow" },
    { num: "05", file: "temporal_foresight.html", title: "\u6642\u7CFB\u5217BiRank\u30FB\u5148\u898B\u30B9\u30B3\u30A2", titleEn: "Temporal Foresight", subtitle: "\u4EBA\u6750\u65E9\u671F\u767A\u898B", subtitleEn: "Early talent discovery", desc: "BiRank\u306E\u6642\u7CFB\u5217\u63A8\u79FB\u3001\u5148\u898B\u30B9\u30B3\u30A2\u306B\u3088\u308B\u65E9\u671F\u4EBA\u6750\u767A\u898B\u3002", descEn: "BiRank time series, early talent discovery via foresight scores.", sources: "temporal_pagerank" },
    { num: "06", file: "network_evolution.html", title: "\u30CD\u30C3\u30C8\u30EF\u30FC\u30AF\u69CB\u9020\u5909\u5316", titleEn: "Network Evolution", subtitle: "\u5354\u696D\u30CD\u30C3\u30C8\u30EF\u30FC\u30AF\u4F4D\u76F8\u5909\u5316", subtitleEn: "Collaboration topology over time", desc: "\u30CE\u30FC\u30C9/\u30A8\u30C3\u30B8\u6570\u3001\u5BC6\u5EA6\u3001\u30AF\u30E9\u30B9\u30BF\u30EA\u30F3\u30B0\u306E\u63A8\u79FB\u3002", descEn: "Node/edge counts, density, clustering trends over time.", sources: "network_evolution" },
    { num: "07", file: "growth_scores.html", title: "\u6210\u9577\u30C8\u30EC\u30F3\u30C9\u30FB\u30B9\u30B3\u30A2\u5206\u6790", titleEn: "Growth Trends", subtitle: "\u30E9\u30A4\u30B8\u30F3\u30B0\u30B9\u30BF\u30FC\u3001\u904E\u5C0F\u8A55\u4FA1", subtitleEn: "Rising stars, undervalued talent", desc: "\u6210\u9577\u30C8\u30EC\u30F3\u30C9\u5206\u5E03\u3001\u30E9\u30A4\u30B8\u30F3\u30B0\u30B9\u30BF\u30FC\u3001\u904E\u5C0F\u8A55\u4FA1\u30A2\u30E9\u30FC\u30C8\u3002", descEn: "Growth trend distribution, rising stars, undervaluation alerts.", sources: "growth, insights_report" },
    { num: "08", file: "person_ranking.html", title: "\u4EBA\u7269\u30E9\u30F3\u30AD\u30F3\u30B0\u30FB\u30B9\u30B3\u30A2\u5206\u6790", titleEn: "Person Ranking", subtitle: "IV Score\u30E9\u30F3\u30AD\u30F3\u30B0", subtitleEn: "IV Score rankings & distributions", desc: "IV Score\u9806\u306E\u4E0A\u4F4D\u4EBA\u7269\u3002\u30B9\u30B3\u30A2\u5206\u5E03\u3001\u30EC\u30FC\u30C0\u30FC\u30C1\u30E3\u30FC\u30C8\u3002", descEn: "Top persons by IV Score. Score distributions, radar charts.", sources: "scores, individual_profiles" },
    { num: "09", file: "compensation_fairness.html", title: "\u5831\u916C\u516C\u5E73\u6027\u5206\u6790", titleEn: "Compensation Fairness", subtitle: "Shapley\u914D\u5206\u3068Gini\u5206\u6790", subtitleEn: "Shapley allocation & Gini analysis", desc: "Shapley\u30D9\u30FC\u30B9\u306E\u516C\u6B63\u914D\u5206\u3001\u4F5C\u54C1\u5225Gini\u4FC2\u6570\u3002", descEn: "Shapley-based fair allocation, per-work Gini coefficients.", sources: "fair_compensation, anime_values" },
    { num: "10", file: "bias_detection.html", title: "\u30D0\u30A4\u30A2\u30B9\u691C\u51FA\u30EC\u30DD\u30FC\u30C8", titleEn: "Bias Detection", subtitle: "\u7CFB\u7D71\u7684\u30D0\u30A4\u30A2\u30B9\u306E\u691C\u51FA\u3068\u88DC\u6B63", subtitleEn: "Systematic bias detection & correction", desc: "\u5F79\u8077\u30FB\u30B9\u30BF\u30B8\u30AA\u30FB\u30AD\u30E3\u30EA\u30A2\u30B9\u30C6\u30FC\u30B8\u5225\u306E\u30D0\u30A4\u30A2\u30B9\u3002", descEn: "Role, studio, career stage biases. Undervaluation alerts.", sources: "bias_report, credit_stats" },
    { num: "11", file: "genre_analysis.html", title: "\u30B8\u30E3\u30F3\u30EB\u30FB\u30B9\u30B3\u30A2\u89AA\u548C\u6027", titleEn: "Genre Analysis", subtitle: "\u54C1\u8CEA\u5E2F\u30FB\u6642\u4EE3\u5225\u306E\u89AA\u548C\u6027", subtitleEn: "Quality-tier & era-based affinity", desc: "\u30B9\u30DA\u30B7\u30E3\u30EA\u30B9\u30C8vs\u30B8\u30A7\u30CD\u30E9\u30EA\u30B9\u30C8\u306E\u30AF\u30E9\u30B9\u30BF\u30EA\u30F3\u30B0\u3002", descEn: "Specialist vs generalist clustering by quality and era.", sources: "genre_affinity, anime_stats" },
    { num: "12", file: "studio_impact.html", title: "\u30B9\u30BF\u30B8\u30AA\u5F71\u97FF\u5206\u6790", titleEn: "Studio Impact", subtitle: "\u30B9\u30BF\u30B8\u30AA\u6240\u5C5E\u306E\u56E0\u679C\u52B9\u679C", subtitleEn: "Causal effect of studio affiliation", desc: "\u9078\u629C/\u51E6\u7F6E/\u30D6\u30E9\u30F3\u30C9\u52B9\u679C\u3001\u69CB\u9020\u63A8\u5B9A\u3001\u30B9\u30BF\u30B8\u30AA\u6BD4\u8F03\u3002", descEn: "Selection/treatment/brand effects, structural estimation.", sources: "causal_identification, studios" },
    { num: "13", file: "credit_statistics.html", title: "\u30AF\u30EC\u30B8\u30C3\u30C8\u7D71\u8A08", titleEn: "Credit Statistics", subtitle: "\u5F79\u8077\u5206\u5E03\u3001\u751F\u7523\u6027", subtitleEn: "Role distribution, productivity", desc: "\u30AF\u30EC\u30B8\u30C3\u30C8\u6570\u3001\u5F79\u8077\u5206\u5E03\u3001\u30B5\u30F3\u30AD\u30FC\u30C0\u30A4\u30A2\u30B0\u30E9\u30E0\u3001\u751F\u7523\u6027\u6307\u6A19\u3002", descEn: "Credit counts, role distribution, Sankey diagrams, productivity metrics.", sources: "credit_stats, role_flow" },
    { num: "14", file: "cooccurrence_groups.html", title: "\u5171\u540C\u5236\u4F5C\u96C6\u56E3\u5206\u6790", titleEn: "Co-occurrence Groups", subtitle: "\u30B3\u30A2\u30B9\u30BF\u30C3\u30D5\u306E\u7E70\u308A\u8FD4\u3057\u30D1\u30BF\u30FC\u30F3", subtitleEn: "Recurring core staff patterns", desc: "3\u4EBA\u4EE5\u4E0A\u306E\u30B3\u30A2\u30B9\u30BF\u30C3\u30D5\u304C\u8907\u6570\u4F5C\u54C1\u3067\u7E70\u308A\u8FD4\u3057\u5171\u53C2\u52A0\u3059\u308B\u30B0\u30EB\u30FC\u30D7\u3002", descEn: "Groups where 3+ core staff repeatedly co-participate across works.", sources: "cooccurrence_groups" },
    { num: "15", file: "ml_clustering.html", title: "ML\u30AF\u30E9\u30B9\u30BF\u30EA\u30F3\u30B0\u5206\u6790", titleEn: "ML Clustering", subtitle: "PCA \u00D7 K-Means", subtitleEn: "PCA dimensionality reduction × K-Means", desc: "20\u6B21\u5143\u7279\u5FB4\u91CF\u306B\u57FA\u3065\u304F\u6559\u5E2B\u306A\u3057\u30AF\u30E9\u30B9\u30BF\u30EA\u30F3\u30B0\u3002", descEn: "Unsupervised clustering on 20-dim feature vectors. PCA scatter, silhouette.", sources: "ml_clusters" },
    { num: "16", file: "network_graph.html", title: "\u30CD\u30C3\u30C8\u30EF\u30FC\u30AF\u30B0\u30E9\u30D5", titleEn: "Network Graph", subtitle: "\u30A4\u30F3\u30BF\u30E9\u30AF\u30C6\u30A3\u30D6\u53EF\u8996\u5316", subtitleEn: "Interactive visualization", desc: "\u4E0A\u4F4D\u4EBA\u7269\u306E\u5354\u696D\u30FB\u5E2B\u5F1F\u30CD\u30C3\u30C8\u30EF\u30FC\u30AF\u3002", descEn: "Top persons' collaboration & mentorship network.", sources: "scores, collaborations" },
    { num: "17", file: "cohort_animation.html", title: "\u30B3\u30DB\u30FC\u30C8\u30FB\u30A8\u30FC\u30B8\u30A7\u30F3\u30C8\u8ECE\u8DE1", titleEn: "Cohort Animation", subtitle: "\u4E16\u4EE3\u5225\u30AD\u30E3\u30EA\u30A2\u6210\u9577", subtitleEn: "Generational career growth animation", desc: "Gapminder\u578B\u30A2\u30CB\u30E1\u30FC\u30B7\u30E7\u30F3\u300213\u30C1\u30E3\u30FC\u30C8\u53CE\u9332\u3002", descEn: "Gapminder-style animation. 13 charts.", sources: "scores, growth, milestones" },
    { num: "18", file: "longitudinal_analysis.html", title: "\u7E26\u65AD\u7684\u30AD\u30E3\u30EA\u30A2\u5206\u6790", titleEn: "Longitudinal Analysis", subtitle: "OMA\u30FB\u56FA\u5B9A\u8CBB/\u5909\u52D5\u8CBB\u30FBFE\u30FBPSM", subtitleEn: "OMA, FE regression, PSM, causal inference", desc: "\u30B9\u30D1\u30B2\u30C3\u30C6\u30A3\u30D7\u30ED\u30C3\u30C8\u304B\u3089\u56E0\u679C\u63A8\u8AD6\u307E\u3067\u300241\u30C1\u30E3\u30FC\u30C8\u300211\u30BB\u30AF\u30B7\u30E7\u30F3\u3002", descEn: "Spaghetti plots to causal inference. 41 charts, 11 sections.", sources: "scores, milestones, transitions" },
    { num: "19", file: "shap_explanation.html", title: "SHAP \u30B9\u30B3\u30A2\u8AAC\u660E\u30EC\u30DD\u30FC\u30C8", titleEn: "SHAP Explanation", subtitle: "Shapley\u5024\u306B\u3088\u308Biv_score\u6C7A\u5B9A\u56E0\u5B50", subtitleEn: "Shapley values for iv_score determinants", desc: "GradientBoosting + TreeExplainer\u3067\u5404\u7279\u5FB4\u91CF\u306E\u9650\u754C\u8CA2\u732E\u3092\u5B9A\u91CF\u5316\u3002", descEn: "GradientBoosting + TreeExplainer for marginal contributions.", sources: "scores" },
    { num: "20", file: "credit_intervals.html", title: "\u30AF\u30EC\u30B8\u30C3\u30C8\u767B\u5834\u9593\u9694\u5206\u6790", titleEn: "Credit Intervals", subtitle: "\u8077\u80FD\u5225\u30FB\u30AD\u30E3\u30EA\u30A2\u5E74\u6570\u5225", subtitleEn: "By role & career year", desc: "\u8077\u80FD\u5225\u30FB\u30AD\u30E3\u30EA\u30A2\u5E74\u6570\u5225\u306E\u6D3B\u52D5\u9593\u9694\u30D1\u30BF\u30FC\u30F3\u3001\u4F11\u6B62\u5206\u6790\u3001\u6D3B\u52D5\u5BC6\u5EA6\u300211\u30C1\u30E3\u30FC\u30C8\u3002", descEn: "Activity interval patterns by role category & career year. Hiatus analysis, activity density. 11 charts.", sources: "credit_intervals" },
];

function renderReports() {
    var container = document.getElementById("page-reports");
    clearChildren(container);

    var wrapper = el("div", { className: "reports-container" });
    wrapper.appendChild(el("h1", { className: "page-title" }, t("reports_title")));
    wrapper.appendChild(el("p", { style: "color:var(--text-secondary);margin-bottom:32px;margin-top:-16px" }, t("reports_subtitle")));

    var grid = el("div", { className: "reports-grid" });
    var isJa = currentLang === "ja";

    REPORTS.forEach(function (r) {
        var card = el("a", {
            className: "report-card",
            href: "/reports/" + r.file,
        });
        card.setAttribute("target", "_blank");
        card.setAttribute("rel", "noopener");
        card.appendChild(el("div", { className: "report-num" }, "REPORT " + r.num));
        card.appendChild(el("h3", null, isJa ? r.title : r.titleEn));
        card.appendChild(el("div", { className: "report-subtitle" }, isJa ? r.subtitle : r.subtitleEn));
        card.appendChild(el("div", { className: "report-desc" }, isJa ? r.desc : r.descEn));
        var meta = el("div", { className: "report-meta" });
        meta.appendChild(el("span", { className: "report-sources" }, r.sources));
        meta.appendChild(el("span", { className: "report-badge" }, "Ready"));
        card.appendChild(meta);
        grid.appendChild(card);
    });

    wrapper.appendChild(grid);
    container.appendChild(wrapper);
}

// ========================================================================
// Router
// ========================================================================
function handleRoute() {
    var hash = location.hash || "#home";

    // Activate correct page
    document.querySelectorAll(".page").forEach(function (p) { p.classList.remove("active"); });
    document.querySelectorAll(".nav-link").forEach(function (a) { a.classList.remove("active"); });

    if (hash.startsWith("#profile/")) {
        document.getElementById("page-profile").classList.add("active");
        renderProfile(decodeURIComponent(hash.substring(9)));
    } else if (hash.startsWith("#search/")) {
        document.getElementById("page-search").classList.add("active");
        document.querySelector('[data-page="home"]').classList.add("active");
        renderSearch(decodeURIComponent(hash.substring(8)));
    } else if (hash === "#search") {
        document.getElementById("page-search").classList.add("active");
        document.querySelector('[data-page="home"]').classList.add("active");
        renderSearch("");
    } else if (hash === "#ranking") {
        document.getElementById("page-ranking").classList.add("active");
        document.querySelector('[data-page="ranking"]').classList.add("active");
        renderRanking();
    } else if (hash === "#reports") {
        document.getElementById("page-reports").classList.add("active");
        document.querySelector('[data-page="reports"]').classList.add("active");
        renderReports();
    } else {
        // Home
        document.getElementById("page-home").classList.add("active");
        document.querySelector('[data-page="home"]').classList.add("active");
        renderHome();
    }

    // Scroll to top on page change
    window.scrollTo(0, 0);
}

// ========================================================================
// Nav Scroll Handler
// ========================================================================
window.addEventListener("scroll", function () {
    var nav = document.getElementById("mainNav");
    nav.classList.toggle("scrolled", window.scrollY > 20);
}, { passive: true });

// ========================================================================
// Language Toggle
// ========================================================================
document.querySelectorAll(".lang-pill").forEach(function (btn) {
    btn.addEventListener("click", function () {
        currentLang = btn.getAttribute("data-lang");
        localStorage.setItem("animetor_lang", currentLang);
        // Update active states
        document.querySelectorAll(".lang-pill").forEach(function (b) {
            b.classList.toggle("active", b.getAttribute("data-lang") === currentLang);
        });
        // Update static i18n elements
        document.querySelectorAll("[data-i18n]").forEach(function (el) {
            el.textContent = t(el.getAttribute("data-i18n"));
        });
        // Re-render current page
        handleRoute();
    });
});

// ========================================================================
// Init
// ========================================================================
// Apply initial language to nav links
document.querySelectorAll(".lang-pill").forEach(function (btn) {
    btn.classList.toggle("active", btn.getAttribute("data-lang") === currentLang);
});
document.querySelectorAll("[data-i18n]").forEach(function (el) {
    el.textContent = t(el.getAttribute("data-i18n"));
});

window.addEventListener("hashchange", handleRoute);
handleRoute();
