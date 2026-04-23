# Test Coverage Report

**Generated**: 2026-02-10
**Total Coverage**: 91.51% (6547 statements, 556 missed)
**Threshold**: 85% (✅ PASSING)

## Summary

Animetor Evalプロジェクトは高いテストカバレッジを達成しています：

- **955 tests** 全てパス（97秒）
- **91.51% coverage** (scrapers除外後)
- **26+ modules with 100% coverage**

## Coverage by Category

### 🟢 Excellent Coverage (95-100%)

**Analysis Modules** (37+ modules):
- ✅ **100% coverage**: anime_stats, circles, collaboration_strength, comparison_matrix, confidence, crossval, decade_analysis, ego_graph, explain, graphml_export, mentorship, milestones, network_density, network_evolution, productivity, role_flow, similarity, skill, stability, studio, time_series, versatility
- ✅ **95-99%**: aggregate_stats (98%), bridges (99%), career (96%), clusters (98%), cohort (98%), collab_diversity (97%), data_quality (97%), growth (96%), influence (99%), network_evolution (98%), outliers (98%), person_tags (98%), protocols (96%), recommendation (98%), seasonal (97%), team_composition (98%), transitions (99%), trust (98%), visualize (98%), work_impact (97%)

**Pipeline & Core** (100%):
- ✅ models.py (100%)
- ✅ utils/config.py (100%)
- ✅ utils/performance.py (100%)
- ✅ utils/role_groups.py (100%)
- ✅ utils/json_io.py (99%)
- ✅ pipeline_phases/core_scoring.py (100%)
- ✅ pipeline_phases/data_loading.py (100%)
- ✅ pipeline_phases/graph_construction.py (100%)

**Database & Infrastructure**:
- ✅ database.py (93%)
- ✅ validation.py (96%)

### 🟡 Good Coverage (85-94%)

- 🟡 **api.py** (89%): FastAPI endpoints - 37/337 lines missed
  - Missing: Error handling paths, some response transformations

- 🟡 **pipeline_phases/analysis_modules.py** (91%): Parallel execution
  - Missing: Error handling in wrapper functions

- 🟡 **pipeline_phases/export_and_viz.py** (93%): Export registry
  - Missing: Some transformer edge cases

- 🟡 **ai_entity_resolution.py** (90%): AI-assisted entity resolution
  - Missing: LLM unavailable paths, confidence edge cases

- 🟡 **entity_resolution.py** (89%): Name matching
  - Missing: Some similarity threshold edge cases

- 🟡 **report.py** (88%): Report generation
  - Missing: HTML generation paths

### 🟠 Moderate Coverage (75-84%)

- 🟠 **cli.py** (77%): CLI commands (225/979 lines missed)
  - Many command paths not tested (interactive prompts, error cases)
  - Recommendation: Add more CLI integration tests

- 🟠 **pipeline.py** (80%): Main pipeline orchestration
  - Missing: Error handling, dry-run edge cases

- 🟠 **visualize_interactive.py** (83%): Plotly visualizations
  - Missing: Some plot configuration paths

- 🟠 **graph.py** (79%): Network analysis
  - Missing: Advanced centrality calculations (betweenness approximation paths)

### 🔴 Low Coverage (<75%)

- 🔴 **pagerank.py** (49%): PageRank calculations
  - 34/67 lines missed - mostly alternate PageRank implementations
  - Many untested paths in weighted PageRank variants
  - **Recommendation**: Add tests for edge cases (disconnected graphs, small graphs)

- 🔴 **synthetic.py** (71%): Synthetic data generation
  - Test data generator - lower priority for coverage

- 🔴 **pipeline_phases/entity_resolution.py** (56%): Entity resolution phase
  - Wrapper for entity_resolution.py - some paths untested

### ⚫ Excluded from Coverage

- **scrapers/** (0-38%): External API dependencies
  - anilist_scraper.py, mal_scraper.py, mediaarts_scraper.py, jvmg_fetcher.py
  - Reason: Require live API access, not practical to test

- **pipeline_old.py** (0%): Deprecated legacy code

## Recommendations

### Priority 1: Critical Gaps

1. **pagerank.py** (49% → target 85%)
   - Add tests for weighted PageRank edge cases
   - Test disconnected graph handling
   - Test small graph edge cases (<5 nodes)

2. **cli.py** (77% → target 85%)
   - Add integration tests for uncovered commands
   - Mock user input for interactive prompts
   - Test error handling paths

### Priority 2: Nice to Have

3. **graph.py** (79% → target 90%)
   - Test approximate betweenness with different k values
   - Test large graph optimizations

4. **visualize_interactive.py** (83% → target 90%)
   - Test all plot types with edge cases
   - Test empty data handling

5. **pipeline.py** (80% → target 90%)
   - Test dry-run mode thoroughly
   - Test error recovery paths

### Priority 3: Low Priority

6. **synthetic.py** (71%): Test data generator - acceptable as-is
7. **pipeline_phases/entity_resolution.py** (56%): Thin wrapper - improve if time permits

## Running Coverage Tests

### Quick Test
```bash
pixi run test-cov
```

### Generate HTML Report
```bash
pixi run test-cov-report
# Opens htmlcov/index.html in browser
```

### Coverage Configuration

See `.coveragerc` for configuration:
- **Minimum threshold**: 85%
- **Excluded**: scrapers, deprecated code, generated files
- **Report format**: HTML + terminal

## Coverage Trends

| Date | Coverage | Tests | Notes |
|------|----------|-------|-------|
| 2026-02-10 | 91.51% | 955 | Initial measurement (Phase 1-4 refactoring complete) |

## Next Steps

1. ✅ Add `.coveragerc` configuration
2. ✅ Integrate coverage into CI/CD
3. ⏳ Address Priority 1 gaps (pagerank.py, cli.py)
4. ⏳ Add coverage badge to README
5. ⏳ Set up coverage tracking over time

---

**Note**: Coverage is a useful metric but not the only measure of code quality. Focus on testing critical paths and edge cases rather than achieving 100% coverage for its own sake.
