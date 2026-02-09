"""CLI インターフェース (Rich + Typer)."""

import typer
from rich.console import Console
from rich.table import Table

from src.log import setup_logging

app = typer.Typer(name="animetor-eval", help="アニメ業界人材ネットワーク評価ツール")
console = Console()


@app.command()
def stats() -> None:
    """DB の統計情報を表示する."""
    setup_logging()

    from src.database import get_connection, get_data_sources, init_db

    conn = get_connection()
    init_db(conn)

    n_persons = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
    n_anime = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
    n_credits = conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
    n_scores = conn.execute("SELECT COUNT(*) FROM scores").fetchone()[0]

    # 役職分布
    role_dist = conn.execute(
        "SELECT role, COUNT(*) as cnt FROM credits GROUP BY role ORDER BY cnt DESC"
    ).fetchall()

    # ソース分布
    source_dist = conn.execute(
        "SELECT source, COUNT(*) as cnt FROM credits GROUP BY source ORDER BY cnt DESC"
    ).fetchall()

    # 年代分布
    year_dist = conn.execute(
        "SELECT year, COUNT(*) as cnt FROM anime WHERE year IS NOT NULL GROUP BY year ORDER BY year DESC LIMIT 10"
    ).fetchall()

    data_sources = get_data_sources(conn)
    conn.close()

    console.print("\n[bold blue]Animetor Eval — DB Statistics[/bold blue]\n")

    summary = Table(title="Summary")
    summary.add_column("Entity", style="cyan")
    summary.add_column("Count", justify="right", style="green")
    summary.add_row("Persons", f"{n_persons:,}")
    summary.add_row("Anime", f"{n_anime:,}")
    summary.add_row("Credits", f"{n_credits:,}")
    summary.add_row("Scores", f"{n_scores:,}")
    console.print(summary)

    if role_dist:
        roles_table = Table(title="Credits by Role")
        roles_table.add_column("Role", style="cyan")
        roles_table.add_column("Count", justify="right", style="green")
        for row in role_dist:
            roles_table.add_row(row["role"], f"{row['cnt']:,}")
        console.print(roles_table)

    if source_dist:
        src_table = Table(title="Credits by Source")
        src_table.add_column("Source", style="cyan")
        src_table.add_column("Count", justify="right", style="green")
        for row in source_dist:
            src_table.add_row(row["source"] or "(empty)", f"{row['cnt']:,}")
        console.print(src_table)

    if year_dist:
        year_table = Table(title="Anime by Year (Top 10)")
        year_table.add_column("Year", style="cyan")
        year_table.add_column("Count", justify="right", style="green")
        for row in year_dist:
            year_table.add_row(str(row["year"]), f"{row['cnt']:,}")
        console.print(year_table)

    if data_sources:
        ds_table = Table(title="Data Sources")
        ds_table.add_column("Source", style="cyan")
        ds_table.add_column("Last Scraped", style="dim")
        ds_table.add_column("Items", justify="right", style="green")
        ds_table.add_column("Status", style="yellow")
        for ds in data_sources:
            ds_table.add_row(
                ds["source"],
                ds["last_scraped_at"] or "-",
                str(ds["item_count"]),
                ds["status"],
            )
        console.print(ds_table)


@app.command()
def ranking(
    top_n: int = typer.Option(30, "--top", "-n", help="表示件数"),
    role: str = typer.Option(None, "--role", "-r", help="役職カテゴリでフィルタ (director/animator/designer/technical)"),
    sort_by: str = typer.Option("composite", "--sort", "-s", help="ソート軸 (composite/authority/trust/skill)"),
    year_from: int = typer.Option(None, "--year-from", help="キャリア開始年の下限"),
    year_to: int = typer.Option(None, "--year-to", help="最新活動年の上限"),
) -> None:
    """スコアランキングを表示する."""
    setup_logging()

    from src.database import get_connection, init_db

    conn = get_connection()
    init_db(conn)

    valid_sort = {"composite", "authority", "trust", "skill"}
    if sort_by not in valid_sort:
        console.print(f"[red]Invalid sort axis: {sort_by}. Use: {', '.join(valid_sort)}[/red]")
        conn.close()
        raise typer.Exit(1)

    if role or year_from or year_to:
        # Load enriched data from scores.json for filtering
        import json as json_mod

        from src.utils.config import JSON_DIR

        scores_path = JSON_DIR / "scores.json"
        if not scores_path.exists():
            console.print("[yellow]No scores.json found. Run 'pixi run pipeline' first.[/yellow]")
            conn.close()
            raise typer.Exit()

        all_data = json_mod.loads(scores_path.read_text())
        filtered = all_data
        if role:
            filtered = [r for r in filtered if r.get("primary_role") == role]
        if year_from:
            filtered = [
                r for r in filtered
                if r.get("career", {}).get("first_year") and r["career"]["first_year"] >= year_from
            ]
        if year_to:
            filtered = [
                r for r in filtered
                if r.get("career", {}).get("latest_year") and r["career"]["latest_year"] <= year_to
            ]
        filtered.sort(key=lambda x: x.get(sort_by, 0), reverse=True)
        rows = filtered[:top_n]
        conn.close()

        if not rows:
            msg = "No persons found"
            if role:
                msg += f" with role={role}"
            if year_from or year_to:
                msg += f" in year range {year_from or '...'}-{year_to or '...'}"
            console.print(f"[yellow]{msg}[/yellow]")
            raise typer.Exit()
    else:
        order_col = f"s.{sort_by}"
        rows = conn.execute(
            f"""SELECT s.person_id, p.name_ja, p.name_en,
                      s.authority, s.trust, s.skill, s.composite
               FROM scores s
               JOIN persons p ON s.person_id = p.id
               ORDER BY {order_col} DESC
               LIMIT ?""",  # noqa: S608
            (top_n,),
        ).fetchall()
        conn.close()
        rows = [dict(r) for r in rows]

    if not rows:
        console.print("[yellow]No scores found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    title = "Animetor Eval — ネットワーク評価ランキング"
    if role:
        title += f" [{role}]"
    if year_from or year_to:
        title += f" ({year_from or '...'}-{year_to or '...'})"
    console.print(f"\n[bold blue]{title}[/bold blue]")
    console.print(
        "[dim]※ スコアはネットワーク位置・密度の指標であり、能力評価ではありません[/dim]\n"
    )

    table = Table()
    table.add_column("#", justify="right", style="dim")
    table.add_column("Name", style="cyan", min_width=25)
    table.add_column("Authority", justify="right", style="blue")
    table.add_column("Trust", justify="right", style="green")
    table.add_column("Skill", justify="right", style="yellow")
    table.add_column("Composite", justify="right", style="bold magenta")

    for i, row in enumerate(rows, 1):
        name = row.get("name_ja") or row.get("name_en") or row.get("name") or row.get("person_id", "?")
        table.add_row(
            str(i),
            name,
            f"{row['authority']:.1f}",
            f"{row['trust']:.1f}",
            f"{row['skill']:.1f}",
            f"{row['composite']:.1f}",
        )

    console.print(table)


@app.command()
def profile(person_id: str = typer.Argument(help="人物ID (例: anilist:p100)")) -> None:
    """特定人物のプロフィールを表示する."""
    setup_logging()

    from src.database import get_connection, init_db

    conn = get_connection()
    init_db(conn)

    person = conn.execute(
        "SELECT * FROM persons WHERE id = ?", (person_id,)
    ).fetchone()
    if not person:
        console.print(f"[red]Person not found: {person_id}[/red]")
        conn.close()
        raise typer.Exit(1)

    score = conn.execute(
        "SELECT * FROM scores WHERE person_id = ?", (person_id,)
    ).fetchone()

    credits = conn.execute(
        """SELECT c.role, c.source, a.title_ja, a.title_en, a.year, a.score
           FROM credits c
           JOIN anime a ON c.anime_id = a.id
           WHERE c.person_id = ?
           ORDER BY a.year DESC""",
        (person_id,),
    ).fetchall()

    conn.close()

    name = person["name_ja"] or person["name_en"] or person_id
    console.print(f"\n[bold blue]Profile: {name}[/bold blue]")
    console.print(f"  ID: {person_id}")
    if person["name_ja"]:
        console.print(f"  名前 (日): {person['name_ja']}")
    if person["name_en"]:
        console.print(f"  Name (EN): {person['name_en']}")

    if score:
        console.print("\n[bold]Scores:[/bold]")
        console.print(f"  Authority: [blue]{score['authority']:.1f}[/blue]")
        console.print(f"  Trust:     [green]{score['trust']:.1f}[/green]")
        console.print(f"  Skill:     [yellow]{score['skill']:.1f}[/yellow]")
        console.print(f"  Composite: [magenta]{score['composite']:.1f}[/magenta]")

    if credits:
        console.print(f"\n[bold]Credits ({len(credits)} total):[/bold]")
        table = Table()
        table.add_column("Year", style="dim")
        table.add_column("Title", style="cyan")
        table.add_column("Role", style="green")
        table.add_column("Score", justify="right")

        for c in credits[:20]:
            title = c["title_ja"] or c["title_en"] or ""
            year = str(c["year"]) if c["year"] else "?"
            anime_score = f"{c['score']:.1f}" if c["score"] else "-"
            table.add_row(year, title[:40], c["role"], anime_score)

        console.print(table)
        if len(credits) > 20:
            console.print(f"  [dim]... and {len(credits) - 20} more[/dim]")

    # Director circles (from circles.json if available)
    from src.utils.config import JSON_DIR

    circles_path = JSON_DIR / "circles.json"
    if circles_path.exists():
        import json as json_mod

        circles_data = json_mod.loads(circles_path.read_text())
        memberships = []
        for dir_id, circle in circles_data.items():
            for member in circle.get("members", []):
                if member["person_id"] == person_id:
                    memberships.append({
                        "director": circle.get("director_name", dir_id),
                        "shared_works": member["shared_works"],
                        "hit_rate": member["hit_rate"],
                    })

        if memberships:
            console.print("\n[bold]Director Circles:[/bold]")
            ct = Table()
            ct.add_column("Director", style="cyan")
            ct.add_column("Shared Works", justify="right")
            ct.add_column("Hit Rate", justify="right")
            for m in sorted(memberships, key=lambda x: x["shared_works"], reverse=True):
                ct.add_row(m["director"], str(m["shared_works"]), f"{m['hit_rate']:.0%}")
            console.print(ct)

    # Score explanation
    scores_path = JSON_DIR / "scores.json"
    if scores_path.exists():
        import json as json_mod

        scores_data = json_mod.loads(scores_path.read_text())
        person_data = next((r for r in scores_data if r["person_id"] == person_id), None)
        if person_data and person_data.get("career"):
            career = person_data["career"]
            console.print("\n[bold]Career:[/bold]")
            console.print(f"  Active: {career.get('first_year', '?')} - {career.get('latest_year', '?')}")
            console.print(f"  Active years: {career.get('active_years', '?')}")
            console.print(f"  Highest stage: {career.get('highest_stage', '?')}")
            if career.get("highest_roles"):
                console.print(f"  Top roles: {', '.join(career['highest_roles'])}")


@app.command()
def search(
    query: str = typer.Argument(help="検索クエリ（名前の部分一致）"),
    limit: int = typer.Option(20, "--limit", "-l", help="表示件数"),
) -> None:
    """人物を名前で検索する."""
    setup_logging()

    from src.database import get_connection, init_db, search_persons

    conn = get_connection()
    init_db(conn)
    rows = search_persons(conn, query, limit)
    conn.close()

    if not rows:
        console.print(f"[yellow]No results for: {query}[/yellow]")
        raise typer.Exit()

    console.print(f"\n[bold blue]Search results for '{query}' ({len(rows)} found):[/bold blue]\n")

    table = Table()
    table.add_column("ID", style="dim")
    table.add_column("Name (JA)", style="cyan")
    table.add_column("Name (EN)", style="cyan")
    table.add_column("Credits", justify="right")
    table.add_column("Composite", justify="right", style="bold magenta")

    for row in rows:
        composite = f"{row['composite']:.1f}" if row["composite"] is not None else "-"
        table.add_row(
            row["id"],
            row["name_ja"] or "",
            row["name_en"] or "",
            str(row["credit_count"]),
            composite,
        )

    console.print(table)


@app.command()
def compare(
    person_a: str = typer.Argument(help="人物ID A"),
    person_b: str = typer.Argument(help="人物ID B"),
) -> None:
    """2人の人物を比較する."""
    setup_logging()

    from src.database import get_connection, init_db

    conn = get_connection()
    init_db(conn)

    def _load_person_data(pid: str) -> dict | None:
        person = conn.execute("SELECT * FROM persons WHERE id = ?", (pid,)).fetchone()
        if not person:
            return None
        score = conn.execute("SELECT * FROM scores WHERE person_id = ?", (pid,)).fetchone()
        credit_count = conn.execute(
            "SELECT COUNT(*) FROM credits WHERE person_id = ?", (pid,)
        ).fetchone()[0]
        roles = conn.execute(
            """SELECT role, COUNT(*) as cnt FROM credits
               WHERE person_id = ? GROUP BY role ORDER BY cnt DESC""",
            (pid,),
        ).fetchall()
        return {
            "id": pid,
            "name_ja": person["name_ja"],
            "name_en": person["name_en"],
            "authority": score["authority"] if score else 0.0,
            "trust": score["trust"] if score else 0.0,
            "skill": score["skill"] if score else 0.0,
            "composite": score["composite"] if score else 0.0,
            "credit_count": credit_count,
            "roles": [(r["role"], r["cnt"]) for r in roles],
        }

    data_a = _load_person_data(person_a)
    data_b = _load_person_data(person_b)
    conn.close()

    if not data_a:
        console.print(f"[red]Person not found: {person_a}[/red]")
        raise typer.Exit(1)
    if not data_b:
        console.print(f"[red]Person not found: {person_b}[/red]")
        raise typer.Exit(1)

    name_a = data_a["name_ja"] or data_a["name_en"] or person_a
    name_b = data_b["name_ja"] or data_b["name_en"] or person_b

    console.print(f"\n[bold blue]Compare: {name_a} vs {name_b}[/bold blue]")
    console.print(
        "[dim]※ スコアはネットワーク位置・密度の指標であり、能力評価ではありません[/dim]\n"
    )

    table = Table()
    table.add_column("Metric", style="cyan")
    table.add_column(name_a[:25], justify="right", style="green")
    table.add_column(name_b[:25], justify="right", style="yellow")
    table.add_column("Diff", justify="right", style="dim")

    for axis in ("authority", "trust", "skill", "composite"):
        va, vb = data_a[axis], data_b[axis]
        diff = va - vb
        diff_str = f"{diff:+.1f}"
        table.add_row(axis.capitalize(), f"{va:.1f}", f"{vb:.1f}", diff_str)

    table.add_row("Credits", str(data_a["credit_count"]), str(data_b["credit_count"]), "")
    console.print(table)

    # 役職比較
    if data_a["roles"] or data_b["roles"]:
        console.print("\n[bold]Roles:[/bold]")
        roles_a = {r: c for r, c in data_a["roles"]}
        roles_b = {r: c for r, c in data_b["roles"]}
        all_roles = sorted(set(roles_a) | set(roles_b))

        rt = Table()
        rt.add_column("Role", style="cyan")
        rt.add_column(name_a[:25], justify="right", style="green")
        rt.add_column(name_b[:25], justify="right", style="yellow")
        for role in all_roles:
            rt.add_row(role, str(roles_a.get(role, 0)), str(roles_b.get(role, 0)))
        console.print(rt)


@app.command()
def similar(
    person_id: str = typer.Argument(help="対象人物ID"),
    top_n: int = typer.Option(10, "--top", "-n", help="表示件数"),
) -> None:
    """類似プロファイルの人物を検索する."""
    setup_logging()
    import json as json_mod

    from src.analysis.similarity import find_similar_persons
    from src.utils.config import JSON_DIR

    scores_path = JSON_DIR / "scores.json"
    if not scores_path.exists():
        console.print("[yellow]No scores.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    results = json_mod.loads(scores_path.read_text())
    similar_list = find_similar_persons(person_id, results, top_n)

    if not similar_list:
        console.print(f"[yellow]Person not found or no similar persons: {person_id}[/yellow]")
        raise typer.Exit()

    # Get target name
    target = next((r for r in results if r["person_id"] == person_id), {})
    target_name = target.get("name") or person_id

    console.print(f"\n[bold blue]Similar to: {target_name}[/bold blue]")
    console.print(
        "[dim]※ コサイン類似度によるスコアプロファイル比較[/dim]\n"
    )

    table = Table()
    table.add_column("#", justify="right", style="dim")
    table.add_column("Name", style="cyan", min_width=25)
    table.add_column("Similarity", justify="right", style="bold green")
    table.add_column("Authority", justify="right", style="blue")
    table.add_column("Trust", justify="right", style="green")
    table.add_column("Skill", justify="right", style="yellow")
    table.add_column("Composite", justify="right", style="magenta")

    for i, s in enumerate(similar_list, 1):
        table.add_row(
            str(i),
            s["name"],
            f"{s['similarity']:.3f}",
            f"{s['authority']:.1f}",
            f"{s['trust']:.1f}",
            f"{s['skill']:.1f}",
            f"{s['composite']:.1f}",
        )

    console.print(table)


@app.command()
def export(
    fmt: str = typer.Option("json", "--format", "-f", help="出力形式 (json/text/csv/all)"),
    output_dir: str = typer.Option(None, "--output", "-o", help="出力ディレクトリ"),
) -> None:
    """スコアデータをファイルにエクスポートする."""
    setup_logging()
    from pathlib import Path

    from src.database import get_connection, init_db
    from src.report import generate_csv_report, generate_html_report, generate_json_report, generate_text_report

    conn = get_connection()
    init_db(conn)

    # scores + persons を結合して results 形式に変換
    rows = conn.execute(
        """SELECT s.person_id, p.name_ja, p.name_en,
                  s.authority, s.trust, s.skill, s.composite
           FROM scores s
           JOIN persons p ON s.person_id = p.id
           ORDER BY s.composite DESC""",
    ).fetchall()
    conn.close()

    if not rows:
        console.print("[yellow]No scores found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    results = [
        {
            "person_id": r["person_id"],
            "name": r["name_ja"] or r["name_en"] or r["person_id"],
            "name_ja": r["name_ja"],
            "name_en": r["name_en"],
            "authority": r["authority"],
            "trust": r["trust"],
            "skill": r["skill"],
            "composite": r["composite"],
        }
        for r in rows
    ]

    out_dir = Path(output_dir) if output_dir else None
    formats = ["json", "text", "csv", "html"] if fmt == "all" else [fmt]

    for f in formats:
        if f == "json":
            path = generate_json_report(
                results, output_path=out_dir / "report.json" if out_dir else None
            )
            console.print(f"[green]JSON report: {path}[/green]")
        elif f == "text":
            path = generate_text_report(
                results, output_path=out_dir / "report.txt" if out_dir else None
            )
            console.print(f"[green]Text report: {path}[/green]")
        elif f == "csv":
            path = generate_csv_report(
                results, output_path=out_dir / "scores.csv" if out_dir else None
            )
            console.print(f"[green]CSV report: {path}[/green]")
        elif f == "html":
            path = generate_html_report(
                results, output_path=out_dir / "report.html" if out_dir else None
            )
            console.print(f"[green]HTML report: {path}[/green]")
        else:
            console.print(f"[red]Unknown format: {f}[/red]")


@app.command()
def timeline(
    person_id: str = typer.Argument(help="人物ID (例: anilist:p100)"),
    output: str = typer.Option(None, "--output", "-o", help="出力ファイルパス"),
) -> None:
    """人物のキャリアタイムラインを可視化する."""
    setup_logging()
    from collections import defaultdict
    from pathlib import Path

    from src.analysis.career import CAREER_STAGE
    from src.analysis.visualize import plot_person_timeline
    from src.database import get_connection, init_db
    from src.models import Role

    conn = get_connection()
    init_db(conn)

    person = conn.execute("SELECT * FROM persons WHERE id = ?", (person_id,)).fetchone()
    if not person:
        console.print(f"[red]Person not found: {person_id}[/red]")
        conn.close()
        raise typer.Exit(1)

    credits = conn.execute(
        """SELECT c.role, a.title_ja, a.title_en, a.year, a.score
           FROM credits c JOIN anime a ON c.anime_id = a.id
           WHERE c.person_id = ? AND a.year IS NOT NULL
           ORDER BY a.year""",
        (person_id,),
    ).fetchall()
    conn.close()

    if not credits:
        console.print(f"[yellow]No credits with year data for {person_id}[/yellow]")
        raise typer.Exit()

    credits_by_year: dict[int, list[dict]] = defaultdict(list)
    career_stages: dict[int, int] = {}
    max_stage_so_far = 0

    for c in credits:
        year = c["year"]
        credits_by_year[year].append({
            "anime_title": c["title_ja"] or c["title_en"] or "",
            "role": c["role"],
            "score": c["score"],
        })
        try:
            role_enum = Role(c["role"])
        except ValueError:
            role_enum = Role.OTHER
        stage = CAREER_STAGE.get(role_enum, 0)
        if stage > max_stage_so_far:
            max_stage_so_far = stage
        career_stages[year] = max(career_stages.get(year, 0), max_stage_so_far)

    name = person["name_ja"] or person["name_en"] or person_id
    out_path = Path(output) if output else None

    plot_person_timeline(
        person_id=person_id,
        credits_by_year=dict(credits_by_year),
        career_stages=career_stages,
        person_name=name,
        output_path=out_path,
    )
    console.print(f"[green]Timeline saved for {name}[/green]")


@app.command()
def history(
    person_id: str = typer.Argument(help="人物ID"),
    limit: int = typer.Option(20, "--limit", "-l", help="表示件数"),
) -> None:
    """人物のスコア履歴を表示する."""
    setup_logging()

    from src.database import get_connection, get_score_history, init_db

    conn = get_connection()
    init_db(conn)

    person = conn.execute("SELECT * FROM persons WHERE id = ?", (person_id,)).fetchone()
    if not person:
        console.print(f"[red]Person not found: {person_id}[/red]")
        conn.close()
        raise typer.Exit(1)

    hist = get_score_history(conn, person_id, limit=limit)
    conn.close()

    if not hist:
        console.print(f"[yellow]No score history for {person_id}[/yellow]")
        raise typer.Exit()

    name = person["name_ja"] or person["name_en"] or person_id
    console.print(f"\n[bold blue]Score History: {name}[/bold blue]\n")

    table = Table()
    table.add_column("Run Date", style="dim")
    table.add_column("Authority", justify="right", style="blue")
    table.add_column("Trust", justify="right", style="green")
    table.add_column("Skill", justify="right", style="yellow")
    table.add_column("Composite", justify="right", style="bold magenta")

    prev_composite = None
    for h in hist:
        delta = ""
        if prev_composite is not None:
            diff = h["composite"] - prev_composite
            delta = f" ({diff:+.1f})" if abs(diff) > 0.01 else ""
        prev_composite = h["composite"]

        run_at = h["run_at"] or ""
        if len(run_at) > 19:
            run_at = run_at[:19]

        table.add_row(
            run_at,
            f"{h['authority']:.1f}",
            f"{h['trust']:.1f}",
            f"{h['skill']:.1f}",
            f"{h['composite']:.1f}{delta}",
        )

    console.print(table)


@app.command()
def crossval() -> None:
    """クロスバリデーション結果を表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "crossval.json"
    if not path.exists():
        console.print("[yellow]No crossval.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    console.print("\n[bold blue]Score Cross-Validation Results[/bold blue]")
    console.print(
        "[dim]スコアの安定性を測定 — クレジットの一部を除外してランキング変動を検証[/dim]\n"
    )

    summary = Table(title="Summary")
    summary.add_column("Metric", style="cyan")
    summary.add_column("Value", justify="right", style="green")
    summary.add_row("Folds", str(data.get("n_folds", "?")))
    summary.add_row("Holdout Ratio", f"{data.get('holdout_ratio', 0):.0%}")
    summary.add_row("Total Credits", f"{data.get('total_credits', 0):,}")
    summary.add_row("Avg Rank Correlation", f"{data.get('avg_rank_correlation', 0):.4f}")
    summary.add_row("Min Rank Correlation", f"{data.get('min_rank_correlation', 0):.4f}")
    summary.add_row("Avg Top-10 Overlap", f"{data.get('avg_top10_overlap', 0):.0%}")
    console.print(summary)

    folds = data.get("fold_results", [])
    if folds:
        ft = Table(title="Per-Fold Results")
        ft.add_column("Fold", justify="right", style="dim")
        ft.add_column("Credits Used", justify="right")
        ft.add_column("Correlation", justify="right", style="green")
        ft.add_column("Top-10 Overlap", justify="right", style="yellow")
        for f in folds:
            ft.add_row(
                str(f["fold"]),
                f"{f['credits_used']:,}",
                f"{f['correlation']:.4f}",
                f"{f['top10_overlap']:.0%}",
            )
        console.print(ft)

    # Interpretation
    avg_corr = data.get("avg_rank_correlation", 0)
    if avg_corr >= 0.95:
        console.print("\n[bold green]Score stability: Excellent (>0.95)[/bold green]")
    elif avg_corr >= 0.85:
        console.print("\n[bold green]Score stability: Good (>0.85)[/bold green]")
    elif avg_corr >= 0.70:
        console.print("\n[bold yellow]Score stability: Moderate (>0.70)[/bold yellow]")
    else:
        console.print("\n[bold red]Score stability: Low (<0.70) — scores may be unreliable[/bold red]")


@app.command()
def influence() -> None:
    """メンター・メンティー関係（影響ツリー）を表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "influence.json"
    if not path.exists():
        console.print("[yellow]No influence.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    console.print("\n[bold blue]Influence Tree — Mentor-Mentee Relationships[/bold blue]")
    console.print(
        "[dim]ディレクターの門下から成長した人材の追跡[/dim]\n"
    )

    summary = Table(title="Summary")
    summary.add_column("Metric", style="cyan")
    summary.add_column("Value", justify="right", style="green")
    summary.add_row("Total Mentors", str(data.get("total_mentors", 0)))
    summary.add_row("Total Mentees", str(data.get("total_mentees", 0)))
    summary.add_row("Avg Nurture Rate", f"{data.get('avg_nurture_rate', 0):.1f}%")
    console.print(summary)

    # Top mentors by mentee count
    mentors = data.get("mentors", {})
    if mentors:
        sorted_mentors = sorted(
            mentors.items(),
            key=lambda x: x[1]["mentee_count"],
            reverse=True,
        )[:15]

        mt = Table(title="Top Mentors (by mentee count)")
        mt.add_column("Mentor ID", style="cyan")
        mt.add_column("Mentees", justify="right", style="green")
        mt.add_column("Nurture Rate", justify="right", style="yellow")
        mt.add_column("Influence Score", justify="right", style="magenta")

        for mentor_id, info in sorted_mentors:
            mt.add_row(
                mentor_id,
                str(info["mentee_count"]),
                f"{info['nurture_rate']:.1f}%",
                f"{info['influence_score']:.1f}",
            )
        console.print(mt)

    # Generation chains
    chains = data.get("generation_chains", [])
    if chains:
        console.print("\n[bold]Generation Chains:[/bold]")
        for i, chain in enumerate(chains[:5], 1):
            console.print(f"  {i}. {' → '.join(chain)} ({len(chain)} generations)")


@app.command()
def validate() -> None:
    """DBデータの品質チェックを実行する."""
    setup_logging()

    from src.database import get_connection, init_db
    from src.validation import validate_all

    conn = get_connection()
    init_db(conn)
    result = validate_all(conn)
    conn.close()

    if result.passed:
        console.print("[bold green]Validation PASSED[/bold green]")
    else:
        console.print("[bold red]Validation FAILED[/bold red]")

    if result.errors:
        console.print("\n[bold red]Errors:[/bold red]")
        for err in result.errors:
            console.print(f"  [red]• {err}[/red]")

    if result.warnings:
        console.print("\n[bold yellow]Warnings:[/bold yellow]")
        for warn in result.warnings:
            console.print(f"  [yellow]• {warn}[/yellow]")

    if result.stats:
        table = Table(title="Validation Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right")
        for k, v in sorted(result.stats.items()):
            table.add_row(k, str(v))
        console.print(table)


@app.command()
def studios() -> None:
    """スタジオ分析結果を表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "studios.json"
    if not path.exists():
        console.print("[yellow]No studios.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    console.print("\n[bold blue]Studio Analysis[/bold blue]")
    console.print("[dim]各スタジオの人材プール分析[/dim]\n")

    table = Table()
    table.add_column("Studio", style="cyan", min_width=20)
    table.add_column("Anime", justify="right")
    table.add_column("Persons", justify="right")
    table.add_column("Avg Score", justify="right", style="green")

    sorted_studios = sorted(
        data.items(),
        key=lambda x: x[1].get("anime_count", 0),
        reverse=True,
    )
    for studio_name, info in sorted_studios[:30]:
        if not studio_name:
            continue
        table.add_row(
            studio_name[:30],
            str(info.get("anime_count", 0)),
            str(info.get("person_count", 0)),
            f"{info.get('avg_person_score', 0):.1f}",
        )

    console.print(table)


@app.command()
def versatility(
    top_n: int = typer.Option(30, "--top", "-n", help="表示件数"),
) -> None:
    """役職多様性スコアを表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    scores_path = JSON_DIR / "scores.json"
    if not scores_path.exists():
        console.print("[yellow]No scores.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    results = json_mod.loads(scores_path.read_text())

    # Filter to those with versatility data
    with_v = [r for r in results if r.get("versatility")]
    if not with_v:
        console.print("[yellow]No versatility data found.[/yellow]")
        raise typer.Exit()

    with_v.sort(key=lambda x: x["versatility"]["score"], reverse=True)

    console.print("\n[bold blue]Role Versatility Ranking[/bold blue]")
    console.print("[dim]複数カテゴリで活動する人材の多様性指標[/dim]\n")

    table = Table()
    table.add_column("#", justify="right", style="dim")
    table.add_column("Name", style="cyan", min_width=25)
    table.add_column("Score", justify="right", style="bold green")
    table.add_column("Categories", justify="right")
    table.add_column("Roles", justify="right")
    table.add_column("Composite", justify="right", style="magenta")

    for i, r in enumerate(with_v[:top_n], 1):
        name = r.get("name") or r.get("name_ja") or r.get("person_id", "?")
        v = r["versatility"]
        table.add_row(
            str(i),
            name,
            f"{v['score']:.0f}",
            str(v["categories"]),
            str(v["roles"]),
            f"{r['composite']:.1f}",
        )

    console.print(table)


@app.command()
def density(
    top_n: int = typer.Option(30, "--top", "-n", help="表示件数"),
) -> None:
    """ネットワーク密度（コラボレーション指標）を表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    scores_path = JSON_DIR / "scores.json"
    if not scores_path.exists():
        console.print("[yellow]No scores.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    results = json_mod.loads(scores_path.read_text())

    with_n = [r for r in results if r.get("network")]
    if not with_n:
        console.print("[yellow]No network density data found.[/yellow]")
        raise typer.Exit()

    with_n.sort(key=lambda x: x["network"]["hub_score"], reverse=True)

    console.print("\n[bold blue]Network Density (Hub Score) Ranking[/bold blue]")
    console.print("[dim]コラボレーション・ハブとしての重要度[/dim]\n")

    table = Table()
    table.add_column("#", justify="right", style="dim")
    table.add_column("Name", style="cyan", min_width=25)
    table.add_column("Hub Score", justify="right", style="bold green")
    table.add_column("Collaborators", justify="right")
    table.add_column("Unique Anime", justify="right")
    table.add_column("Composite", justify="right", style="magenta")

    for i, r in enumerate(with_n[:top_n], 1):
        name = r.get("name") or r.get("name_ja") or r.get("person_id", "?")
        n = r["network"]
        table.add_row(
            str(i),
            name,
            f"{n['hub_score']:.1f}",
            str(n["collaborators"]),
            str(n["unique_anime"]),
            f"{r['composite']:.1f}",
        )

    console.print(table)


@app.command()
def outliers() -> None:
    """スコア外れ値を検出・表示する."""
    import json as json_mod

    from src.analysis.outliers import detect_outliers
    from src.utils.config import JSON_DIR

    scores_path = JSON_DIR / "scores.json"
    if not scores_path.exists():
        console.print("[yellow]No scores.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    results = json_mod.loads(scores_path.read_text())
    out = detect_outliers(results)

    console.print("\n[bold blue]Score Outlier Detection[/bold blue]")
    console.print(f"[dim]検出された外れ値: {out['total_outliers']} 名[/dim]\n")

    for axis, data in out.get("axis_outliers", {}).items():
        if data["high"] or data["low"]:
            console.print(f"\n[bold]{axis.capitalize()}[/bold] (bounds: {data['bounds']['iqr_lower']:.1f} - {data['bounds']['iqr_upper']:.1f})")

            if data["high"]:
                ht = Table(title=f"{axis} — High Outliers")
                ht.add_column("Name", style="cyan")
                ht.add_column("Value", justify="right", style="red")
                ht.add_column("Z-score", justify="right")
                for entry in data["high"][:10]:
                    ht.add_row(entry["name"], f"{entry['value']:.1f}", f"{entry['zscore']:.2f}")
                console.print(ht)

            if data["low"]:
                lt = Table(title=f"{axis} — Low Outliers")
                lt.add_column("Name", style="cyan")
                lt.add_column("Value", justify="right", style="blue")
                lt.add_column("Z-score", justify="right")
                for entry in data["low"][:10]:
                    lt.add_row(entry["name"], f"{entry['value']:.1f}", f"{entry['zscore']:.2f}")
                console.print(lt)


@app.command()
def growth(
    trend_filter: str = typer.Option(None, "--trend", "-t", help="トレンドフィルタ (rising/stable/declining/inactive)"),
    top_n: int = typer.Option(30, "--top", "-n", help="表示件数"),
) -> None:
    """成長トレンドを表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "growth.json"
    if not path.exists():
        console.print("[yellow]No growth.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    console.print("\n[bold blue]Growth Trends[/bold blue]")
    console.print("[dim]キャリアトレンドに基づく成長傾向分析[/dim]\n")

    # Summary
    summary = data.get("trend_summary", {})
    if summary:
        st = Table(title="Trend Summary")
        st.add_column("Trend", style="cyan")
        st.add_column("Count", justify="right", style="green")
        for trend, count in sorted(summary.items(), key=lambda x: -x[1]):
            st.add_row(trend, str(count))
        console.print(st)

    # Person details
    persons = data.get("persons", {})
    if trend_filter:
        persons = {pid: d for pid, d in persons.items() if d.get("trend") == trend_filter}

    items = sorted(persons.items(), key=lambda x: x[1].get("activity_ratio", 0), reverse=True)[:top_n]

    if items:
        table = Table(title="Person Growth Details")
        table.add_column("Person ID", style="cyan")
        table.add_column("Trend", style="bold")
        table.add_column("Credits", justify="right")
        table.add_column("Recent", justify="right")
        table.add_column("Activity", justify="right", style="green")
        table.add_column("Span", justify="right")

        for pid, d in items:
            trend_style = {
                "rising": "[green]rising[/green]",
                "stable": "[yellow]stable[/yellow]",
                "declining": "[red]declining[/red]",
                "inactive": "[dim]inactive[/dim]",
                "new": "[cyan]new[/cyan]",
            }.get(d["trend"], d["trend"])
            table.add_row(
                pid[:25],
                trend_style,
                str(d["total_credits"]),
                str(d["recent_credits"]),
                f"{d['activity_ratio']:.0%}",
                f"{d['career_span']}y",
            )
        console.print(table)


@app.command()
def teams(
    top_n: int = typer.Option(20, "--top", "-n", help="表示件数"),
) -> None:
    """チーム構成分析（成功作品のスタッフパターン）を表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "teams.json"
    if not path.exists():
        console.print("[yellow]No teams.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    console.print("\n[bold blue]Team Composition Analysis[/bold blue]")
    console.print("[dim]高評価作品のチーム構成パターン[/dim]\n")

    # Team size stats
    stats = data.get("team_size_stats", {})
    if stats:
        st = Table(title="Team Size Statistics")
        st.add_column("Metric", style="cyan")
        st.add_column("Value", justify="right", style="green")
        for k, v in stats.items():
            st.add_row(k.replace("_", " ").title(), str(v))
        console.print(st)

    # High-score teams
    teams_list = data.get("high_score_teams", [])[:top_n]
    if teams_list:
        table = Table(title=f"Top {top_n} High-Score Teams")
        table.add_column("Title", style="cyan", min_width=25)
        table.add_column("Year", justify="right")
        table.add_column("Score", justify="right", style="green")
        table.add_column("Size", justify="right")
        table.add_column("Avg Person Score", justify="right", style="magenta")

        for t in teams_list:
            table.add_row(
                t["title"][:30],
                str(t.get("year", "?")),
                f"{t.get('anime_score', 0):.1f}",
                str(t["team_size"]),
                f"{t.get('avg_person_score', 0):.1f}" if t.get("avg_person_score") else "-",
            )
        console.print(table)

    # Recommended pairs
    pairs = data.get("recommended_pairs", [])[:10]
    if pairs:
        pt = Table(title="Recommended Pairs (frequently in high-score works)")
        pt.add_column("Person A", style="cyan")
        pt.add_column("Person B", style="cyan")
        pt.add_column("Shared Works", justify="right", style="green")
        for p in pairs:
            pt.add_row(p["person_a"], p["person_b"], str(p["shared_high_score_works"]))
        console.print(pt)


@app.command()
def decades() -> None:
    """年代別トレンド分析を表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "decades.json"
    if not path.exists():
        console.print("[yellow]No decades.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    console.print("\n[bold blue]Decade Analysis[/bold blue]")
    console.print("[dim]10年単位でのアニメ業界トレンド[/dim]\n")

    table = Table()
    table.add_column("Decade", style="cyan")
    table.add_column("Credits", justify="right", style="green")
    table.add_column("Persons", justify="right")
    table.add_column("Anime", justify="right")
    table.add_column("Avg Score", justify="right", style="yellow")

    for decade, info in sorted(data.get("decades", {}).items()):
        table.add_row(
            decade,
            f"{info['credit_count']:,}",
            str(info["unique_persons"]),
            str(info["unique_anime"]),
            f"{info.get('avg_anime_score', 0):.1f}" if info.get("avg_anime_score") else "-",
        )

    console.print(table)


@app.command()
def tags(
    tag_filter: str = typer.Option(None, "--tag", "-t", help="タグでフィルタ"),
    top_n: int = typer.Option(30, "--top", "-n", help="表示件数"),
) -> None:
    """人物タグを表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "tags.json"
    if not path.exists():
        console.print("[yellow]No tags.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    console.print("\n[bold blue]Person Tags[/bold blue]")
    console.print("[dim]スコア・キャリアデータに基づく自動タグ[/dim]\n")

    # Tag summary
    summary = data.get("tag_summary", {})
    if summary:
        st = Table(title="Tag Distribution")
        st.add_column("Tag", style="cyan")
        st.add_column("Count", justify="right", style="green")
        for tag, count in sorted(summary.items(), key=lambda x: -x[1]):
            st.add_row(tag, str(count))
        console.print(st)

    # Filtered persons
    person_tags = data.get("person_tags", {})
    if tag_filter:
        filtered = {pid: tags for pid, tags in person_tags.items() if tag_filter in tags}
        console.print(f"\n[bold]Persons with tag '{tag_filter}': {len(filtered)}[/bold]")
        for pid in list(filtered)[:top_n]:
            console.print(f"  {pid}: {', '.join(filtered[pid])}")


@app.command()
def bridges(
    top_n: int = typer.Option(30, "--top", "-n", help="表示件数"),
) -> None:
    """コミュニティ間ブリッジ人物を表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "bridges.json"
    if not path.exists():
        console.print("[yellow]No bridges.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    console.print("\n[bold blue]Community Bridge Persons[/bold blue]")
    console.print("[dim]異なるコラボレーション・コミュニティをつなぐキーパーソン[/dim]\n")

    stats = data.get("stats", {})
    if stats:
        st = Table(title="Bridge Statistics")
        st.add_column("Metric", style="cyan")
        st.add_column("Value", justify="right", style="green")
        st.add_row("Total Persons", str(stats.get("total_persons", 0)))
        st.add_row("Communities", str(stats.get("total_communities", 0)))
        st.add_row("Cross-Community Edges", str(stats.get("total_cross_edges", 0)))
        st.add_row("Bridge Persons", str(stats.get("bridge_person_count", 0)))
        console.print(st)

    bridge_persons = data.get("bridge_persons", [])[:top_n]
    if bridge_persons:
        table = Table(title=f"Top {top_n} Bridge Persons")
        table.add_column("#", justify="right", style="dim")
        table.add_column("Person ID", style="cyan")
        table.add_column("Bridge Score", justify="right", style="bold green")
        table.add_column("Cross Edges", justify="right")
        table.add_column("Communities", justify="right")

        for i, b in enumerate(bridge_persons, 1):
            table.add_row(
                str(i),
                b["person_id"],
                str(b["bridge_score"]),
                str(b["cross_community_edges"]),
                str(b["communities_connected"]),
            )
        console.print(table)


@app.command()
def mentorships(
    top_n: int = typer.Option(30, "--top", "-n", help="表示件数"),
) -> None:
    """推定メンターシップ関係を表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "mentorships.json"
    if not path.exists():
        console.print("[yellow]No mentorships.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    console.print("\n[bold blue]Inferred Mentorships[/bold blue]")
    console.print("[dim]共演パターンから推定された師弟関係[/dim]\n")

    console.print(f"  Total mentorships: {data.get('total', 0)}")

    mentorship_list = data.get("mentorships", [])[:top_n]
    if mentorship_list:
        table = Table(title=f"Top {top_n} Mentorships")
        table.add_column("#", justify="right", style="dim")
        table.add_column("Mentor", style="cyan")
        table.add_column("Mentee", style="green")
        table.add_column("Shared Works", justify="right")
        table.add_column("Stage Gap", justify="right")
        table.add_column("Confidence", justify="right", style="bold yellow")

        for i, m in enumerate(mentorship_list, 1):
            table.add_row(
                str(i),
                m["mentor_id"],
                m["mentee_id"],
                str(m["shared_works"]),
                str(m["stage_gap"]),
                f"{m['confidence']:.0f}",
            )
        console.print(table)

    tree = data.get("tree", {})
    roots = tree.get("roots", [])
    if roots:
        console.print(f"\n[bold]Mentorship roots: {', '.join(roots[:10])}[/bold]")


@app.command()
def milestones(
    person_id: str = typer.Argument(help="人物ID"),
) -> None:
    """人物のキャリアマイルストーンを表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "milestones.json"
    if not path.exists():
        console.print("[yellow]No milestones.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    if person_id not in data:
        console.print(f"[yellow]No milestones for: {person_id}[/yellow]")
        raise typer.Exit()

    events = data[person_id]
    console.print(f"\n[bold blue]Career Milestones: {person_id}[/bold blue]\n")

    table = Table()
    table.add_column("Year", style="dim")
    table.add_column("Type", style="cyan")
    table.add_column("Description", style="green")

    for event in events:
        year = str(event.get("year", "-"))
        table.add_row(year, event["type"], event["description"])

    console.print(table)


@app.command(name="net-evolution")
def net_evolution() -> None:
    """ネットワーク進化（年ごとのネットワーク変化）を表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "network_evolution.json"
    if not path.exists():
        console.print("[yellow]No network_evolution.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    console.print("\n[bold blue]Network Evolution[/bold blue]")
    console.print("[dim]年ごとのコラボレーションネットワーク変化[/dim]\n")

    table = Table()
    table.add_column("Year", style="cyan")
    table.add_column("Active", justify="right")
    table.add_column("Cumulative", justify="right", style="green")
    table.add_column("New Persons", justify="right")
    table.add_column("New Edges", justify="right")
    table.add_column("Density", justify="right", style="yellow")

    for year in data.get("years", []):
        snap = data["snapshots"].get(str(year), {})
        table.add_row(
            str(year),
            str(snap.get("active_persons", 0)),
            str(snap.get("cumulative_persons", 0)),
            str(snap.get("new_persons", 0)),
            str(snap.get("new_edges", 0)),
            f"{snap.get('density', 0):.4f}",
        )

    console.print(table)

    trends = data.get("trends", {})
    if trends:
        console.print("\n[bold]Trends:[/bold]")
        console.print(f"  Person growth: +{trends.get('person_growth', 0)}")
        console.print(f"  Edge growth: +{trends.get('edge_growth', 0)}")
        console.print(f"  Avg new persons/year: {trends.get('avg_new_persons_per_year', 0)}")


@app.command(name="genre-affinity")
def genre_affinity_cmd(
    person_id: str = typer.Argument(help="人物ID"),
) -> None:
    """人物のジャンル親和性を表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "genre_affinity.json"
    if not path.exists():
        console.print("[yellow]No genre_affinity.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    if person_id not in data:
        console.print(f"[yellow]No genre data for: {person_id}[/yellow]")
        raise typer.Exit()

    info = data[person_id]
    console.print(f"\n[bold blue]Genre Affinity: {person_id}[/bold blue]\n")

    console.print(f"  Primary tier: [bold]{info['primary_tier']}[/bold]")
    console.print(f"  Primary era: [bold]{info['primary_era']}[/bold]")
    if info.get("avg_anime_score"):
        console.print(f"  Avg anime score: {info['avg_anime_score']:.2f}")

    if info.get("score_tiers"):
        table = Table(title="Score Tier Distribution")
        table.add_column("Tier", style="cyan")
        table.add_column("Percentage", justify="right", style="green")
        for tier, pct in sorted(info["score_tiers"].items(), key=lambda x: -x[1]):
            table.add_row(tier, f"{pct:.1f}%")
        console.print(table)

    if info.get("eras"):
        table = Table(title="Era Distribution")
        table.add_column("Era", style="cyan")
        table.add_column("Percentage", justify="right", style="green")
        for era, pct in sorted(info["eras"].items(), key=lambda x: -x[1]):
            table.add_row(era, f"{pct:.1f}%")
        console.print(table)


@app.command()
def productivity(
    top_n: int = typer.Option(30, "--top", "-n", help="表示件数"),
) -> None:
    """生産性指標ランキングを表示する."""
    import json as json_mod

    from src.utils.config import JSON_DIR

    path = JSON_DIR / "productivity.json"
    if not path.exists():
        console.print("[yellow]No productivity.json found. Run 'pixi run pipeline' first.[/yellow]")
        raise typer.Exit()

    data = json_mod.loads(path.read_text())

    console.print("\n[bold blue]Productivity Ranking[/bold blue]")
    console.print("[dim]クレジット密度による生産性指標[/dim]\n")

    items = sorted(data.items(), key=lambda x: x[1]["credits_per_year"], reverse=True)[:top_n]

    table = Table()
    table.add_column("#", justify="right", style="dim")
    table.add_column("Person ID", style="cyan")
    table.add_column("Credits/Year", justify="right", style="bold green")
    table.add_column("Total Credits", justify="right")
    table.add_column("Active Years", justify="right")
    table.add_column("Consistency", justify="right", style="yellow")

    for i, (pid, d) in enumerate(items, 1):
        table.add_row(
            str(i),
            pid[:25],
            f"{d['credits_per_year']:.1f}",
            str(d["total_credits"]),
            str(d["active_years"]),
            f"{d['consistency_score']:.2f}",
        )

    console.print(table)


@app.command(name="data-quality")
def data_quality() -> None:
    """データ品質スコアを表示する."""
    setup_logging()

    from src.analysis.data_quality import compute_data_quality_score
    from src.database import get_connection, get_db_stats, init_db

    conn = get_connection()
    init_db(conn)

    stats = get_db_stats(conn)
    total_credits = stats.get("credits", 0)
    total_persons = stats.get("persons", 0)
    total_anime = stats.get("anime", 0)

    credits_with_source = conn.execute(
        "SELECT COUNT(*) FROM credits WHERE source != ''"
    ).fetchone()[0] if total_credits else 0

    persons_with_score = conn.execute(
        "SELECT COUNT(*) FROM scores"
    ).fetchone()[0] if total_persons else 0

    anime_with_year = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NOT NULL"
    ).fetchone()[0] if total_anime else 0

    anime_with_score = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE score IS NOT NULL"
    ).fetchone()[0] if total_anime else 0

    source_count = conn.execute(
        "SELECT COUNT(DISTINCT source) FROM credits WHERE source != ''"
    ).fetchone()[0]

    latest_year_row = conn.execute(
        "SELECT MAX(year) FROM anime WHERE year IS NOT NULL"
    ).fetchone()
    latest_year = latest_year_row[0] if latest_year_row else None

    conn.close()

    result = compute_data_quality_score(
        stats={"latest_year": latest_year},
        credits_with_source=credits_with_source,
        total_credits=total_credits,
        persons_with_score=persons_with_score,
        total_persons=total_persons,
        anime_with_year=anime_with_year,
        total_anime=total_anime,
        anime_with_score=anime_with_score,
        source_count=source_count,
    )

    overall = result["overall_score"]
    if overall >= 80:
        color = "green"
    elif overall >= 50:
        color = "yellow"
    else:
        color = "red"

    console.print("\n[bold blue]Data Quality Score[/bold blue]")
    console.print(f"[bold {color}]Overall: {overall:.1f} / 100[/bold {color}]\n")

    dt = Table(title="Dimensions")
    dt.add_column("Dimension", style="cyan")
    dt.add_column("Score", justify="right", style="green")
    dt.add_column("Description")
    for dim, info in result["dimensions"].items():
        dt.add_row(dim.title(), f"{info['score']:.1f}", info["description"])
    console.print(dt)

    if result["recommendations"]:
        console.print("\n[bold yellow]Recommendations:[/bold yellow]")
        for rec in result["recommendations"]:
            console.print(f"  [yellow]- {rec}[/yellow]")


if __name__ == "__main__":
    app()


@app.command(name="resolve-check")
def entity_resolution_check(
    export_csv: str = typer.Option(None, "--export", "-e", help="Export matches to CSV for review"),
    min_confidence: float = typer.Option(0.0, "--min-conf", help="Minimum confidence to export"),
    max_confidence: float = typer.Option(1.0, "--max-conf", help="Maximum confidence to export"),
    review_csv: str = typer.Option(None, "--review", "-r", help="Calculate precision from reviewed CSV"),
) -> None:
    """Entity resolution evaluation report."""
    setup_logging()

    from src.analysis.entity_resolution import (
        cross_source_match,
        exact_match_cluster,
        romaji_match,
        similarity_based_cluster,
    )
    from src.analysis.entity_resolution_eval import (
        calculate_precision_from_review,
        export_matches_for_review,
        format_resolution_report,
        generate_resolution_report,
    )
    from src.database import get_connection, init_db
    from src.models import Person

    conn = get_connection()
    init_db(conn)

    person_rows = conn.execute("SELECT * FROM persons").fetchall()
    conn.close()

    if not person_rows:
        console.print("[yellow]No persons in database[/yellow]")
        return

    persons = [
        Person(
            id=row["id"],
            name_ja=row["name_ja"],
            name_en=row["name_en"],
            aliases=row["aliases"].split(",") if row["aliases"] else [],
        )
        for row in person_rows
    ]

    console.print(f"[cyan]Loaded {len(persons):,} persons[/cyan]\n")

    # Run resolution with tracking
    exact = exact_match_cluster(persons)
    cross = cross_source_match(persons)
    
    already_matched = set(exact) | set(cross)
    remaining = [p for p in persons if p.id not in already_matched]
    romaji = romaji_match(remaining)
    
    already_matched = already_matched | set(romaji)
    remaining = [p for p in persons if p.id not in already_matched]
    similarity = similarity_based_cluster(remaining, threshold=0.95)

    canonical_map = {**exact, **cross, **romaji, **similarity}
    strategy_breakdown = {
        "exact": exact,
        "cross_source": cross,
        "romaji": romaji,
        "similarity": similarity,
    }

    report = generate_resolution_report(persons, canonical_map, strategy_breakdown)
    console.print(format_resolution_report(report))

    if export_csv:
        export_matches_for_review(report, export_csv, min_confidence, max_confidence)
        console.print(f"\n[green]✓ Exported to {export_csv}[/green]")

    if review_csv:
        precision = calculate_precision_from_review(review_csv)
        console.print("\n[bold cyan]Precision by Strategy:[/bold cyan]")
        pt = Table()
        pt.add_column("Strategy")
        pt.add_column("Precision", justify="right")
        for strategy, prec in precision.items():
            color = "green" if prec >= 0.95 else "yellow" if prec >= 0.85 else "red"
            pt.add_row(strategy, f"[{color}]{prec * 100:.1f}%[/{color}]")
        console.print(pt)
