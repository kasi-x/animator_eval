#= ダークテーマ — Plotly 暗色テーマとの統一 =#

const DARK_BG = colorant"#24243e"
const DARK_PLOT_BG = RGBA(0, 0, 0, 0.2)
const TEXT_COLOR = colorant"#c0c0d0"
const GRID_COLOR = RGBA(1, 1, 1, 0.08)

# アクセント色パレット (Plotly版と共通)
const ACCENT_PALETTE = [
    colorant"#f093fb",  # pink
    colorant"#a0d2db",  # cyan
    colorant"#f5576c",  # red
    colorant"#fda085",  # orange
    colorant"#667eea",  # indigo
    colorant"#06D6A0",  # green
    colorant"#FFD166",  # yellow
    colorant"#EF476F",  # crimson
]

"""
    dark_theme() -> Theme

Plotly 暗色テーマと一致する CairoMakie テーマを返す。
"""
function dark_theme()
    Theme(
        backgroundcolor = DARK_BG,
        textcolor = TEXT_COLOR,
        Axis = (
            backgroundcolor = DARK_PLOT_BG,
            xgridcolor = GRID_COLOR,
            ygridcolor = GRID_COLOR,
            xtickcolor = TEXT_COLOR,
            ytickcolor = TEXT_COLOR,
            xlabelcolor = TEXT_COLOR,
            ylabelcolor = TEXT_COLOR,
            titlecolor = TEXT_COLOR,
            xticklabelcolor = TEXT_COLOR,
            yticklabelcolor = TEXT_COLOR,
            bottomspinecolor = GRID_COLOR,
            leftspinecolor = GRID_COLOR,
            topspinevisible = false,
            rightspinevisible = false,
        ),
        Legend = (
            backgroundcolor = RGBA(0, 0, 0, 0.3),
            framecolor = GRID_COLOR,
            labelcolor = TEXT_COLOR,
            titlecolor = TEXT_COLOR,
        ),
        fontsize = 12,
        fonts = (; regular = "sans-serif"),
    )
end

const DARK_THEME = dark_theme()
