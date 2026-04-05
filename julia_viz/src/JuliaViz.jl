"""
JuliaViz — ChartSpec JSON → CairoMakie SVG レンダリング

Python 側の ChartSpec を JSON で受け取り、CairoMakie で論文品質 SVG を出力する。
"""
module JuliaViz

using CairoMakie
using JSON3
using Colors

include("themes/dark_theme.jl")
include("chart_types.jl")
include("render_dispatch.jl")
include("renderers/scatter.jl")
include("renderers/bar.jl")
include("renderers/violin.jl")
include("renderers/line.jl")
include("renderers/stacked_area.jl")
include("renderers/histogram.jl")
include("renderers/heatmap.jl")

export render_chart, render_and_save, DARK_THEME

end # module
