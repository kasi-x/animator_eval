#= レンダリング・ディスパッチ

多重ディスパッチで ChartSpec → Figure → SVG を実行する。
各 renderers/*.jl が render_chart メソッドを追加する。
=#

"""
    render_chart(spec::AbstractChartSpec) -> Figure

チャート仕様を CairoMakie Figure に変換する。
各チャートタイプの renderers/*.jl がメソッドを定義。
"""
function render_chart end

"""
    get_color(spec::AbstractChartSpec, idx::Int) -> Colorant

パレットから色を取得（循環）。
"""
function get_color(spec, idx::Int)
    palette = spec.colors.palette
    hex = palette[mod1(idx, length(palette))]
    return parse(Colorant, hex)
end

"""
    build_axis_kwargs(spec) -> Dict{Symbol, Any}

AxisSpec からAxis コンストラクタ用の kwargs を構築する。
log scale は Axis 作成時に設定する必要がある（後から設定するとデータなしで
autolimits が走り、0 を含む場合にエラーになる）。
"""
function build_axis_kwargs(spec)
    kw = Dict{Symbol, Any}(:title => spec.title)
    if !isempty(spec.x_axis.label)
        kw[:xlabel] = spec.x_axis.label
    end
    if !isempty(spec.y_axis.label)
        kw[:ylabel] = spec.y_axis.label
    end
    if spec.x_axis.log_scale
        kw[:xscale] = log10
    end
    if spec.y_axis.log_scale
        kw[:yscale] = log10
    end
    return kw
end

"""
    apply_axis_limits!(ax, spec)

Axis 作成後に range 制限のみ適用する。
"""
function apply_axis_limits!(ax, spec)
    if spec.x_axis.range !== nothing
        xlims!(ax, spec.x_axis.range...)
    end
    if spec.y_axis.range !== nothing
        ylims!(ax, spec.y_axis.range...)
    end
end

"""
    clamp_for_log(vals::Vector, log_scale::Bool) -> Vector

log scale 用に 0 以下の値を最小正値の 0.1 倍にクランプする。
"""
function clamp_for_log(vals::Vector, log_scale::Bool)
    if !log_scale
        return vals
    end
    positives = filter(v -> v > 0, vals)
    min_pos = isempty(positives) ? 1.0 : minimum(positives)
    return [max(v, min_pos * 0.1) for v in vals]
end

"""
    make_figure_and_axis(spec) -> (Figure, Axis)

Figure + Axis 作成の共通ボイラープレート。
"""
function make_figure_and_axis(spec)
    fig = Figure(size = (900, spec.height))
    ax = Axis(fig[1, 1]; build_axis_kwargs(spec)...)
    apply_axis_limits!(ax, spec)
    return fig, ax
end

"""
    render_and_save(json_str::String, output_path::String)

JSON → パース → レンダリング → SVG 保存。Python から呼ばれるメインエントリポイント。
"""
function render_and_save(json_str::String, output_path::String)
    set_theme!(DARK_THEME)
    spec = parse_chart_spec(json_str)
    fig = render_chart(spec)
    CairoMakie.save(output_path, fig)
    return output_path
end
