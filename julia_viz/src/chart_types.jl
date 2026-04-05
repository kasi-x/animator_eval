#= ChartSpec の Julia 構造体ミラー

Python 側の frozen dataclass を JSON3 で読み込むための構造体定義。
StructTypes.jl で JSON キーとフィールド名を対応付ける。
=#

using StructTypes

# ── 軸・色 ──

Base.@kwdef struct AxisSpec
    label::String = ""
    log_scale::Bool = false
    range::Union{Nothing, Vector{Float64}} = nothing
end
StructTypes.StructType(::Type{AxisSpec}) = StructTypes.Struct()

Base.@kwdef struct ColorMapping
    palette::Vector{String} = ["#f093fb"]
    opacity::Float64 = 0.7
end
StructTypes.StructType(::Type{ColorMapping}) = StructTypes.Struct()

# ── 説明メタ ──

Base.@kwdef struct ExplanationMeta
    question::String = ""
    reading_guide::String = ""
    key_findings::Vector{String} = String[]
    caveats::Vector{String} = String[]
    context::String = ""
    significance::String = ""
end
StructTypes.StructType(::Type{ExplanationMeta}) = StructTypes.Struct()

# ── チャート仕様 ──

# 共通フィールドを持つ抽象型
abstract type AbstractChartSpec end

Base.@kwdef struct ScatterSpec <: AbstractChartSpec
    chart_id::String
    title::String
    chart_type::String = "ScatterSpec"
    x::Vector{Float64} = Float64[]
    y::Vector{Float64} = Float64[]
    labels::Union{Nothing, Vector{String}} = nothing
    mode::String = "markers"
    show_regression::Bool = false
    label_top_n::Int = 0
    max_points::Int = 5000
    height::Int = 500
    x_axis::AxisSpec = AxisSpec()
    y_axis::AxisSpec = AxisSpec()
    colors::ColorMapping = ColorMapping()
    explanation::ExplanationMeta = ExplanationMeta()
end
StructTypes.StructType(::Type{ScatterSpec}) = StructTypes.Struct()

Base.@kwdef struct BarSpec <: AbstractChartSpec
    chart_id::String
    title::String
    chart_type::String = "BarSpec"
    categories::Vector{String} = String[]
    values::Vector{Float64} = Float64[]
    stacked_series::Union{Nothing, Dict{String, Vector{Float64}}} = nothing
    orientation::String = "v"
    bar_mode::String = "stack"
    height::Int = 500
    x_axis::AxisSpec = AxisSpec()
    y_axis::AxisSpec = AxisSpec()
    colors::ColorMapping = ColorMapping()
    explanation::ExplanationMeta = ExplanationMeta()
end
StructTypes.StructType(::Type{BarSpec}) = StructTypes.Struct()

Base.@kwdef struct ViolinSpec <: AbstractChartSpec
    chart_id::String
    title::String
    chart_type::String = "ViolinSpec"
    groups::Dict{String, Vector{Float64}} = Dict{String, Vector{Float64}}()
    raincloud::Bool = false
    show_box::Bool = true
    height::Int = 500
    x_axis::AxisSpec = AxisSpec()
    y_axis::AxisSpec = AxisSpec()
    colors::ColorMapping = ColorMapping()
    explanation::ExplanationMeta = ExplanationMeta()
end
StructTypes.StructType(::Type{ViolinSpec}) = StructTypes.Struct()

Base.@kwdef struct HistogramSpec <: AbstractChartSpec
    chart_id::String
    title::String
    chart_type::String = "HistogramSpec"
    values::Vector{Float64} = Float64[]
    nbins::Int = 30
    height::Int = 500
    x_axis::AxisSpec = AxisSpec()
    y_axis::AxisSpec = AxisSpec()
    colors::ColorMapping = ColorMapping()
    explanation::ExplanationMeta = ExplanationMeta()
end
StructTypes.StructType(::Type{HistogramSpec}) = StructTypes.Struct()

Base.@kwdef struct HeatmapSpec <: AbstractChartSpec
    chart_id::String
    title::String
    chart_type::String = "HeatmapSpec"
    z::Vector{Vector{Float64}} = Vector{Float64}[]
    x_labels::Vector{String} = String[]
    y_labels::Vector{String} = String[]
    show_text::Bool = true
    height::Int = 500
    x_axis::AxisSpec = AxisSpec()
    y_axis::AxisSpec = AxisSpec()
    colors::ColorMapping = ColorMapping()
    explanation::ExplanationMeta = ExplanationMeta()
end
StructTypes.StructType(::Type{HeatmapSpec}) = StructTypes.Struct()

Base.@kwdef struct LineSpec <: AbstractChartSpec
    chart_id::String
    title::String
    chart_type::String = "LineSpec"
    series::Dict{String, Vector{Vector{Float64}}} = Dict{String, Vector{Vector{Float64}}}()
    height::Int = 500
    x_axis::AxisSpec = AxisSpec()
    y_axis::AxisSpec = AxisSpec()
    colors::ColorMapping = ColorMapping()
    explanation::ExplanationMeta = ExplanationMeta()
end
StructTypes.StructType(::Type{LineSpec}) = StructTypes.Struct()

Base.@kwdef struct StackedAreaSpec <: AbstractChartSpec
    chart_id::String
    title::String
    chart_type::String = "StackedAreaSpec"
    x::Vector{Float64} = Float64[]
    series::Dict{String, Vector{Float64}} = Dict{String, Vector{Float64}}()
    height::Int = 500
    x_axis::AxisSpec = AxisSpec()
    y_axis::AxisSpec = AxisSpec()
    colors::ColorMapping = ColorMapping()
    explanation::ExplanationMeta = ExplanationMeta()
end
StructTypes.StructType(::Type{StackedAreaSpec}) = StructTypes.Struct()

# ── JSON → 型ディスパッチ用マッピング ──

const CHART_TYPE_MAP = Dict{String, Type}(
    "ScatterSpec"     => ScatterSpec,
    "BarSpec"         => BarSpec,
    "ViolinSpec"      => ViolinSpec,
    "HistogramSpec"   => HistogramSpec,
    "HeatmapSpec"    => HeatmapSpec,
    "LineSpec"        => LineSpec,
    "StackedAreaSpec" => StackedAreaSpec,
)

"""
    parse_chart_spec(json_str::String) -> AbstractChartSpec

JSON 文字列からチャートタイプを判別し、対応する構造体にパースする。
"""
function parse_chart_spec(json_str::String)
    # まず chart_type フィールドだけ読み取る
    raw = JSON3.read(json_str)
    chart_type = get(raw, :chart_type, "")
    T = get(CHART_TYPE_MAP, chart_type, nothing)
    if T === nothing
        error("Unknown chart_type: $chart_type")
    end
    return JSON3.read(json_str, T)
end
