#= Stacked Area レンダラー =#

function render_chart(spec::StackedAreaSpec)
    fig, ax = make_figure_and_axis(spec)

    x = spec.x
    # 挿入順序を保持（Python dict の挿入順序と一致させる）
    series_names = collect(keys(spec.series))

    n = length(x)
    cumulative = zeros(n)

    for (i, name) in enumerate(series_names)
        vals = spec.series[name]
        lower = copy(cumulative)
        upper = cumulative .+ vals
        color = get_color(spec, i)

        # 塗りつぶし (band)
        band!(ax, x, lower, upper;
            color = (color, 0.4),
            label = name,
        )
        # 上端の線
        lines!(ax, x, upper;
            color = color,
            linewidth = 1.5,
        )

        cumulative .= upper
    end

    if length(series_names) > 1
        axislegend(ax; position = :lt)
    end

    return fig
end
