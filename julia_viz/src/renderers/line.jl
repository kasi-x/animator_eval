#= Line レンダラー =#

function render_chart(spec::LineSpec)
    fig, ax = make_figure_and_axis(spec)

    # 挿入順序を保持（Python dict の挿入順序と一致させる）
    series_names = collect(keys(spec.series))

    for (i, name) in enumerate(series_names)
        points = spec.series[name]
        xs = [p[1] for p in points]
        ys = clamp_for_log([p[2] for p in points], spec.y_axis.log_scale)

        color = get_color(spec, i)

        lines!(ax, xs, ys;
            color = color,
            linewidth = 2,
            label = name,
        )
        scatter!(ax, xs, ys;
            color = color,
            markersize = 4,
        )
    end

    if length(series_names) > 1
        axislegend(ax; position = :rt)
    end

    return fig
end
