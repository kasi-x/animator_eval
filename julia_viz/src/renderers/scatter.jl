#= Scatter レンダラー =#

function render_chart(spec::ScatterSpec)
    fig, ax = make_figure_and_axis(spec)

    x = clamp_for_log(spec.x, spec.x_axis.log_scale)
    y = clamp_for_log(spec.y, spec.y_axis.log_scale)

    # サブサンプル
    if length(x) > spec.max_points
        rng = Random.MersenneTwister(42)
        idx = sort(Random.randperm(rng, length(x))[1:spec.max_points])
        x = x[idx]
        y = y[idx]
    end

    color = get_color(spec, 1)
    scatter!(ax, x, y;
        color = (color, spec.colors.opacity),
        markersize = 4,
    )

    # 回帰線
    if spec.show_regression && length(x) >= 3
        mask = .!isnan.(x) .& .!isinf.(x) .& .!isnan.(y) .& .!isinf.(y)
        cx, cy = x[mask], y[mask]
        if length(cx) >= 3
            A = hcat(cx, ones(length(cx)))
            coeffs = A \ cy
            x_line = [minimum(cx), maximum(cx)]
            y_line = coeffs[1] .* x_line .+ coeffs[2]
            lines!(ax, x_line, y_line;
                color = colorant"#FFD166",
                linewidth = 2,
                linestyle = :dash,
            )
        end
    end

    return fig
end
