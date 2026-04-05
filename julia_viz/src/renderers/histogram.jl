#= Histogram レンダラー =#

function render_chart(spec::HistogramSpec)
    fig, ax = make_figure_and_axis(spec)

    vals = clamp_for_log(spec.values, spec.x_axis.log_scale)
    color = get_color(spec, 1)
    hist!(ax, vals;
        bins = spec.nbins,
        color = (color, 0.7),
        strokecolor = (color, 1.0),
        strokewidth = 0.5,
    )

    return fig
end
