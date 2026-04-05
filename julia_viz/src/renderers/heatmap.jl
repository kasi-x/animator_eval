#= Heatmap レンダラー =#

function render_chart(spec::HeatmapSpec)
    fig, ax = make_figure_and_axis(spec)

    # Vector{Vector{Float64}} → Matrix{Float64} に変換
    nrows = length(spec.z)
    ncols = nrows > 0 ? length(spec.z[1]) : 0
    z = zeros(ncols, nrows)  # CairoMakie heatmap: x=cols, y=rows
    for j in 1:nrows
        for i in 1:ncols
            z[i, j] = spec.z[j][i]
        end
    end

    heatmap!(ax, 1:ncols, 1:nrows, z;
        colormap = :RdBu,
    )

    if ncols > 0
        ax.xticks = (1:ncols, spec.x_labels)
    end
    if nrows > 0
        ax.yticks = (1:nrows, spec.y_labels)
    end
    ax.xticklabelrotation = pi/6

    # テキストアノテーション
    if spec.show_text && ncols > 0 && nrows > 0
        for i in 1:ncols, j in 1:nrows
            val = z[i, j]
            text!(ax, i, j; text = string(round(val; digits=2)),
                align = (:center, :center),
                fontsize = 10,
                color = abs(val) > 1.0 ? :white : TEXT_COLOR,
            )
        end
    end

    Colorbar(fig[1, 2]; colormap = :RdBu, label = "z-score")

    return fig
end
