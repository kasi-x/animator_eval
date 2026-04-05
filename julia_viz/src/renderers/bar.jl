#= Bar レンダラー =#

function render_chart(spec::BarSpec)
    fig, ax = make_figure_and_axis(spec)

    if spec.stacked_series !== nothing && !isempty(spec.stacked_series)
        # Stacked bar
        series_names = collect(keys(spec.stacked_series))
        n_cats = length(spec.categories)
        cumulative = zeros(n_cats)

        for (i, name) in enumerate(series_names)
            vals = spec.stacked_series[name]
            color = get_color(spec, i)
            if spec.orientation == "h"
                barplot!(ax, 1:n_cats, vals;
                    offset = cumulative,
                    color = color,
                    direction = :x,
                    label = name,
                )
            else
                barplot!(ax, 1:n_cats, vals;
                    offset = cumulative,
                    color = color,
                    label = name,
                )
            end
            cumulative .+= vals
        end

        if spec.orientation == "h"
            ax.yticks = (1:n_cats, spec.categories)
        else
            ax.xticks = (1:n_cats, spec.categories)
        end
        axislegend(ax; position = :rt)
    else
        # Simple bar
        n = length(spec.categories)
        color = get_color(spec, 1)
        if spec.orientation == "h"
            barplot!(ax, 1:n, spec.values;
                color = color,
                direction = :x,
            )
            ax.yticks = (1:n, spec.categories)
        else
            barplot!(ax, 1:n, spec.values;
                color = color,
            )
            ax.xticks = (1:n, spec.categories)
            ax.xticklabelrotation = pi/6
        end
    end

    return fig
end
