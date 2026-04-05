#= Violin レンダラー =#

function render_chart(spec::ViolinSpec)
    # Violin/boxplot の KDE はデータ範囲外に伸びるため、log scale では
    # 負の値で log10 エラーが発生する。代わりにデータを log 変換し
    # 線形 axis にカスタム tick ラベルを付ける。
    use_log_y = spec.y_axis.log_scale
    kw = build_axis_kwargs(spec)
    if use_log_y
        delete!(kw, :yscale)  # log scale は手動変換で対応
    end
    fig = Figure(size = (900, spec.height))
    ax = Axis(fig[1, 1]; kw...)
    apply_axis_limits!(ax, spec)

    # 挿入順序を保持（Python dict の挿入順序と一致させる）
    group_names = collect(keys(spec.groups))

    all_log_vals = Float64[]  # tick 計算用

    for (i, name) in enumerate(group_names)
        raw_vals = spec.groups[name]
        if use_log_y
            vals = [log10(max(v, 0.1)) for v in raw_vals]
            append!(all_log_vals, vals)
        else
            vals = raw_vals
        end
        if length(vals) < 3
            continue
        end

        color = get_color(spec, i)

        if spec.raincloud && length(vals) >= 40
            # Half-violin (raincloud style)
            violin!(ax, fill(i, length(vals)), vals;
                color = (color, 0.3),
                side = :right,
                show_median = true,
            )
            # Box overlay
            boxplot!(ax, fill(i, length(vals)), vals;
                color = (color, 0.5),
                width = 0.15,
                show_outliers = false,
            )
        elseif spec.raincloud && length(vals) >= 5
            # Box + jitter for small samples
            boxplot!(ax, fill(i, length(vals)), vals;
                color = (color, 0.5),
                width = 0.3,
            )
        else
            # Standard violin
            violin!(ax, fill(i, length(vals)), vals;
                color = (color, 0.4),
                show_median = true,
            )
            if spec.show_box
                boxplot!(ax, fill(i, length(vals)), vals;
                    color = (color, 0.6),
                    width = 0.15,
                    show_outliers = false,
                )
            end
        end
    end

    ax.xticks = (1:length(group_names), group_names)
    ax.xticklabelrotation = pi/8

    # log 変換した場合はカスタム tick ラベルを設定
    if use_log_y && !isempty(all_log_vals)
        lo = floor(Int, minimum(all_log_vals))
        hi = ceil(Int, maximum(all_log_vals))
        ticks = collect(lo:hi)
        labels = [string(round(Int, 10.0^t)) for t in ticks]
        ax.yticks = (Float64.(ticks), labels)
    end

    return fig
end
