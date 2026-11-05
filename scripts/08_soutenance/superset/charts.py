"""Definition des charts Superset (une entree par visuel de la soutenance). Chaque chart = un besoin des decideurs du CHU.
Les titres portent l'unite ; le message (insight) est porte par les blocs de texte de l'histoire (histoire.py)."""

PALETTE = "supersetColors"


def F(col, op, val):
    """Filtre simple (clause WHERE) au format adhoc de Superset."""
    return {"expressionType": "SIMPLE", "clause": "WHERE", "subject": col, "operator": op, "comparator": val}


def _base(viz, ds_id, **kw):
    p = {"viz_type": viz, "datasource": f"{ds_id}__table", "color_scheme": PALETTE, "adhoc_filters": [],
         "extra_form_data": {}, "time_range": "No filter", "row_limit": 1000}
    p.update(kw)
    return p


def big_number(ds, metric, subheader="", fmt=",d", filters=None):
    return _base("big_number_total", ds, metric=metric, subheader=subheader, y_axis_format=fmt,
                 adhoc_filters=filters or [], header_font_size=0.5, subheader_font_size=0.3, force_timestamp_formatting=False)


def bar(ds, x, metrics, groupby=None, horizontal=False, stack=False, filters=None, limit=1000, xtitle="", ytitle="",
        fmt=",.0f", labels=True, sort_by=None, sort_asc=False, bounds=(None, None), legend=None, rotate=0, order_metric=None):
    groupby = groupby or []
    if sort_by is None:  # tri par la mesure ; en barres horizontales la 1re categorie est en bas : tri croissant pour mettre le maximum en haut
        sort_by = metrics[0] if isinstance(metrics[0], str) else None
        sort_asc = horizontal
    return _base("echarts_timeseries_bar", ds, x_axis=x, x_axis_force_categorical=True, metrics=metrics, groupby=groupby,
                 adhoc_filters=filters or [], order_desc=True, row_limit=limit, orientation="horizontal" if horizontal else "vertical",
                 stack="Stack" if stack else None, show_legend=(len(metrics) > 1 or bool(groupby)) if legend is None else legend,
                 legendType="scroll", legendOrientation="top", rich_tooltip=True, show_value=labels, show_empty_columns=True,
                 x_axis_title=xtitle, x_axis_title_margin=30 if not horizontal else 15, y_axis_title=ytitle, y_axis_title_margin=45 if not horizontal else 30,
                 y_axis_title_position="Left" if not horizontal else "Bottom", y_axis_format=fmt, truncateYAxis=False,
                 y_axis_bounds=list(bounds), x_axis_sort=sort_by if not sort_asc or sort_by else None,
                 x_axis_sort_asc=sort_asc, xAxisLabelRotation=rotate, sort_series_type="sum", sort_series_ascending=False,
                 truncate_metric=True, comparison_type="values", annotation_layers=[], tooltipTimeFormat="smart_date",
                 zoomable=False, minorTicks=False, showTooltipTotal=bool(stack), showTooltipPercentage=False, orderDesc=True)


def line(ds, x, metrics, filters=None, xtitle="", ytitle="", fmt=",.0f", groupby=None):
    return _base("echarts_timeseries_line", ds, x_axis=x, x_axis_force_categorical=True, metrics=metrics, groupby=groupby or [],
                 adhoc_filters=filters or [], order_desc=True, row_limit=10000, seriesType="line", opacity=0.2, markerEnabled=False,
                 show_legend=bool(groupby), legendType="scroll", legendOrientation="top", rich_tooltip=True, zoomable=True,
                 x_axis_title=xtitle, x_axis_title_margin=40, y_axis_title=ytitle, y_axis_title_margin=55, y_axis_title_position="Left",
                 y_axis_format=fmt, truncateYAxis=False, y_axis_bounds=[None, None], x_axis_sort=x, x_axis_sort_asc=True,
                 xAxisLabelRotation=45, show_empty_columns=True, truncate_metric=True, comparison_type="values",
                 annotation_layers=[], forecastEnabled=False, tooltipTimeFormat="smart_date", minorTicks=False, logAxis=False)


def pie(ds, groupby, metric, filters=None, fmt=",d"):
    return _base("pie", ds, groupby=[groupby], metric=metric, adhoc_filters=filters or [], donut=True, innerRadius=40, outerRadius=80,
                 show_labels=True, label_type="percent", number_format=fmt, show_legend=True, legendType="scroll",
                 legendOrientation="bottom", labels_outside=False, label_line=False, show_total=True, sort_by_metric=True, row_limit=100)


def table_brut(ds, colonnes, tri):
    """Tableau en mode brut (lignes telles quelles) : tri = [(colonne, croissant)]."""
    return _base("table", ds, query_mode="raw", groupby=[], metrics=[], all_columns=colonnes, percent_metrics=[], adhoc_filters=[],
                 order_desc=True, row_limit=1000, page_length=0, include_search=False, table_timestamp_format="smart_date",
                 show_cell_bars=False, color_pn=False, allow_render_html=False, order_by_cols=[json_pair(t) for t in tri],
                 server_page_length=10, align_pn=False, column_config={})


def table(ds, groupby, metrics, filters=None, order=None, limit=1000):
    return _base("table", ds, query_mode="aggregate", groupby=groupby, metrics=metrics, all_columns=[], percent_metrics=[],
                 adhoc_filters=filters or [], order_desc=True, row_limit=limit, page_length=0, include_search=False,
                 table_timestamp_format="smart_date", show_cell_bars=False, color_pn=False, allow_render_html=False,
                 order_by_cols=[json_pair(order)] if order else [], server_page_length=10, align_pn=False, column_config={})


def json_pair(order):
    import json
    return json.dumps([order[0], order[1]])


def country_map(ds, entity, metric, filters=None, fmt=",.0f", scheme="dark_blue"):
    return _base("country_map", ds, select_country="france_regions", entity=entity, metric=metric, adhoc_filters=filters or [],
                 color_scheme="", linear_color_scheme=scheme, number_format=fmt, show_bubbles=False, row_limit=1000)
