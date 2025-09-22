SQL_HELPER_CATALOG_STR = """
Available SQL helpers:

- get_total_volume(year, month, [asset_class], [product_type], [product])
    → Total trading volume (SUM) for given filters.

- get_adv(year, month, [asset_class], [product_type], [product])
    → Average daily volume (ADV) for given filters.

- compare_yoy_volume(year, month, [asset_class], [product_type], [product])
    → Year-over-year volume comparison for same month (current vs previous year).

- compare_mom_volume(year, month, [asset_class], [product_type], [product])
    → Month-over-month volume comparison (current vs previous month).

- top_asset_classes_by_volume(year, [month], top_n, [product], [product_type])
    → Top N asset classes by total volume.

- top_asset_classes_by_adv(year, [month], top_n, [product], [product_type])
    → Top N asset classes by ADV.

- top_product_types_by_volume(year, [month], top_n, [asset_class], [product])
    → Top N product types (e.g. cash, derivative) by volume.

- top_product_types_by_adv(year, [month], top_n, [asset_class], [product])
    → Top N product types by ADV.

- top_products_by_volume(year, month, top_n, [asset_class], [product_type])
    → Top N products (leaf-level instruments) by volume.

- top_products_by_adv(year, [month], top_n, [asset_class], [product_type])
    → Top N products (leaf-level instruments) by ADV.

- total_volume_by_entity(year, [month], entity, [asset_class], [product_type], [product])
    → Total volume grouped by chosen entity (asset_class / product_type / product).

- trend_adv(year_start, year_end, [asset_class], [product_type], [product])
    → ADV trend over a range of years.

- month_over_month_volume(year, [asset_class], [product_type], [product])
    → Monthly volume trend within a single year.

- ytd_volume(year, upto_month, [asset_class], [product_type], [product])
    → Year-to-date volume up to a given month.

- pct_change_adv(year1, month1, year2, month2, [asset_class], [product_type], [product])
    → Percent change in ADV between two periods.

"""