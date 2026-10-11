
# Athena Analytics — Execution Results

**Project:** AWS Retail Data Engineering V1  
**Athena database:** `customer_intelligence`  
**Evidence type:** exported Athena query results (CSV)

## Results index

| Results | Repository SQL | Result file | Outcome |
|---|---|---|---|
| E06-A: Cross-Gold KPI snapshot | `sql/analytics/cross_domain/01_gold_kpi_snapshot.sql` | [`cross_gold_kpi_snapshot.csv`](athena/results/cross_gold_kpi_snapshot.csv) | Aggregate net revenue and completed-order counts match across three Gold tables |
| E06-B: Monthly revenue trend | `sql/analytics/daily_sales/01_monthly_revenue_trend.sql` | [`monthly_revenue_trend.csv`](athena/results/monthly_revenue_trend.csv) | 33 observed monthly rows; revenue and order totals match KPI snapshot |
| E06-C: Top customers | `sql/analytics/customer_360/01_top_customers.sql` | [`top_customers.csv`](athena/results/top_customers.csv) | 20 ranked customer rows returned |
| E06-D: Top products | `sql/analytics/product_performance/01_top_products.sql` | [`top_products.csv`](athena/results/top_products.csv) | 20 ranked product rows returned; one negative estimated COGS anomaly identified |


## E06-A — Cross-Gold KPI snapshot

| Gold model | Net sales | Completed-order count |
|---|---:|---:|
| Daily Sales | 122296158.61 | 77593 |
| Customer 360 | 122296158.61 | 77593 |
| Product Performance | 122296158.61 | 77593 |

**Result:** Both aggregate measures match across the three independently refreshed Gold models in the captured snapshot. 

## E06-B — Monthly revenue trend

- Observed monthly rows: **33**.
- First month: **2024-01-01**; last observed month: **2026-10-01**.
- Sum of monthly net sales: **122296158.61**.
- Sum of monthly completed orders: **77,593**.
- Highest observed month: **2026-01-01**, net sales **4073747.33**.


The monthly net sales and order totals match E06-A exactly.

## E06-C — Top customers

The exported result contains **20 customer rows**, with geography, acquisition channel, lifetime orders, units, net sales, average order value and last order date.

| Rank | Customer ID | Lifetime orders | Lifetime net sales |
|---:|---|---:|---:|
| 1 | `CUST007345` | 19 | 39472.74 |
| 2 | `CUST008566` | 18 | 37310.12 |
| 3 | `CUST009070` | 18 | 37198.02 |
| 4 | `CUST002003` | 15 | 36916.95 |
| 5 | `CUST005987` | 19 | 35043.00 |


## E06-D — Top products

The exported result contains **20 product rows** including product category, units, revenue, estimated COGS and estimated gross margin.

| Rank | Product ID | Category / subcategory | Completed orders | Units | Net sales |
|---:|---|---|---:|---:|---:|
| 1 | `PROD00403` | Electronics / Phones | 193 | 603 | 601835.99 |
| 2 | `PROD00102` | Grocery / Snacks | 178 | 574 | 550958.15 |
| 3 | `PROD00241` | Grocery / Beverages | 163 | 511 | 522267.70 |
| 4 | `PROD00487` | Home / Furniture | 176 | 514 | 510899.44 |
| 5 | `PROD00314` | Sports / Team Sports | 159 | 496 | 509448.64 |

- Combined net sales of the returned top 20 products: **10107358.36**. This is a subset, **not** the total product Gold revenue.

## Reproduction

1. Open the corresponding query in `sql/analytics/`.
2. Replace its database placeholder with `customer_intelligence`.
3. Run in Athena against the deployed Gold tables.
4. Export results as CSV and compare with the evidence files linked above.
5. Record query ID, execution timestamp, and any updated outputs separately if reproducing later; refreshed data may differ.


### Verified Athena analytics

## Known Findings and Limitations

### Product Cost and Margin Anomaly

Athena Product Performance results identified the following values for product `PROD00352`:

| Metric | Value |
|---|---:|
| Net sales | 478,055.12 |
| Estimated COGS | -2,285.00 |
| Estimated gross margin | 480,340.12 |
| Estimated gross margin percentage | 100.48% |

The negative estimated COGS results in a gross margin percentage exceeding 100%.

### Monthly Revenue Coverage

The monthly revenue analytics returned 33 observed monthly records.

- September 2026 is absent from the results.
- October 2026 contains partial-month data.
- Comparisons involving these periods should be interpreted cautiously.

The missing month has not been established as an ETL defect.

### Analytics Validation Scope

Four representative Athena analytics queries were executed and their CSV results captured:

1. Cross-Gold KPI reconciliation
2. Monthly revenue trend
3. Top customers
4. Top products





