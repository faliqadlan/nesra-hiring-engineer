# Data dictionary

Daily e-commerce performance for about 500 online shops selling mostly beauty and personal-care products.

## Files

| File | Contents |
|---|---|
| `shops_90d.csv.gz` | 90 consecutive days of history, 2024-12-21 to 2025-03-20. |
| `delta.csv.gz` | The next incremental extract from the same source, delivered after `shops_90d.csv.gz`. Same columns. |

Both files are gzip-compressed UTF-8, comma-separated, with a header row. Fields containing commas are quoted.

## Grain

One row per **shop × product × date**. A row exists only when the product had any activity that day (views or sales); a missing product-day means no activity was recorded.

## Columns

| Column | Type | Unit | Description |
|---|---|---|---|
| `shop_id` | string | | Shop identifier, e.g. `SHP123456`. |
| `shop_name` | string | | Shop display name. |
| `platform` | string | | Marketplace the shop sells on: `TikTok Shop`, `Shopee` or `Tokopedia`. |
| `date` | date | `YYYY-MM-DD` | Calendar day of activity (local time, Asia/Jakarta). |
| `product_id` | string | | Product identifier, e.g. `PRD12345678`. Unique per shop listing. |
| `product_name` | string | | Product listing title. |
| `category` | string | | Product category. |
| `price` | integer | IDR | Average selling price per unit that day (after discounts). On days with no sales, the product's typical selling price. |
| `units_sold` | integer | units | Units sold that day. |
| `orders` | integer | orders | Orders containing the product that day. One order can contain several units. |
| `revenue` | integer | IDR | Gross merchandise value for the product that day, `price × units_sold`. |
| `ad_spend` | integer | IDR | Advertising spend attributed to the product that day. |
| `views` | integer | count | Product impressions that day. |
| `clicks` | integer | count | Product clicks (detail-page visits) that day. |

All money is in Indonesian rupiah (IDR), with no decimals.
