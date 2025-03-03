# Fetch Rewards Coding Exercise

In this exercise, we aim to design and implement a data model that tracks user receipts and items with associated brand information. Our primary goal is to structure these tables and relationships so that data is clearly organized with logical representative of real-world usage. Beyond modeling, the exercise includes writing queries to answer specific business questions, evaluating data quality issues, and crafting a formal email to communicate our findings to a business leader.

## Task 1: Data Review
This section summarizes my development to extract and normalize data from JSON file to final tables in Snowflake data warehouse, explore the relationship, and visualize them in a entity relationshi diagram (ERD). The details are shown by steps:

- For **receipts.json.gz** and **users.json.gz**, choose `gunzip` command to decompress. Note the **users.json.gz** contains a tar archive, we need `tar xzvf` to decompress it.
- Upload all files into Snowflake data warehouse in a stage using semi-structured VARIANT columns. Create structured tables: **users**, **brands**, and **receipts** that mirror the JSON structure and extract the data into their final forms.
- In **users.json**, the column `rewardsReceiptItemList` field is a nested array, indicating each receipt contains multiple items. So I chose to normalize it by creating a child table called **receipt_items** and link it back to the main **receipts** table using `receipt_id`.
- The **receipt_item** table contains columns `barcode`, `description`, `final_price`, `item_price`, `needs_fetch_review`, `partner_item_id`, `prevent_target_gap_points`, `quantity_purchased`, `user_flagged_barcode`, `user_flagged_new_item`, `user_flagged_price`, `user_flagged_quantity`.
<details>
  <summary>SQL Codes</summary>
  
``` sql
--create a users table for users.json data
CREATE OR REPLACE TABLE json_user_data (
    data_users VARIANT
);

COPY INTO json_user_data
FROM @TRANSFORM_DEV.DBT_HGAO_PERFORMANCE.TEST/users.json
FILE_FORMAT = (TYPE = 'JSON');

CREATE OR REPLACE TABLE users (
    user_id VARCHAR,
    active BOOLEAN,
    created_date TIMESTAMP_NTZ,
    last_login TIMESTAMP_NTZ,
    role VARCHAR,
    sign_up_source VARCHAR,
    state VARCHAR
);

INSERT INTO users
SELECT
    data_users:"_id"."$oid"::STRING AS user_id,
    data_users:"active"::BOOLEAN AS active,
    TO_TIMESTAMP_NTZ(data_users:"createdDate"."$date"::NUMBER / 1000) AS created_date,
    TO_TIMESTAMP_NTZ(data_users:"lastLogin"."$date"::NUMBER / 1000) AS last_login,
    data_users:"role"::STRING AS role,
    data_users:"signUpSource"::STRING AS sign_up_source,
    data_users:"state"::STRING AS state
FROM json_user_data;

-- select * from users

-- create brands table for brands.json data
-- note that there are nested keys ('ref' and 'id') for cpg, we decompose them into separate columns
CREATE OR REPLACE TABLE json_brands_data (
    data_brands VARIANT
);

COPY INTO json_brands_data
FROM @TRANSFORM_DEV.DBT_HGAO_PERFORMANCE.TEST/brands.json
FILE_FORMAT = (TYPE = 'JSON');

CREATE OR REPLACE TABLE brands (
    brand_id VARCHAR,
    barcode VARCHAR,
    brand_code VARCHAR,
    category VARCHAR,
    category_code VARCHAR,
    cpg_id VARCHAR,
    cpg_ref VARCHAR,
    name VARCHAR,
    top_brand BOOLEAN
);

INSERT INTO brands
SELECT
    data_brands:"_id"."$oid"::STRING AS brand_id,
    data_brands:"barcode"::STRING AS barcode,
    data_brands:"brandCode"::STRING AS brand_code,
    data_brands:"category"::STRING AS category,
    data_brands:"categoryCode"::STRING AS category_code,
    data_brands:"cpg"."$id"."$oid"::STRING AS cpg_id,
    data_brands:"cpg"."$ref"::STRING AS cpg_ref,
    data_brands:"name"::STRING AS name,
    data_brands:"topBrand"::BOOLEAN AS top_brand
FROM json_brands_data;

-- select * from brands
-- create receipts table for receipts.json
-- The 'rewardsReceiptItemList' is an array of objects. We can create a separate child table
CREATE OR REPLACE TABLE json_receipts_data (
    data_receipts VARIANT
);

COPY INTO json_receipts_data
FROM @TRANSFORM_DEV.DBT_HGAO_PERFORMANCE.TEST/receipts.json
FILE_FORMAT = (TYPE = 'JSON');

-- create header table as receipts for top-level receipt information
CREATE OR REPLACE TABLE receipts (
    receipt_id             VARCHAR,     
    bonus_points_earned    NUMBER,
    bonus_points_reason    VARCHAR,
    create_date            DATE,
    date_scanned           DATE,
    finished_date          DATE,
    modify_date            DATE,
    points_awarded_date    DATE,
    points_earned          NUMBER,        
    purchase_date          DATE, 
    purchased_item_count   NUMBER,
    rewards_receipt_status VARCHAR,
    total_spent            NUMBER,        
    user_id                VARCHAR
);

-- create child table that contains item information from each row of receipt
CREATE OR REPLACE TABLE receipt_items (
    receipt_id                VARCHAR, 
    barcode                   STRING,
    description               VARCHAR,
    final_price               NUMBER,   
    item_price                NUMBER,   
    needs_fetch_review        BOOLEAN,
    partner_item_id           VARCHAR,
    prevent_target_gap_points BOOLEAN,
    quantity_purchased        NUMBER,
    user_flagged_barcode      VARCHAR,
    user_flagged_new_item     BOOLEAN,
    user_flagged_price        NUMBER,   
    user_flagged_quantity     NUMBER
);


-- insert values into header table
INSERT INTO receipts
SELECT
    data_receipts:"_id"."$oid"::STRING AS receipt_id,
    data_receipts:"bonusPointsEarned"::NUMBER AS bonus_points_earned,
    data_receipts:"bonusPointsEarnedReason"::STRING AS bonus_points_reason,
    TO_DATE(TO_TIMESTAMP_NTZ(data_receipts:"createDate"."$date"::NUMBER / 1000)) AS create_date,
    TO_DATE(TO_TIMESTAMP_NTZ(data_receipts:"dateScanned"."$date"::NUMBER / 1000)) AS date_scanned,
    TO_DATE(TO_TIMESTAMP_NTZ(data_receipts:"finishedDate"."$date"::NUMBER / 1000)) AS finished_date,
    TO_DATE(TO_TIMESTAMP_NTZ(data_receipts:"modifyDate"."$date"::NUMBER / 1000)) AS modify_date,
    TO_DATE(TO_TIMESTAMP_NTZ(data_receipts:"pointsAwardedDate"."$date"::NUMBER / 1000)) AS points_awarded_date,
    data_receipts:"pointsEarned"::NUMBER AS points_earned,
    TO_DATE(TO_TIMESTAMP_NTZ(data_receipts:"purchaseDate"."$date"::NUMBER / 1000)) AS purchase_date,
    data_receipts:"purchasedItemCount"::NUMBER AS purchased_item_count,
    data_receipts:"rewardsReceiptStatus"::STRING AS rewards_receipt_status,
    data_receipts:"totalSpent"::NUMBER AS total_spent,
    data_receipts:"userId"::STRING AS user_id
FROM json_receipts_data;

-- insert values into child table
INSERT INTO receipt_items
SELECT
    data_receipts:"_id"."$oid"::STRING AS receipt_id,
    item.value:"barcode"::STRING AS barcode,
    item.value:"description"::STRING AS description,
    item.value:"finalPrice"::NUMBER AS final_price,
    item.value:"itemPrice"::NUMBER AS item_price,
    item.value:"needsFetchReview"::BOOLEAN AS needs_fetch_review,
    item.value:"partnerItemId"::STRING AS partner_item_id,
    item.value:"preventTargetGapPoints"::BOOLEAN AS prevent_target_gap_points,
    item.value:"quantityPurchased"::NUMBER AS quantity_purchased,
    item.value:"userFlaggedBarcode"::STRING AS user_flagged_barcode,
    item.value:"userFlaggedNewItem"::BOOLEAN AS user_flagged_new_item,
    item.value:"userFlaggedPrice"::NUMBER AS user_flagged_price,
    item.value:"userFlaggedQuantity"::NUMBER AS user_flagged_quantity
FROM json_receipts_data,
LATERAL FLATTEN(input => data_receipts:"rewardsReceiptItemList") item;
```
</details>
  
By exploration, there are 4 final tables that can be represented in an ERD shown below.
![image](https://github.com/user-attachments/assets/93f2c1d3-2897-409e-be27-8f06782a7861)

### Tables
- **users**: The primary key would be `user_id` which is renamed from `_id` for clarification.
- **receipts**: The primary key is `receipt_id`, which is renamed from `_id` for clafification. And there is foregin key `user_id` to link **Users** table.
- **receipt_items**: There is no primary key. The foregin key `barcode` links to **brands** table and `receipt_id` links to **receipts** table.
- **brands**: I choose `barcode` as primary key and `brand_id` as surrogate key.
### Relationships
- **users** -> **receipts**: one to many: One user can have many receipts and each receipt should belong to exactly one user.
- **receipts** -> **receipt_item**: one to many: One receipt can contain many items and each receipt_item belongs to exactly one receipt.
- **brands** -> **receipt_items**: One brand can appear in many items and each item belongs to one brand or may not have brand infomation after investigation.

## Task 2: Resolve Proposed Questions
### I. What are the top 5 brands by receipts scanned for most recent month?
To answer this question, I counted number of receipts for the same receipt_item using `barcode` and left join **brands** for brand information. The query results are summarized in the table. It turns out that only two types of items are recorded in the recent month `2021-03`. And there is no brand information based on barcode from **brands** table. Therefore, we manually check the description and found out the top 2 brands are: **Thindust** and **Mueller Austria**
| BARCODE | DESCRIPTION | BRAND_NAME  | NUMBER OF RECEIPTS_SCANNED |
| ------- | ----------- | ----- | -------------------------- |
|B07BRRLSVC | thindust summer face mask - sun protection neck gaiter for outdooractivities | NULL | 13 |
|B076FJ92M4 | mueller austria hypergrind precision electric spice/coffee grinder millwith large grinding capacity and hd motor also for spices, herbs, nuts,grains, white | NULL | 13 |
<details>
  <summary>SQL Codes</summary>
  
```sql
WITH item_recent_month AS (
    SELECT
        i.receipt_id,
        i.description,
        i.barcode,
        DATE_TRUNC('month', r.date_scanned) AS scan_month
    FROM
        receipt_items i
    JOIN
        receipts r
    ON
        i.receipt_id = r.receipt_id
    WHERE
        r.date_scanned >= DATE_TRUNC('month', (SELECT MAX(date_scanned) FROM receipts))
        AND r.date_scanned < DATEADD(month, 1, DATE_TRUNC('month', (SELECT MAX(date_scanned) FROM receipts)))
),

-- join the brand information with name and category
base_recent_month AS(
    SELECT
        i.barcode,
        MAX(i.description) AS description,
        MAX(b.name) AS brand_name,
        MAX(b.category) AS category,
        COUNT(DISTINCT i.receipt_id) AS receipts_scanned
    FROM
        item_recent_month i
    LEFT JOIN
        brands b
    ON
        i.barcode = b.barcode
    GROUP BY
        i.barcode
    ORDER BY
        receipts_scanned DESC
    LIMIT
        5
)

SELECT * from base_recent_month
```
</details>

### II. How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
To answer this question, I changed the time window condition from recent month to the previous month and summarized the results in the table below. Compared to the recent month's result, **Thindust** and **Mueller Austria** are still two top brands. **FlipBelt**, **Doritos**, and **Suave** can be ranked as third, fourth, and fifth respectively considering items for `4011` and `1234` are not recognized.
| BARCODE | DESCRIPTION | BRAND_NAME  | NUMBER OF RECEIPTS_SCANNED |
| ------- | ----------- | ----- | -------------------------- |
|B07BRRLSVC | thindust summer face mask - sun protection neck gaiter for outdooractivities | NULL | 40 |
|B076FJ92M4 | mueller austria hypergrind precision electric spice/coffee grinder millwith large grinding capacity and hd motor also for spices, herbs, nuts,grains, white | NULL | 40 |
|NULL| flipbelt level terrain waist pouch, neon yellow, large/32-35 | NULL| 36 |
|4011 | ITEM NOT FOUND | NULL | 33 |
|1234 | NULL| 5 |
|028400642255| DORITOS TORTILLA CHIP SPICY SWEET CHILI REDUCED FAT BAG 1 OZ | NULL | 3 |
|079400066619| SUAVE PROFESSIONALS MOISTURIZING SHAMPOO LIQUID PLASTIC BOTTLE RP 12.6 OZ - 0079400066612| NULL | 2 |

<details>
  <summary>SQL Codes</summary>
  
```sql
WITH item_previous_month AS (
    SELECT
        i.receipt_id,
        i.description,
        i.barcode,
        DATE_TRUNC('month', r.date_scanned) AS scan_month
    FROM
        receipt_items i
    JOIN
        receipts r
    ON
        i.receipt_id = r.receipt_id
    WHERE
        r.date_scanned >= DATEADD(month, -1, DATE_TRUNC('month', (SELECT MAX(date_scanned) FROM receipts)))
        AND r.date_scanned <= DATE_TRUNC('month', (SELECT MAX(date_scanned) FROM receipts))
),

-- join the brand information with name and category
base_previous_month AS(
    SELECT
        i.barcode,
        MAX(i.description) AS description,
        MAX(b.name) AS brand_name,
        MAX(b.category) AS category,
        COUNT(DISTINCT i.receipt_id) AS receipts_scanned
    FROM
        item_previous_month i
    LEFT JOIN
        brands b
    ON
        i.barcode = b.barcode
    GROUP BY
        i.barcode
    ORDER BY
        receipts_scanned DESC
    LIMIT
        7
)

SELECT * from base_previous_month
```
</details>

### III. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
To answer this question, I averaged `total_spent` from **receipts** based on `FINISHED` and `REJECTED` groups. Apparently, `Accepted` is greater.
| REWARD_RECEIPTS_STATUS | AVERAGE_SPEND |
| ------- | ----------- |
|FINISHED | 80.86 |
|REJECTED | 23.35 |

<details>
  <summary>SQL Codes</summary>
  
```sql
SELECT
    rewards_receipt_status,
    ROUND(AVG(total_spent), 2) AS average_spend   
FROM
    receipts
WHERE
    rewards_receipt_status IN ('FINISHED', 'REJECTED')
GROUP BY
    rewards_receipt_status
```
</details>

### IV. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater? 
To answer this question, I calculated the sum of `purchased_item_count` from **Receipts** based on `FINISHED` and `REJECTED` groups. Apparently, `Accepted` is greater.
| REWARD_RECEIPTS_STATUS | TOTAL_NUM_PURCHASED_ITEMS |
| ------- | ----------- |
|FINISHED | 8184 |
|REJECTED | 173 |

<details>
  <summary>SQL Codes</summary>
  
```sql
SELECT
    rewards_receipt_status,
    SUM(purchased_item_count) AS total_num_purchased_items 
FROM
    receipts
WHERE
    rewards_receipt_status IN ('FINISHED', 'REJECTED')
GROUP BY
    rewards_receipt_status
```
</details>

### V. Which brand has the most spend among users who were created within the past 6 months?
To answer this question, I firstly create a cte to filter out users who created the account within past 6 months, then find receipts based on selected users, and calculate the spend based based on `final_price` from **receipt_items**. Similar to Q1 and Q2, we can only identify brand from `description`, which should be HUGGIES.
| BRAND_NAME |DESCRIPTION | TOTAL_SPEND |
| ------- | ----------- | ---------- |
|NULL | HUGGIES SIMPLY CLEAN PREMOISTENED WIPE FRAGRANCE FREE BAG 216 COUNT | 32340 |

<details>
  <summary>SQL Codes</summary>
  
```sql
WITH selected_users AS (
    SELECT
        user_id
    FROM
        users
    WHERE
        created_date >= DATEADD(month, -6, (SELECT MAX(created_date) FROM users))
),

-- 2. find receipts based on selected users
selected_receipts AS (
    SELECT
        r.receipt_id,
        r.user_id
    FROM
        receipts r
    JOIN
        selected_users u
    ON
        r.user_id = u.user_id
)

-- 3. calculate spend on brand
SELECT
    MAX(b.brand_id) AS brand_id,
    MAX(b.name) AS brand_name,
    MAX(i.description) AS description,
    SUM(i.final_price) AS total_spend
FROM 
    receipt_items i
JOIN
    selected_receipts rr
  ON
    i.receipt_id = rr.receipt_id
LEFT JOIN
    brands b
  ON
    i.barcode = b.barcode
GROUP BY
    i.barcode
ORDER BY
    total_spend DESC
LIMIT
    1
```
</details>

### VI. Which brand has the most transactions among users who were created within the past 6 months?
Chose distinct `receipt_id` for each item and calculate the count. The top 1st item is banana, which has no brand according to `description`. So we chose top 2nd: **Thindust** as the answer.
| BRAND_NAME |DESCRIPTION | TRANSACTION_COUNT |
| ------- | ----------- | ---------- |
|NULL | Yellow Bananas | 113 |
|NULL | thindust summer face mask - sun protection neck gaiter for outdooractivities | 54 | 

<details>
  <summary>SQL Codes</summary>
  
```sql
WITH selected_users AS (
    SELECT
        user_id
    FROM
        users
    WHERE
        created_date >= DATEADD(month, -6, (SELECT MAX(created_date) FROM users))
),

-- 2. find receipts based on selected users
selected_receipts AS (
    SELECT
        r.receipt_id,
        r.user_id
    FROM
        receipts r
    JOIN
        selected_users u
    ON
        r.user_id = u.user_id
)

-- 3. calculate transactions
SELECT
    MAX(b.brand_id) AS brand_id,
    MAX(b.name) AS name,
    MAX(b.category) AS category,
    MAX(i.description) AS description,
    COUNT(DISTINCT r.receipt_id) AS transaction_count
FROM
    receipt_items i
JOIN
    selected_receipts r
  ON
    i.receipt_id = r.receipt_id
LEFT JOIN
    brands b
  ON
    i.barcode = b.barcode
GROUP BY
    i.barcode
ORDER BY
    transaction_count DESC
LIMIT
    5
```
</details>

## Task 3: Data Quality Evaluation
In general, I conduct this evaluation by two steps: 1. Dive into each table first for missing values, duplicate rows, and outliers; 2. Check the integrity between tables.

### users table: 495 rows
- There are 48 missing rows for `sign_up_resource`, which turns out to be the same user information with 48 duplicated rows.
- There are only 212 unique `user_id` with 70 user_id that have duplicated rows. The duplcaited percentage is 33%

<details>
  <summary>SQL Codes</summary>
  
```sql
-- Users
-- check missing values
SELECT 
    COUNT(*) AS total_rows,
    COUNT(user_id) AS non_null_user_id,
    COUNT(active) AS non_null_active,
    COUNT(created_date) AS non_null_created_date,
    COUNT(role) AS non_null_role,
    COUNT(sign_up_source) AS non_sign_up_source,
    COUNT(*) - COUNT(user_id) AS missing_user_id,
    COUNT(*) - COUNT(active) AS missing_active,
    COUNT(*) - COUNT(created_date) AS missing_created_date,
    COUNT(*) - COUNT(role) AS missing_role,
    COUNT(*) - COUNT(sign_up_source) AS missing_sign_up_source
FROM users;

select * from users
-- check duplicate rows
WITH duplicated_rows AS (
SELECT
    user_id,
    created_date,
    COUNT(*) AS cnt
FROM 
    users
GROUP BY
    user_id, created_date
HAVING
    COUNT(*) > 1   
)

SELECT
    COUNT(DISTINCT d.user_id) AS num_duplicated_user_id,
    COUNT(DISTINCT u.user_id) AS num_total_user_id,
    COUNT(DISTINCT d.user_id)*1.0/
    COUNT(DISTINCT u.user_id) AS duplicated_pct
FROM
    users u
left join
    duplicated_rows d
on
    u.user_id = d.user_id
```
</details>


### brands table: 1167 rows
- There are 7 brands that have different `brand_id` but share the same `barcode`: 511111504788, 511111305125, 511111504139, 511111204923, 511111605058, 511111004790, 511111704140.
- Significant `category_code` (around 55.7%) is missing.

<details>
  <summary>SQL Codes</summary>
  
```sql
-- Brands
SELECT 
    COUNT(*) AS total_rows,
    COUNT(brand_id) AS non_null_brand_id,
    COUNT(barcode) AS non_null_barcode,
    COUNT(brand_code) AS non_null_brand_code,
    COUNT(category) AS non_null_category,
    COUNT(category_code) AS non_null_category_code,
    COUNT(cpg_id) AS non_null_cpg_id,
    COUNT(cpg_ref) AS non_null_cpg_ref,
    COUNT(name) AS non_null_name,
    COUNT(top_brand) AS non_null_top_brand,
    
    COUNT(*) - COUNT(brand_id) AS missing_brand_id,
    COUNT(*) - COUNT(barcode) AS missing_barcode,
    COUNT(*) - COUNT(brand_code) AS missing_brand_code,
    COUNT(*) - COUNT(category) AS missing_category,
    COUNT(*) - COUNT(category_code) AS missing_category_code,
    COUNT(*) - COUNT(cpg_id) AS missing_cpg_id,
    COUNT(*) - COUNT(cpg_ref) AS missing_cpg_ref,
    COUNT(*) - COUNT(name) AS missing_name,
    COUNT(*) - COUNT(top_brand) AS missing_top_brand
FROM brands;

-- notice significant category_code missing
SELECT SUM(CASE WHEN category_code IS NULL THEN 1 ELSE 0 END)*1.0 / COUNT(*) AS pct FROM brands

-- check duplicated rows by barcode
WITH duplicated_rows AS (
    SELECT
        barcode,
        COUNT(*) AS cnt
    FROM 
        brands
    GROUP BY
        barcode
    HAVING COUNT(*) > 1   
)

SELECT * FROM brands WHERE barcode IN (511111504788, 511111305125, 511111504139, 511111204923, 511111605058, 511111004790, 511111704140)
```
</details>

### receipts table: 1119 rows
- It seems there is no obvious quality issue. Every `receipt_id` is unique.
- Checked `NULL` in `purchased_item_count` and `total_spent`. They only appear when `rewards_receipt_status` in ('PENDING', 'REJECTED'), which is reasonable.
- Checked extreme values for `purchased_item_count` and `total_spent` using 99 percentile value as thresholds. The extreme counts and spent align with each other, namely, no extreme spent with few counts or extreme counts with small amount of spent. Two of them who may have potential data quality issue are already flagged in `rewards_receipt_status`.

<details>
  <summary>SQL Codes</summary>
  
```sql
-- Receipts
-- check missing values
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT receipt_id) AS num_receipts,
    SUM(CASE WHEN purchased_item_count IS NULL AND rewards_receipt_status NOT IN ('SUBMITTED', 'PENDING') THEN 1 ELSE 0 END) AS missing_count,
    SUM(CASE WHEN rewards_receipt_status IS NULL THEN 1 ELSE 0 END) AS missing_status,
    SUM(CASE WHEN total_spent IS NULL AND rewards_receipt_status NOT IN ('SUBMITTED', 'PENDING') THEN 1 ELSE 0 END) AS missing_total_spent
FROM
    receipts;

-- check extreme values for item count 
WITH threshold AS (
    SELECT 
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY purchased_item_count) 
        AS p99_purchased_item_count,
    FROM receipts
)

SELECT
    r.*
FROM 
    receipts r,
    threshold t
WHERE
    r.purchased_item_count IS NOT NULL 
    AND r.purchased_item_count > t.p99_purchased_item_count
ORDER BY
    r.purchased_item_count DESC

-- check extreme values for total spent
WITH threshold AS (
    SELECT 
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_spent) 
        AS p99_total_spent,
    FROM receipts
)

SELECT
    r.*
FROM 
    receipts r,
    threshold t
WHERE
    r.total_spent IS NOT NULL 
    AND r.total_spent > t.p99_total_spent
ORDER BY
    r.total_spent DESC
```
</details>

### **users** -> **receipts**
We need to check whether all users in **receipts** can be identified in the **users** table. It turns out there are 117 unique `user_id` that can not be identified in **users** table.

<details>
  <summary>SQL Codes</summary>
  
```sql
SELECT
    DISTINCT r.user_id
FROM
    receipts AS r
LEFT JOIN
    users AS u
ON
    r.user_id = u.user_id
WHERE
    u.user_id IS NULL
```
</details>



### receipt_items: 6941 rows
Since this is a child table extracted from **receipts**, there is no field description. 
- 3851 items are missing barcodes, accounting for 55.48% of the total data.
- 150 items have no information about `barcode`, `description`, `final_price`, `item_price`, and `quantity_purchased`, which needs to be reviewed.
- Not clear about the difference between `final_price` and `item_price`.
- `description` has inconsistencies in capitalization.
  
In general, there are missing and unstructured information that needs to be handled. Also, since **receipt_item** are strongly correlated with **receipts** and **brands**, we need to develop advanced checks for quality issues.

<details>
  <summary>SQL Codes</summary>
  
```sql
-- Receipt_items
-- check barcode
SELECT
    COUNT(*)
FROM
    receipt_items
WHERE
    barcode IS NULL

-- check both barcode and description
SELECT
    *
FROM
    receipt_items
WHERE
    barcode IS NULL
    AND description IS NULL
    AND final_price IS NULL
    AND item_price IS NULL
```
</details>

### receipts -> receipt_items
Since one receipt may contain multiple receipt items, we need to make sure the `purchased_item_count` in **receipts** aligns with summation of `quantity_purchased`. The logic is similar to `total_spent` and `final_price`.
- There are 57 receipts where the total amount spent does not match the sum of individual item costs.
- There are 40 receipts where the total counts does not match the sum of indiviual item counts.

<details>
  <summary>SQL Codes</summary>
  
```sql
-- receipts -> receipt_items
-- check total spent
WITH items_cost as (
    SELECT
        receipt_id,
        SUM(final_price) AS all_spent
    FROM
        receipt_items
    GROUP BY
        receipt_id
) 

SELECT
    r.receipt_id,
    r.total_spent,
    i.all_spent
FROM
    receipts r
JOIN
    items_cost i
ON
    r.receipt_id = i.receipt_id
WHERE
    r.total_spent != i.all_spent

-- check total counts
WITH items_count as (
    SELECT
        receipt_id,
        SUM(quantity_purchased) AS total_counts
    FROM
        receipt_items
    GROUP BY
        receipt_id
) 

SELECT
    r.receipt_id,
    r.purchased_item_count,
    i.total_counts
FROM
    receipts r
JOIN
    items_count i
ON
    r.receipt_id = i.receipt_id
WHERE
    r.purchased_item_count != i.total_counts
```
</details>

### brands -> receipt_items
In Task 2, we identified certain items without brand information. We need to compile a list of these items for future brand labeling.
- There are 553 distinct barcodes, 3000 rows from **receipt_items** that can not be identified from `brands` table, which needs to contain more brand information to label the items in receipts.
- In **brand** table, `barcode` only contains numeric values, whereas in **receipt_items** table, it includes string.
- In **receipt_items** table, many items' `barcode` = 4011 means 'ITEM NOT FOUND', which should be labeled correctly. `barcode` = 1234 has no description information, which should be furtherly investigated.

<details>
<summary>SQL Codes</summary>
  
```sql
-- brands -> receipt_items
-- brand identification using barcode
WITH barcode_item AS (
    SELECT
        DISTINCT r.barcode
    FROM
        receipt_items AS r
    LEFT JOIN
        brands AS b
    ON
        r.barcode = b.barcode
    WHERE
        b.barcode IS NULL
)

SELECT
    r.*
FROM
    receipt_items r
JOIN
    barcode_item b
ON
    r.barcode = b.barcode
```
</details>

## Task 4: Communicate with Stakeholders

**Subject**: Data Quality Issues for Fetch Reward Datasets

Dear [product manager/business leader name],

Hope everything is well! I'd like to share an update on the data quality review I recently completed for our Fetch Rewards datasets, including users, receipts, items in each receipt and brands tables. I conducted automated data validation checks to uncover potential issues missing values, duplicates, and referential inconsistences between datasets. Here are the key findings and my proposed next steps to address them:

#### Key Findings

1. Large portion of items mssing brand references and details
   
   - More than half of the items in our receipts cannot be matched to entities in brands table.
   - Many items are labeled 'ITEM NOT FOUND' or contain in complete descriptions, and the brand information is contained in the description.
   - These gaps make it harder to accurately track purchases. As a result, our system may fail to recongize these items and award corresponding points to customers, which could reduce their engagement and gave us negative feedback.
   - **Recommendation**: Prioritize expanding and updating the brands table to cover missing products by taking advantage of description from the item tables
2. Inconsistent item counts and total spent

   - Around 4.5% of receipts have recorded inconsistent item counts and total money spent. While not severe, these discrepancies can affect point calculations.
   - **Recommendation**: The engineering team should regularly review and fix these inconsistencies to ensure accurate reward allocations.
3. Missing values and duplicates in user table

   - 10% of receipts cannot be matched to a known user, meaning some customers may not receive their rewards.
   - The users table also contains 33% duplicate records, complicating user account management.
   - **Recommendation**: Investigate incomplete user data, remove or merge duplicates, and establish process to prevent future inconsistencies.

4. Inconsistent barcodes in brand table

   - Discovered different brands sharing the same barcode, causing confusion and misattribution when awarding brand-specific points.
   - **Recommendation**: Eliminate duplicate barcodes by consolidating brand entries and enforcing a unique barcode constraint.
  
#### Proposed Next Steps

In general, we need to firstly address the data quality issues I proposed by recommendations, set up automated alerts for missing values, duplicates, and referential inconsistencies. Then, schedule periodic audits to maintain data integrity as the datasets grows to a much bigger size.

Addressing these issues is critical for our rewards program to function smoothly and provide a positive experience for our customers. Please let me know if you have any questions or would like to discuss these recommendations further. I appreciate your support and look forward to working with you to enhance our data quality.

Best

Hang Gao

Engineer Analytics
   
   

   
    

   




