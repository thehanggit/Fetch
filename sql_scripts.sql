--- Take 1. create tables to extract all data from json files which are stored in the stage.
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

-- select * from receipt_items
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--- Task2. Write queries to answer below questions
-- Q1. What are the top 5 brands by receipts scanned for most recent month?
-- each receipt contain multiple items. Assume each item can be attributed to different brands.
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
)

select * from item_recent_month

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

-- Q2. How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
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

-- Q3. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
SELECT
    rewards_receipt_status,
    ROUND(AVG(total_spent), 2) AS average_spend
FROM
    receipts
WHERE
    rewards_receipt_status IN ('FINISHED', 'REJECTED')
GROUP BY
    rewards_receipt_status

-- Q4. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
SELECT
    rewards_receipt_status,
    SUM(purchased_item_count) AS total_num_purchased_items
FROM
    receipts
WHERE
    rewards_receipt_status IN ('FINISHED', 'REJECTED')
GROUP BY
    rewards_receipt_status

-- Q5. Which brand has the most spend among users who were created within the past 6 months?
-- 1. create a user table who created the account within past 6 months
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

-- Q6. Which brand has the most transactions among users who were created within the past 6 months?
-- definition: transactions mean number of receipts
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

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Quality Checks
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
FROM
    brands;

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

-- Users -> Receipts
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

select * from receipt_items
