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
- **Users**: The primary key would be `user_id` which is renamed from `_id` for clarification.
- **Receipts**: The primary key is `receipt_id`, which is renamed from `_id` for clafification. And there is foregin key `user_id` to link **Users** table.
- **Receipt_items**: There is no primary key. The foregin key `barcode` links to **Brands** table and `receipt_id` links to **Receipts** table.
- **Brands**: I choose `barcode` as primary key and `brand_id` as surrogate key.
### Relatioships
- **Users** -> **Receipts**: one to many: One user can have many receipts and each receipt should belong to exactly one user.
- **Receipts** -> **Receipt_item**: one to many: One receipt can contain many items and each receipt_item belongs to exactly one receipt.
- **Brands** -> **Receipt_items**: one brand can appear in many items and each item belongs to one brand or may not have brand infomation.

## Task 2: Resolve Proposed Questions
### I. What are the top 5 brands by receipts scanned for most recent month?
To answer this question, I counted number of receipts for the same receipt_item using `barcode` and left join **Brands** for brand information. The query results are summarized in the table. It turns out that only two types of items are recorded in the recent month `2021-03`. And there is no brand information based on barcode from **Brands** table. Therefore, we manually check the description and found out the top 2 brands are: **Thindust** and **Mueller Austria**
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

### III. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
To answer this question, I averaged `total_spent` from **Receipts** based on `FINISHED` and `REJECTED` groups. Apparently, `ACeepcted` is greater.
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
To answer this question, I calculated the sum of `purchased_item_count` from **Receipts** based on `FINISHED` and `REJECTED` groups. Apparently, `ACeepcted` is greater.
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
To answer this question, I firstly create a cte to filter out users who created the account within past 6 months, then find receipts based on selected users, and calculate the spend based based on `final_price` from **Receipt_items**. Similar to question 1 and 2, there is no brand information from **Brands** for some items. And the top 1st item is actually yellow pepers, I choose the top 2nd item as the most spend brand: MILLER LITE
| BRAND_NAME |DESCRIPTION | TOTAL_SPEND |
| ------- | ----------- | ---------- |
|NULL | Yellow Bell Peper | 21395 |
|NULL | MILLER LITE 24 PACK 12OZ CAN | 6383 |

<details>
  <summary>SQL Codes</summary>
  
```sql
WITH selected_users AS (
    SELECT
        user_id
    FROM
        users
    WHERE
        DATE_TRUNC('month', created_date) >= (SELECT DATE_TRUNC('month', MAX(created_date) - INTERVAL '6 months') FROM users)
        AND DATE_TRUNC('year', created_date) = (SELECT DATE_TRUNC('year', MAX(created_date))  FROM users)
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
    5
```
</details>

### VI. Which brand has the most transactions among users who were created within the past 6 months?
Chose distinct `receipt_id` for each item and calculate the count. Similar to question 5, the top 1st item is banana, which has no brand. So we chose top 2nd: **Thindust** as the answer.
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



