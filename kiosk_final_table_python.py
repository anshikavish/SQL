import mysql.connector

# Establish the MySQL connection
conn = mysql.connector.connect(
    host='host_name',
    database='database',
    user='username',
    password=r'password'
)

# Create a cursor object
cursor = conn.cursor()
print("start")

# Define SQL statements
sql_script = """
-- Step 1: Create the Buffer Table with Data
CREATE TABLE kiosk.kiosk_final_buffer AS
WITH ssid_counts AS (
    SELECT
        sdssId,
        COUNT(*) AS ssid_count
    FROM kiosk.sales_detail
    GROUP BY sdssId
),
BillData AS (
    SELECT
        psd.sdBatchId AS batch_id,
        psd.sdBatchCode AS batch_code,
        psd.sdRate AS Rate,
        psd.sdBillingRate AS billing_rate,
        psd.sdTaxPerc AS tax_perc,
        psd.sdQty AS Qty,
        psd.sdAmount AS Amount,
        psd.sdTaxAmt AS tax_amt,
        psd.sdLineNetAmount AS line_net_amount,
        psd.sdDiscTotPerc AS disc_tot_perc,
        psd.sdDiscTotAmount AS disc_tot_amount,
        psd.sdCostPriceFIFO AS cost_price_FIFO,
        pss.EmployeeCode AS employee_code,
        pss.EmployeeName AS employee_name,
        pss.ssDate AS Date,
        pss.ssPartyName AS party_name,
        pss.ssItemTotal AS item_total,
        pss.ssItemDiscountTotal AS item_disc_total,
        pss.ssTotalTax AS total_tax,
        pss.ssNetAmount AS net_amount,
        pss.ssDocStartTS AS Timestamp,
        pss.ssPayTotal AS Bill_amount,
        pss.ssFormatedNo AS Bill_no,
        pss.ssBillPaidDate AS bill_paid_date,
        pss.ssSL AS ssSL,
        nl.nlCode AS nlCode,
        nl.nlName AS nlName,
        nl.zhZonalHead AS ASM_Name,
        nl.nlAccountId AS nlaccountId,
        nl.nlAdd1 AS nlAdd1,
        nl.nlAdd2 AS nlAdd2,
        nl.nlAdd3 AS nlAdd3,
        nl.nlPlace AS nlPlace,
        nl.nlPhone1 AS nlPhone1,
        nl.nlPhone2 AS nlPhone2,
        nl.nlState AS nlState,
        nl.nlTs AS nlTs,
        nl.nlPinCode AS nlPinCode,
        nl.nlVertical AS nlVertical,
        nl.nlArea AS nlArea,
        nl.nlID AS nlID,
        nl.nlShortName AS nlShortName,
        psd.sdStoreId as Store_Id,
        pm.pmCode AS pmCode,
        pm.pmForm AS pmForm,
        pm.pmMRP AS pmMRP,
        pm.pmLSaleRate AS pmLSaleRate,
        pm.pmTs AS pmTs,
        pm.pmShortName AS pmShortName,
        pm.pmSize AS pmSize,
        pm.pmBarCode AS pmBarCode,
        pm.pmID AS pmID,
        ROW_NUMBER() OVER (PARTITION BY pss.ssFormatedNo ORDER BY psd.sdTS) AS row_num
    FROM
        kiosk.sales_detail psd
    JOIN
        kiosk.nodeslist1 nl ON psd.sdStoreId = nl.nlID
    LEFT JOIN (
        kiosk.sales_summary pss
        INNER JOIN ssid_counts psc ON pss.ssid = psc.sdssId
    ) ON (
        (psd.sdssId = pss.ssid AND psc.ssid_count = 1) OR
        (psd.sdStoreId = pss.ssStoreId AND psc.ssid_count > 1 AND psd.sdssId = pss.ssid)
    )
    JOIN
        kiosk.product_master pm ON psd.sdItemCode = pm.pmCode
)
SELECT
    batch_id,
    batch_code,
    Rate,
    billing_rate,
    tax_perc,
    Qty,
    Amount,
    tax_amt,
    line_net_amount,
    disc_tot_perc,
    disc_tot_amount,
    cost_price_FIFO,
    employee_code,
    employee_name,
    Date,
    party_name,
    item_total,
    item_disc_total,
    total_tax,
    net_amount,
    Timestamp,
    CASE WHEN row_num = 1 THEN Bill_amount ELSE NULL END AS Bill_amount,
    CASE WHEN row_num = 1 THEN Bill_no ELSE NULL END AS Bill_no,
    bill_paid_date,
    ssSL,
    nlCode,
    nlName,
    ASM_Name,
    nlaccountId,
    nlAdd1,
    nlAdd2,
    nlAdd3,
    nlPlace,
    nlPhone1,
    nlPhone2,
    nlState,
    nlTs,
    nlPinCode,
    nlVertical,
    nlArea,
    nlID,
    nlShortName,
    Store_Id,
    pmCode,
    pmForm,
    pmMRP,
    pmLSaleRate,
    pmTs,
    pmShortName,
    pmSize,
    pmBarCode,
    pmID
FROM
    BillData;

-- Step 2: Drop the Original Table
DROP TABLE IF EXISTS kiosk.kiosk_final;

-- Step 3: Create the Original Table from Buffer
CREATE TABLE kiosk.kiosk_final AS
SELECT * FROM kiosk.kiosk_final_buffer;

-- Step 4: Drop the Buffer Table (Optional, if buffer no longer needed)
DROP TABLE IF EXISTS kiosk.kiosk_final_buffer;

-- Step 5: Alter the original table to add the store_region column
ALTER TABLE kiosk.kiosk_final
    ADD COLUMN store_region VARCHAR(50);

-- Step 6: Update the store_region column based on state conditions
UPDATE kiosk.kiosk_final
    SET store_region = CASE
        WHEN nlState = 'Andaman and Nicobar Islands' THEN 'South India'
        WHEN nlState = 'Andhra Pradesh' THEN 'South India'
        WHEN nlState = 'Arunachal Pradesh' THEN 'North-East India'
        WHEN nlState = 'Assam' THEN 'North-East India'
        WHEN nlState = 'Bihar' THEN 'East India'
        WHEN nlState = 'Chandigarh' THEN 'North India'
        WHEN nlState = 'Chhattisgarh' THEN 'Central India'
        WHEN nlState = 'Dadra and Nagar Haveli' THEN 'West India'
        WHEN nlState = 'Daman and Diu' THEN 'West India'
        WHEN nlState = 'Delhi' THEN 'North India'
        WHEN nlState = 'Goa' THEN 'West India'
        WHEN nlState = 'Gujarat' THEN 'West India'
        WHEN nlState = 'Haryana' THEN 'North India'
        WHEN nlState = 'Himachal Pradesh' THEN 'North India'
        WHEN nlState = 'Jammu and Kashmir' THEN 'North India'
        WHEN nlState = 'Jharkhand' THEN 'East India'
        WHEN nlState = 'Karnataka' THEN 'South India'
        WHEN nlState = 'Kerala' THEN 'South India'
        WHEN nlState = 'Ladakh' THEN 'North India'
        WHEN nlState = 'Lakshadweep' THEN 'South India'
        WHEN nlState = 'Madhya Pradesh' THEN 'Central India'
        WHEN nlState = 'Maharashtra' THEN 'West India'
        WHEN nlState = 'Manipur' THEN 'North-East India'
        WHEN nlState = 'Meghalaya' THEN 'North-East India'
        WHEN nlState = 'Mizoram' THEN 'North-East India'
        WHEN nlState = 'Nagaland' THEN 'North-East India'
        WHEN nlState = 'Odisha' THEN 'East India'
        WHEN nlState = 'Puducherry' THEN 'South India'
        WHEN nlState = 'Punjab' THEN 'North India'
        WHEN nlState = 'Rajasthan' THEN 'West India'
        WHEN nlState = 'Sikkim' THEN 'North-East India'
        WHEN nlState = 'Tamil Nadu' OR nlState = 'Tamilnadu' THEN 'South India'
        WHEN nlState = 'Telangana' THEN 'South India'
        WHEN nlState = 'Tripura' THEN 'North-East India'
        WHEN nlState = 'Uttar Pradesh' THEN 'North India'
        WHEN nlState = 'Uttarakhand' THEN 'North India'
        WHEN nlState = 'West Bengal' THEN 'East India'
        ELSE 'Unknown'
    END;
"""

# Execute each command in the SQL script
for statement in sql_script.strip().split(";"):
    if statement.strip():  # Skip any empty statements
        print("start")
        cursor.execute(statement)
        conn.commit()  # Commit after each statement
        print("complete")
# Close the cursor and the connection
cursor.close()
conn.close()

print("Table update and refresh completed successfully.")
