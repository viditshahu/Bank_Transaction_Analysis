-- Bank dataset
SELECT * FROM `project-a8fdf5df-1869-4c49-b9b.banking_project.bank_transactions` LIMIT 1000;

-- Create a clean preprocessed table
CREATE OR REPLACE TABLE `project-a8fdf5df-1869-4c49-b9b.banking_project.bank_transactions_cleaned` AS
WITH cleaned AS (
    SELECT
        transaction_id,
        customer_id,
        loan_id,
        transaction_date,
        -- Handle nulls: replace with defaults
        IFNULL(transaction_type, 'UNKNOWN') AS transaction_type,
        IFNULL(amount, 0) AS amount,
        IFNULL(interest_rate, 0) AS interest,
        IFNULL(account_balance, 0) AS balance,
        
        -- Standardize transaction type values
        CASE 
            WHEN LOWER(transaction_type) IN ('credit','cr') THEN 'CREDIT'
            WHEN LOWER(transaction_type) IN ('debit','dr') THEN 'DEBIT'
            ELSE 'UNKNOWN'
        END AS standardized_transaction_type,
        
        -- Remove duplicates using ROW_NUMBER
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id 
            ORDER BY transaction_date DESC
        ) AS rn
    FROM `project-a8fdf5df-1869-4c49-b9b.banking_project.bank_transactions`
)
SELECT
    transaction_id,
    customer_id,
    loan_id,
    transaction_date,
    standardized_transaction_type AS transaction_type,
    amount,
    interest,
    balance
FROM cleaned
WHERE rn = 1;  -- Keep only latest record per transaction_id

-- Clean data
SELECT * FROM `project-a8fdf5df-1869-4c49-b9b.banking_project.bank_transactions_cleaned` LIMIT 1000;

-- Net Cash Flow
SELECT
    loan_id,
    SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END) -
    SUM(CASE WHEN transaction_type = 'DEBIT' THEN amount ELSE 0 END) AS net_cash_flow
FROM `project-a8fdf5df-1869-4c49-b9b.banking_project.bank_transactions_cleaned`
GROUP BY loan_id;

-- Total Credit & Total Debit
SELECT
    loan_id,
    SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END) AS total_credit,
    SUM(CASE WHEN transaction_type = 'DEBIT' THEN amount ELSE 0 END) AS total_debit
FROM `project-a8fdf5df-1869-4c49-b9b.banking_project.bank_transactions_cleaned`
GROUP BY loan_id;

-- Credit–Debit Ratio
SELECT
    loan_id,
    SAFE_DIVIDE(
        SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END),
        SUM(CASE WHEN transaction_type = 'DEBIT' THEN amount ELSE 0 END)
    ) AS credit_debit_ratio
FROM `project-a8fdf5df-1869-4c49-b9b.banking_project.bank_transactions_cleaned`
GROUP BY loan_id;

-- Interest Earned
SELECT
    loan_id,
    SUM(interest) AS interest_earned
FROM `project-a8fdf5df-1869-4c49-b9b.banking_project.bank_transactions_cleaned`
GROUP BY loan_id;

-- Loan Recovery Rate
SELECT
    loan_id,
    SAFE_DIVIDE(
        SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END),
        SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END) + MAX(balance)
    ) AS loan_recovery_rate
FROM `project-a8fdf5df-1869-4c49-b9b.banking_project.bank_transactions_cleaned`
GROUP BY loan_id;

