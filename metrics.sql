WITH
user_first_payments AS (
    SELECT
        user_id,
        MIN(payment_date) AS first_payment_date
    FROM project.games_payments
    GROUP BY user_id
),

distinct_months AS (
    SELECT DISTINCT DATE_TRUNC('month', payment_date) AS month
    FROM project.games_payments
    ORDER BY DATE_TRUNC('month', payment_date)
),

user_monthly_payments AS (
    SELECT
        user_id,
        DATE_TRUNC('month', payment_date) AS month,
        SUM(revenue_amount_usd) AS monthly_amount
    FROM project.games_payments
    GROUP BY user_id, DATE_TRUNC('month', payment_date)
),

user_activity AS (
    SELECT
        m.month,
        u.user_id,
        ump.monthly_amount AS current_amount,
        LAG(ump.monthly_amount) OVER (PARTITION BY u.user_id ORDER BY m.month) AS prev_amount,
        LEAD(ump.monthly_amount) OVER (PARTITION BY u.user_id ORDER BY m.month) AS next_amount,
        fp.first_payment_date
    FROM distinct_months m
    CROSS JOIN (SELECT DISTINCT user_id FROM project.games_payments) u
    LEFT JOIN user_monthly_payments ump ON m.month = ump.month AND u.user_id = ump.user_id
    LEFT JOIN user_first_payments fp ON u.user_id = fp.user_id
),

monthly_metrics AS (
    SELECT
        month,
        user_id,
        SUM(COALESCE(current_amount, 0)) AS mrr,

        SUM(CASE WHEN DATE_TRUNC('month', first_payment_date) = month
                 THEN COALESCE(current_amount, 0) ELSE 0 END) AS new_mrr,

        SUM(CASE WHEN prev_amount IS NOT NULL AND current_amount > prev_amount
                 THEN current_amount - prev_amount ELSE 0 END) AS expansion_mrr,

        SUM(CASE WHEN prev_amount IS NOT NULL AND current_amount < prev_amount
                 THEN prev_amount - current_amount ELSE 0 END) AS contraction_mrr,

        SUM(CASE WHEN current_amount IS NOT NULL AND next_amount IS NULL
                 THEN current_amount ELSE 0 END) AS churn_mrr,

        SUM(CASE WHEN prev_amount IS NULL AND current_amount IS NOT NULL
                 AND DATE_TRUNC('month', first_payment_date) < month - INTERVAL '1 month'
                 THEN current_amount ELSE 0 END) AS back_from_churn_mrr,

        COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', first_payment_date) = month
                            THEN user_id END) AS new_users_count,

        COUNT(DISTINCT CASE WHEN current_amount IS NOT NULL
                            THEN user_id END) AS users_count,

        COUNT(DISTINCT CASE WHEN current_amount IS NOT NULL AND next_amount IS NULL
                            THEN user_id END) AS churned_users_count
    FROM user_activity
    GROUP BY month, user_id
)

SELECT
    TO_CHAR(month, 'YYYY-MM') AS month,
    m.user_id,
    gpu.age,
    gpu.language,
    ROUND(COALESCE(mrr, 0), 2) AS mrr,
    ROUND(COALESCE(mrr / NULLIF(users_count, 0), 0), 2) AS arppu,
    ROUND(COALESCE(new_mrr, 0), 2) AS new_mrr,
    ROUND(COALESCE(expansion_mrr, 0), 2) AS expansion_mrr,
    ROUND(COALESCE(contraction_mrr, 0), 2) AS contraction_mrr,
    ROUND(COALESCE(churn_mrr, 0), 2) AS churn_mrr,
    ROUND(COALESCE(back_from_churn_mrr, 0), 2) AS back_from_churn_mrr,
    COALESCE(new_users_count, 0) AS new_users_count,
    COALESCE(users_count, 0) AS users_count,
    COALESCE(churned_users_count, 0) AS churned_users_count
FROM monthly_metrics as m
left join project.games_paid_users gpu on m.user_id = gpu.user_id
WHERE month IS NOT NULL
ORDER BY month;