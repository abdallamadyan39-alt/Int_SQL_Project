WITH customer_last_purchase AS (
	SELECT 
		ca.cohort_year ,
		ca.customerkey ,
		ca.cleaned_name ,
		ca.orderdate,
		ROW_NUMBER() OVER(PARTITION BY ca.customerkey ORDER BY ca.orderdate DESC ) AS rn ,
		ca.first_purchase_date
	FROM
		cohort_analysis ca
),
churned_customers AS (
	SELECT 
		cohort_year ,
		customerkey,
		cleaned_name ,
		orderdate AS last_purchase_date,
		CASE 
			WHEN orderdate < (
				SELECT
					MAX(orderdate)
				FROM
					sales
			) - INTERVAL '6 MONTH' THEN 'Churned'
			ELSE 'Active'
		END AS customer_status
	FROM
		customer_last_purchase
	WHERE
		rn = 1
		AND first_purchase_date < (
			SELECT
				MAX(orderdate)
			FROM
				sales
		) - INTERVAL '6 MONTH'
)
SELECT 
	cohort_year ,
	customer_status,
	COUNT(customerkey) AS num_customers,
	SUM(COUNT(customerkey)) OVER(PARTITION BY cohort_year ) AS total_customers,
	ROUND(COUNT(customerkey) / SUM(COUNT(customerkey)) OVER(PARTITION BY cohort_year ), 2) AS satus_percentage
FROM
	churned_customers
GROUP BY
	cohort_year ,
	customer_status
	  
	
	
	
	
	
	
	
	
	
	
	
	