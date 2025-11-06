DECLARE DAYS_FROM INT64;
DECLARE DAYS_TO INT64;
DECLARE DATE_FROM DATE;
DECLARE DATE_TO DATE;

SET DAYS_FROM = 2;
SET DAYS_TO = 0;
SET DATE_FROM = DATE_SUB(CURRENT_DATE(), INTERVAL DAYS_FROM DAY);
SET DATE_TO = DATE_SUB(CURRENT_DATE(), INTERVAL DAYS_TO DAY);

-- DROP TABLE IF EXISTS integration_layer.supabase_precheckout;

CREATE TABLE IF NOT EXISTS integration_layer.supabase_precheckout
	(
		id STRING,
		id_date STRING,
		created_at TIMESTAMP,
		utm_hash STRING,
		email STRING,
		phone STRING,
		name STRING,
		selected_plan STRING,
		checkout_system STRING,
		promo STRING,
		referral_code STRING,
		user_agent STRING,
		path STRING
	)
PARTITION BY
	DATE(created_at)
CLUSTER BY
	id
;
CREATE TEMP TABLE supabase_precheckout AS
	(
		SELECT
			id,
			FORMAT_DATE('%Y%m%d', DATE(created_at)) AS id_date,
			TIMESTAMP(created_at) AS created_at,
			TO_HEX(MD5(CONCAT(
				COALESCE(utm_source, '(not set)'),
				COALESCE(utm_medium, '(not set)'),
				COALESCE(utm_campaign, '(not set)'),
				COALESCE(utm_term, '(not set)'),
				COALESCE(utm_content, '(not set)')
			))) AS utm_hash,
			email,
			CAST(phone AS STRING) AS phone,
			name,
			selected_plan,
			checkout_system,
			promo,
			referral_code,
			user_agent,
			referrer,
			path
		FROM
			raw_data.supabase_precheckout
		WHERE
			DATE(created_at) BETWEEN DATE_FROM AND DATE_TO
			AND email NOT LIKE '%@innerai.com%'
			AND utm_source NOT LIKE '%test%'
			AND utm_medium NOT LIKE '%test%'
			AND utm_campaign NOT LIKE '%test%'
	)
;
INSERT INTO integration_layer.supabase_precheckout
	(
		SELECT
			t.id,
			t.id_date,
			t.created_at,
			t.utm_hash,
			t.email,
			t.phone,
			t.name,
			t.selected_plan,
			t.checkout_system,
			t.promo,
			t.referral_code,
			t.user_agent,
			t.path
		FROM
			supabase_precheckout t
			LEFT JOIN integration_layer.supabase_precheckout s
				USING(id)
		WHERE
			s.id IS NULL
	)
;