DECLARE DAYS_FROM INT64;
DECLARE DAYS_TO INT64;
DECLARE DATE_FROM STRING;
DECLARE DATE_TO STRING;

SET DAYS_FROM = 3;
SET DAYS_TO = 1;
SET DATE_FROM = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL DAYS_FROM DAY));
SET DATE_TO = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL DAYS_TO DAY));

DROP TABLE IF EXISTS integration_layer.historical_funnel;

CREATE TABLE IF NOT EXISTS integration_layer.historical_funnel
	(
		unique_event_id STRING,
		id_date STRING,
		event_timestamp TIMESTAMP,
		event_name STRING,
		user_id STRING,
		session_hash STRING,
		promo STRING,
		discount STRING,
		referral_code STRING,
		landing_page STRING,
		checkout_page STRING,
		utm_hash STRING,
		transaction_id STRING,
		purchase_revenue FLOAT64
	)
PARTITION BY
	DATE(event_timestamp)
CLUSTER BY
	unique_event_id
;

CREATE TEMP TABLE base AS
	(
		SELECT
			FORMAT_DATE('%Y-%m-%d', TIMESTAMP(DATETIME(TIMESTAMP_MICROS(event_timestamp), 'UTC'), 'America/Sao_Paulo')) AS id_date,
			TIMESTAMP(DATETIME(TIMESTAMP_MICROS(event_timestamp), 'UTC'), 'America/Sao_Paulo') AS event_timestamp,
			CASE
				WHEN event_name = 'begin_checkout' THEN 'add_payment_info'
				ELSE event_name
			END AS event_name,
			user_pseudo_id,
			user_id,
			CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) AS session_id,
			TO_HEX(SHA256(CONCAT(
				user_pseudo_id,
				CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
			))) AS session_hash,
			REGEXP_REPLACE(SPLIT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), '?')[0], '(http[s]?://|www\\.)', '') AS page_location,
			SPLIT(REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), "promo=[A-Za-z0-9]+"), 'promo=')[1] AS promo,
			SPLIT(REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), "discount=[A-Za-z0-9]+"), 'discount=')[1] AS discount,
			SPLIT(REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), "referral_code=[A-Za-z0-9]+"), 'referral_code=')[1] AS referral_code,
			TO_HEX(MD5(CONCAT(
				COALESCE(session_traffic_source_last_click.manual_campaign.source, '(not set)'),
				COALESCE(session_traffic_source_last_click.manual_campaign.medium, '(not set)'),
				COALESCE(session_traffic_source_last_click.manual_campaign.campaign_name, '(not set)'),
				COALESCE(session_traffic_source_last_click.manual_campaign.term, '(not set)'),
				COALESCE(session_traffic_source_last_click.manual_campaign.content, '(not set)')
			 ))) AS utm_hash,
			ecommerce.transaction_id,
			ecommerce.purchase_revenue
		FROM
			`analytics-473217.analytics_300161196.events_*`
		WHERE
			_TABLE_SUFFIX BETWEEN DATE_FROM AND DATE_TO
			AND REGEXP_CONTAINS((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'page_location'), '.*(staging.*innerai|pay/test).*') IS FALSE
			AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%innerai.com%'
			AND event_name IN ('first_visit', 'page_view', 'session_start', 'login', 'pre_checkout_loaded', 'pre_checkout_submitted', 'add_to_cart', 'sign_up', 'add_payment_info', 'purchase', 'begin_checkout')
		GROUP BY
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
	)
;
CREATE TEMP TABLE aux AS
	(
		SELECT
			TO_HEX(SHA256(CONCAT(
				user_pseudo_id,
				CAST(event_timestamp AS STRING),
				event_name,
				session_id
			))) AS unique_event_id,
			id_date,
			event_timestamp,
			event_name,
			user_id,
			session_hash,
			utm_hash,
			transaction_id,
			purchase_revenue,
			LAST_VALUE(promo IGNORE NULLS) OVER (PARTITION BY session_hash ORDER BY event_timestamp) AS promo,
			LAST_VALUE(discount IGNORE NULLS) OVER (PARTITION BY session_hash ORDER BY event_timestamp) AS discount,
			LAST_VALUE(referral_code IGNORE NULLS) OVER (PARTITION BY session_hash ORDER BY event_timestamp) AS referral_code,
			MAX(
				CASE
					WHEN page_location LIKE '%v_.innerai.com%' THEN 0
					WHEN page_location LIKE '%app.innerai.com%' THEN 0
					WHEN page_location LIKE '%platform.innerai.com%' THEN 0
					WHEN page_location LIKE '%docs.innerai.com%' THEN 0
					WHEN page_location LIKE '%forms.innerai.com%' THEN 0
					WHEN page_location LIKE '%innerai.com/pt%' THEN 
						CASE
							WHEN page_location LIKE '%/pt/' THEN 0
							WHEN page_location LIKE '%/pt' THEN 0
							ELSE 1
						END
					WHEN page_location LIKE '%innerai.com/en%' THEN 
						CASE
							WHEN page_location LIKE '%/en/' THEN 0
							WHEN page_location LIKE '%/en' THEN 0
							ELSE 1
						END
					WHEN page_location LIKE '%innerai.com/es%' THEN 
						CASE
							WHEN page_location LIKE '%/es/' THEN 0
							WHEN page_location LIKE '%/es' THEN 0
							ELSE 1
						END
					WHEN page_location LIKE '%innerai.com%login' THEN 0
					WHEN page_location LIKE '%innerai.com%/legal%' THEN 0
					ELSE 1
				END
			) OVER (PARTITION BY session_hash) AS user_flag_session,
			FIRST_VALUE(
				CASE
					WHEN page_location LIKE '%checkout.innerai.com%' THEN 'checkout.innerai.com/'
					WHEN page_location LIKE '%buy.innerai.com%' THEN 'buy.innerai.com/'
					WHEN page_location LIKE '%app.innerai.com%' THEN 'app.innerai.com/'
					WHEN page_location LIKE '%v_.innerai.com%' THEN 'app.innerai.com/'
					WHEN page_location LIKE 'innerai.com%pt' THEN 'innerai.com/'
					WHEN page_location LIKE 'innerai.com%pt/' THEN 'innerai.com/'
					WHEN page_location LIKE 'innerai.com%es' THEN 'innerai.com/'
					WHEN page_location LIKE 'innerai.com%es/' THEN 'innerai.com/'
					WHEN page_location LIKE 'innerai.com%en' THEN 'innerai.com/'
					WHEN page_location LIKE 'innerai.com%en/' THEN 'innerai.com/'
					WHEN page_location IN ('innerai.com/') THEN 'innerai.com/'
					WHEN page_location LIKE '%platform.innerai.com%' THEN 'platform.innerai.com/'
					ELSE page_location
				END) OVER (PARTITION BY session_hash ORDER BY event_timestamp) AS landing_page,
			LAST_VALUE(CASE WHEN page_location LIKE 'checkout%' OR page_location LIKE 'buy%' THEN SPLIT(page_location, '/')[0] END IGNORE NULLS) OVER (PARTITION BY session_hash ORDER BY event_timestamp) AS checkout_page
		FROM
			base
	)
;
CREATE TEMP TABLE historical_funnel AS
	(
		SELECT
			unique_event_id,
			id_date,
			event_timestamp,
			event_name,
			user_id,
			session_hash,
			promo,
			discount,
			referral_code,
			CASE
				WHEN user_flag_session = 0 THEN 'only app or platform'
				ELSE landing_page
			END AS landing_page,
			checkout_page,
			utm_hash,
			transaction_id,
			purchase_revenue
		FROM
			aux
		GROUP BY
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
	)
;
INSERT INTO integration_layer.historical_funnel
	(
		SELECT
			a.unique_event_id,
			a.id_date,
			a.event_timestamp,
			a.event_name,
			a.user_id,
			a.session_hash,
			a.promo,
			a.discount,
			a.referral_code,
			a.landing_page,
			a.checkout_page,
			a.utm_hash,
			a.transaction_id,
			a.purchase_revenue
		FROM
			historical_funnel a
			LEFT JOIN integration_layer.historical_funnel b
				USING(unique_event_id)
		WHERE
			b.unique_event_id IS NULL
	)
;