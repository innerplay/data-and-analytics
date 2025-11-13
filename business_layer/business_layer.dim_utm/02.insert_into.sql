DECLARE DATE_FROM STRING;
DECLARE DATE_TO STRING;
DECLARE DAYS_FROM INT64;
DECLARE DAYS_TO INT64;

SET DAYS_FROM = 2;
SET DAYS_TO = 0;
SET DATE_FROM = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL DAYS_FROM DAY));
SET DATE_TO = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL DAYS_TO DAY));

CREATE TEMP TABLE dim_utm AS
	(
		SELECT
			TO_HEX(MD5(CONCAT(source, medium, campaign, term, content))) AS utm_hash,
			CASE
				WHEN source = '(not set)' AND medium = '(not set)' AND campaign = '(not set)' AND term = '(not set)' AND content = '(not set)' THEN 'direct'
				WHEN source = 'google' AND medium = 'cpc' THEN 'google-paid'
				WHEN source IN ('meta', 'meta-ads', 'ig', 'facebook') THEN 'meta-paid'
				WHEN source IN ('taboola', 'blue') THEN 'remarketing'
				WHEN source IN ('youtube', 'yt', 'youtube-organic') THEN 'partners'
				WHEN medium = 'email' THEN 'email'
				WHEN medium = 'organic' THEN 'organic'
				WHEN medium = 'referral' THEN 'referral'
				WHEN medium = 'social' THEN 'social'
				ELSE 'not mapped'
			END AS channel,
			source,
			medium,
			campaign,
			term,
			content
		FROM
			(
				SELECT
					COALESCE(session_traffic_source_last_click.manual_campaign.source, '(not set)') AS source,
					COALESCE(session_traffic_source_last_click.manual_campaign.medium, '(not set)') AS medium,
					COALESCE(session_traffic_source_last_click.manual_campaign.campaign_name, '(not set)') AS campaign,
					COALESCE(session_traffic_source_last_click.manual_campaign.term, '(not set)') AS term,
					COALESCE(session_traffic_source_last_click.manual_campaign.content, '(not set)') AS content
				FROM
					`analytics-473217.analytics_300161196.events_*`
				WHERE
					_TABLE_SUFFIX BETWEEN DATE_FROM AND DATE_TO
					AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%innerai.com%'
				GROUP BY
					1, 2, 3, 4, 5
				UNION ALL
				SELECT
					COALESCE(utm_source, '(not set)') AS source,
					COALESCE(utm_medium, '(not set)') AS medium,
					COALESCE(utm_campaign, '(not set)') AS campaign,
					COALESCE(utm_term, '(not set)') AS term,
					COALESCE(utm_content, '(not set)') AS content
				FROM
					`analytics-473217.raw_data.supabase_precheckout`
				WHERE
					FORMAT_DATE('%Y%m%d', DATE(created_at)) BETWEEN DATE_FROM AND DATE_TO
					AND email NOT LIKE '%@innerai.com%'
				GROUP BY
					1, 2, 3, 4, 5
				UNION ALL
				SELECT
					COALESCE(utm_source, '(not set)') AS source,
					COALESCE(utm_medium, '(not set)') AS medium,
					COALESCE(utm_campaign, '(not set)') AS campaign,
					COALESCE(utm_term, '(not set)') AS term,
					COALESCE(utm_content, '(not set)') AS content
				FROM
					`analytics-473217.raw_data.transactions_data`
				WHERE
					FORMAT_DATE('%Y%m%d', DATE(timestamp)) BETWEEN DATE_FROM AND DATE_TO
				GROUP BY
					1, 2, 3, 4, 5
			)
		GROUP BY
			1, 2, 3, 4, 5, 6, 7
	)
;
INSERT INTO `analytics-473217.business_layer.dim_utm`
	(
		SELECT
			t.utm_hash,
			t.channel,
			t.source,
			t.medium,
			t.campaign,
			t.term,
			t.content
		FROM
			dim_utm t
			LEFT JOIN `analytics-473217.business_layer.dim_utm` du
				ON t.utm_hash = du.utm_hash
		WHERE
			du.utm_hash IS NULL
		GROUP BY
			1, 2, 3, 4, 5, 6, 7
	)
;
UPDATE `analytics-473217.business_layer.dim_utm` du
SET
	du.channel = t.channel
FROM
	dim_utm t
WHERE
	du.utm_hash = t.utm_hash
	AND du.channel <> t.channel
;
