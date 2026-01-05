DECLARE DAYS_FROM INT64;
DECLARE DAYS_TO INT64;
DECLARE DATE_FROM STRING;
DECLARE DATE_TO STRING;

SET DAYS_FROM = 3;
SET DAYS_TO = 1;
SET DATE_FROM = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL DAYS_FROM DAY));
SET DATE_TO = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL DAYS_TO DAY));

-- DROP TABLE IF EXISTS integration_layer.campaign_conversion;

CREATE TABLE IF NOT EXISTS integration_layer.campaign_conversion
	(
		id_date STRING,
		channel STRING,
		campaign STRING,
		impressions INT64,
		clicks INT64,
		cost FLOAT64,
		sessions INT64,
		pre_checkouts INT64,
		purchases INT64,
		revenue FLOAT64
	)
PARTITION BY
  _PARTITIONDATE
CLUSTER BY
	channel
;

DELETE FROM integration_layer.campaign_conversion
WHERE id_date >= DATE_FROM

;
CREATE TEMP TABLE ga AS
	(
		SELECT
			f.id_date,
			f.utm_hash,
			CASE
				WHEN u.channel IN ('meta-paid', 'google-paid') THEN u.channel
				WHEN f.referral_code <> '' THEN 'affiliates'
				WHEN f.promo <> '' THEN 'rewards'
				WHEN f.discount IN (
					'1744124823370x697250709357723600'
					'1744132488476x405811884189024260'
					'1744124896344x704062133947859000'
					'1744124622994x346120195514564600'
					'1752442102593x921578297885720600'
					'1752442085700x158446776052613120'
					'1752442094156x761718273202192400'
					'1752442077134x649694427368390700'
					'1752506493408x270962718209736700'
					'1752506486667x169547983502442500'
				) THEN 'rewards'
				WHEN f.discount <> '' THEN 'internal-sellers'
				ELSE u.channel
			END AS channel,
			COUNT(DISTINCT f.session_hash) AS sessions
		FROM
			integration_layer.historical_funnel f
			LEFT JOIN business_layer.dim_utm u
				USING(utm_hash)
		WHERE
			f.id_date >= DATE_FROM
		GROUP BY
			1, 2, 3
	)
;
CREATE TEMP TABLE precheckout AS
	(
		SELECT
			s.id_date,
			s.utm_hash,
			CASE
				WHEN u.channel IN ('meta-paid', 'google-paid') THEN u.channel
				WHEN s.referral_code <> '' THEN 'affiliates'
				WHEN s.promo <> '' THEN 'rewards'
				ELSE u.channel
			END AS channel,
			COUNT(DISTINCT s.email) AS pre_checkouts
		FROM
			integration_layer.supabase_precheckout s
			LEFT JOIN business_layer.dim_utm u
				USING(utm_hash)
		WHERE
			id_date >= DATE_FROM
		GROUP BY
			1, 2, 3
	)
;
CREATE TEMP TABLE transactions_data AS
	(
		SELECT
			t.id_date,
			t.utm_hash,
			CASE
				WHEN u.channel IN ('meta-paid', 'google-paid') THEN u.channel
				WHEN t.seller_email IS NOT NULL THEN 'internal-sellers'
				WHEN t.affiliate_id <> '' THEN 'affiliates'
				WHEN t.discount IN (
					'1744124823370x697250709357723600'
					'1744132488476x405811884189024260'
					'1744124896344x704062133947859000'
					'1744124622994x346120195514564600'
					'1752442102593x921578297885720600'
					'1752442085700x158446776052613120'
					'1752442094156x761718273202192400'
					'1752442077134x649694427368390700'
					'1752506493408x270962718209736700'
					'1752506486667x169547983502442500'
				) THEN 'rewards'
				ELSE u.channel
			END AS channel,
			COUNT(DISTINCT t.id) AS purchases,
			SUM(t.amount) AS revenue
		FROM
			integration_layer.transactions_data t
			LEFT JOIN business_layer.dim_utm u
				USING(utm_hash)
		WHERE
			t.id_date >= DATE_FROM
		GROUP BY
			1, 2, 3
	)
;
CREATE TEMP TABLE gads_cost AS
	(
		SELECT
			FORMAT_DATE('%Y%m%d', segments_date) AS id_date,
			'google-paid' AS channel,
			CAST(campaign_id AS STRING) AS campaign_id,
			SUM(metrics_cost_micros) / 1000000 AS cost,
			SUM(metrics_impressions) AS impressions,
			SUM(metrics_clicks) AS clicks
		FROM
			ads_3308845392.p_ads_CampaignStats_3308845392
		WHERE
			segments_date >= PARSE_DATE('%Y%m%d', DATE_FROM)
		GROUP BY
			1, 2, 3
	)
;
CREATE TEMP TABLE meta_cost AS
	(
		SELECT
			FORMAT_DATE('%Y%m%d', datestart) AS id_date,
			'meta-paid' AS channel,
			CampaignId,
			CampaignName,
			AdSetId,
			AdSetName,
			AdId,
			AdName,
			COALESCE(
				CAST(campaignid AS STRING),
				CAST(adsetid AS STRING),
				CAST(adid AS STRING)
			) AS campaign_hash,
			CONCAT(
				campaignname,
				adsetname,
				adname
			) AS campaign_hash1,
			SUM(Impressions) AS impressions,
			SUM(LinkClicks) AS clicks,
			SUM(Spend) AS cost
		FROM
			meta_ads.AdInsights
		WHERE
			DATE_TRUNC(_PARTITIONDATE, DAY) >= PARSE_DATE('%Y%m%d', DATE_FROM)
			AND (Spend > 0
				OR LinkClicks > 0)
		GROUP BY
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	)
;
CREATE TEMP TABLE aux AS
	(
		SELECT
			COALESCE(g.id_date, t.id_date, p.id_date) AS id_date,
			COALESCE(g.channel, t.channel, p.channel) AS channel,
			u.campaign,
			CASE
				WHEN u.source || '/' || u.medium = 'google/cpc' THEN '(not set)'
				ELSE u.term
			END AS term,
			CASE
				WHEN u.source || '/' || u.medium = 'google/cpc' THEN '(not set)'
				ELSE u.content
			END AS content,
			COALESCE(SUM(g.sessions), 0) AS sessions,
			COALESCE(SUM(p.pre_checkouts), 0) AS pre_checkouts,
			COALESCE(SUM(t.purchases), 0) AS purchases,
			COALESCE(SUM(t.revenue), 0) AS revenue
		FROM
			ga g
			FULL JOIN transactions_data t
				USING(id_date, utm_hash)
			FULL JOIN precheckout p
				USING(id_date, utm_hash)
			LEFT JOIN business_layer.dim_utm u
				ON COALESCE(g.utm_hash, t.utm_hash, p.utm_hash) = u.utm_hash
		GROUP BY
			1, 2, 3, 4, 5
	)
;
CREATE TEMP TABLE base AS
	(
		SELECT
			COALESCE(a.id_date, mc.id_date, gc.id_date) AS id_date,
			COALESCE(a.channel, mc.channel, gc.channel) AS channel,
			COALESCE(a.campaign, mc.CampaignId, gc.campaign_id) AS campaign,
			COALESCE(SUM(mc.impressions), SUM(gc.impressions), 0) AS impressions,
			COALESCE(SUM(mc.clicks), SUM(gc.clicks), 0) AS clicks,
			COALESCE(SUM(mc.cost), SUM(gc.cost), 0) AS cost,
			SUM(a.sessions) AS sessions,
			SUM(a.pre_checkouts) AS pre_checkouts,
			SUM(a.purchases) AS purchases,
			SUM(a.revenue) AS revenue
		FROM
			aux a
			FULL JOIN meta_cost mc
				ON mc.id_date = a.id_date
				AND mc.channel = a.channel
				AND (mc.campaign_hash = CONCAT(a.campaign, a.term, a.content)
					OR mc.campaign_hash = CONCAT(a.campaign, a.content, a.term)
					OR mc.campaign_hash1 = CONCAT(a.campaign, a.term, a.content)
					OR mc.campaign_hash1 = CONCAT(a.campaign, a.content, a.term))
			FULL JOIN gads_cost gc
				ON gc.id_date = a.id_date
				AND gc.channel = a.channel
				AND gc.campaign_id = a.campaign
		GROUP BY
			1, 2, 3
	)
;
INSERT INTO integration_layer.campaign_conversion
	(
		id_date,
		channel,
		campaign,
		impressions,
		clicks,
		cost,
		sessions,
		pre_checkouts,
		purchases,
		revenue
	)
	SELECT
		id_date,
		channel,
		campaign,
		CAST(impressions AS INT64),
		CAST(clicks AS INT64),
		cost,
		sessions,
		pre_checkouts,
		purchases,
		revenue
	FROM
		base
;