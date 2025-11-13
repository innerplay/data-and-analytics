CREATE TABLE IF NOT EXISTS business_layer.dim_utm
	(
		utm_hash STRING,
		channel STRING,
		source STRING,
		medium STRING,
		campaign STRING,
		term STRING,
		content STRING
	)
CLUSTER BY
	utm_hash
