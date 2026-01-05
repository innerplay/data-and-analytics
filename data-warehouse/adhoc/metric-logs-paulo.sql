CREATE TEMP TABLE token_data AS
	SELECT
		d.date,
		m.user_email,
		CASE
			WHEN LOWER(m.user_plan) LIKE '%guest mode%' THEN 'guest'
			WHEN m.user_plan IN ('fair use unlimited', 'trialing', 'free') THEN 'free'
			WHEN m.user_plan = 'pro subscription' THEN 'pro'
			WHEN m.user_plan = 'plus subscription' THEN 'plus'
			WHEN m.user_plan = 'lite subscription' THEN 'lite'
			WHEN m.user_plan = 'ultra subscription' THEN 'ultra'
			ELSE 'b2b'
		END AS plan,
		CASE
			WHEN m.created_at < '2025-10-29' AND m.source != 'new_chat_api_v2' AND m.ai_model IN ('gpt-5-mini', 'gpt-5-nano', 'gpt-5-chat-latest') THEN 'gpt-4.1'
			ELSE m.ai_model
		END AS ai_model,
		m.request_cost AS request_cost,
		CASE
			WHEN m.prompt_tokens > 200000 AND LOWER(m.ai_model) LIKE '%claude%' THEN 200000
			ELSE m.prompt_tokens
		END AS input_tokens,
		m.completion_tokens AS output_tokens,
		m.id
	FROM
		integration_layer.metric_logs m
		INNER JOIN business_layer.dim_date d
			USING(id_date)
	WHERE
		d.year_month = '202510'
		AND m.prompt_tokens > 0
		AND m.item_type = 'message'
		AND ai_model NOT IN (' - ', 'gpt-image-1')
		AND LOWER(user_email) NOT LIKE '%@innerai%'
	GROUP BY
		1, 2, 3, 4, 5, 6, 7, 8
;
SELECT
	date,
	user_email,
	-- CASE
	-- 	WHEN ai_model IN (
	-- 	'claude-3.5-haiku', 'haiku', 'gpt-4o-mini', 'llama-3.3', 'Llama 3.1', 'Llama 3.1 ', 'deepseek-3', 'deepseek', 'claude-3-5-haiku-latest',
	-- 	'llama-3', 'gemini-2', 'openai/gpt-4o-mini', 'flux-images', 'flux-pro', 'flux', 'flux-ultra-image', 'gemini-2.5-flash-preview-04-17',
	-- 	'gemini-2.5-flash', 'gpt-4.1-nano', 'llama-4-scout', 'gaia', 'llama', 'gpt-oss-120b', 'gpt-5-mini', 'command-r', 'gemini-fal-ai', 'flux-kontext-pro', 'claude-4.5-haiku'
	-- 	) THEN 'Fast'
	-- 	WHEN ai_model IN (
	-- 	'bedrock', 'claude-3.5', 'Claude 3.5', 'Claude 3.5 ', 'claude-3.7-non-thinking', 'gpt-4o', 'GPT-4o', 'inner-ai-assistant', 'azure', 'mistral',
	-- 	'perplexity', 'Sonar Web', 'grok', 'gemini', 'sabia', 'sabia-3', 'gemini-2-pro', 'Gemini 1.5 Pro', 'Gemini', 'chatgpt-4o-latest', 'gemini-2.5-pro',
	-- 	'gpt-4.1', 'grok-3', 'llama-4-maverick', 'sabia-3.1', 'claude-4-non-thinking', 'amazon-nova-premier', 'gpt-5', 'gpt-5-chat', 'gpt-5-chat-latest', 'kimi-k2', 'claude-4.5', 'grok-4-fast-non-reasoning'
	-- 	) THEN 'Advanced'
	-- 	WHEN ai_model IN (
	-- 	'deepseek-r1-small', 'o3-mini', 'gpt-o1', 'o1 Mini', 'deepseek-r1', 'claude-3.7', 'qwen-qwq-32b', 'qwen/qwen3-32b', 'grok-3-mini', 'o4-mini', 'claude-4', 'o3', 'grok-4', 'claude-4.5-non-thinking'
	-- 	) THEN 'Deep'
	-- 	ELSE 'Other'
	-- END AS model_type,
	ai_model,
	COUNT(DISTINCT id) AS messages_sent,
	SUM(input_tokens)  AS total_input_tokens,
	SUM(output_tokens) AS total_output_tokens,
	-- Sum of cost across all model_names that map to this same model_type, with special GPT-4o pricing before April 15, 2025:
	SUM(
		CASE 
			-- Apply special pricing for GPT-4o and GPT-4o before April 15, 2025
			WHEN (ai_model IN ('gpt-4o', 'GPT-4o') AND date < '2025-04-15') THEN input_tokens * (SELECT input_cost_unit FROM business_layer.dim_model_cost WHERE model_name = 'chatgpt-4o-latest')
			ELSE input_tokens * cm.input_cost_unit
		END) AS total_input_cost,
	SUM(
		CASE 
			-- Apply special pricing for GPT-4o and GPT-4o before April 15, 2025
			WHEN (td.ai_model IN ('gpt-4o', 'GPT-4o') AND date < '2025-04-15') THEN 
				td.output_tokens * (SELECT output_cost_unit FROM business_layer.dim_model_cost WHERE model_name = 'chatgpt-4o-latest')
			ELSE 
				td.output_tokens * cm.output_cost_unit
		END) AS total_output_cost,
	SUM(
		CASE 
			-- Apply special pricing for GPT-4o and GPT-4o before April 15, 2025
			WHEN (td.ai_model IN ('gpt-4o', 'GPT-4o') AND td.date < '2025-04-15') THEN 
				(td.input_tokens * (SELECT input_cost_unit FROM business_layer.dim_model_cost WHERE model_name = 'chatgpt-4o-latest')) +
				(td.output_tokens * (SELECT output_cost_unit FROM business_layer.dim_model_cost WHERE model_name = 'chatgpt-4o-latest'))
			ELSE 
				(td.input_tokens * cm.input_cost_unit) +
				(td.output_tokens * cm.output_cost_unit)
		END
	) AS grand_total_cost
FROM token_data td
LEFT JOIN business_layer.dim_model_cost cm
	ON td.ai_model = cm.model_name
GROUP BY
	1, 2, 3
;


-- #3 DEPOIS RODA ESSA PRA VER SE FALTA MAPEAR MODELO
-- select distinct model_name from chat_usage where model_type = 'Other'