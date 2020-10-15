SELECT
    unicodigo as chave_1,
    regexp_replace(unicpfcnpj, '[\.|\-|\/]', '', 'gi')  as chave_2,
    unicodigo as id,
	uninomerazao as nome,
	regexp_replace(unicpfcnpj, '[\.|\-|\/]', '', 'gi') as cpfcnpj,
	CASE LENGTH(unicpfcnpj)
		WHEN 14 THEN 'FISICA'
		WHEN 18 THEN 'JURIDICA'
	END as tipo
FROM wun.tbunico
WHERE unisituacao = 1
AND unicpfcnpj NOT IN ('000.000.000-00', '00.000.000/0000-00')
LIMIT 15