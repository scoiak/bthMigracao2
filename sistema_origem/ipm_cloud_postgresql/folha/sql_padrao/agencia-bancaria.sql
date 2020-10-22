select * from(
	select '300' as sistema,
	'agencia-bancaria' as tipo_registro,
	 agencia.bcaagencia as id,
	 agencia.bcaagencia as chave_dsk1,
	 COALESCE(public.bth_get_id_gerado('300', 'bancos', CAST(banco.bcocodigo as text)),0) as idBanco,
	 banco.bcocodigo as cod_febraban_banco,
	 bcanome as nome,
	 bcaagencia as numeroAgencia,
	 bcadigito as digitoAgencia,
	 public.bth_get_id_gerado('300', 'cidades', cast (cidade.cidnome as text)) as cidade,
	 public.bth_get_situacao_registro('300', 'agencias-bancarias', CAST(agencia.bcaagencia as text)) as situacao_registro
	from wun.tbbancoagencia agencia
	inner join wun.tbbanco banco on banco.bcocodigo = agencia.bcocodigo
	inner join wun.tbcidade cidade on cidade.cidcodigo = agencia.cidcodigo
) tab
where situacao_registro in (0)