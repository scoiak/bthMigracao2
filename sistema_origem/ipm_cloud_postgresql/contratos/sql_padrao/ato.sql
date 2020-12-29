select
	row_number() over() as id,
	'305' as sistema,
	'ato' as tipo_registro,
	*
from (
	select distinct
		ato.txjnumero as nro_ato,
		ato.txjano as ano_ato,
		(case cat.tctcodigo
			when 1 then  'DECRETO'
			when 2 then  'PORTARIA'
			when 3 then  'RESOLUCAO'
			when 4 then  'EDITAL'
			when 5 then  'LEI'
			when 6 then  'CONVENIO'
			when 7 then  'MEDIDA_PROVISORIA'
			when 8 then  'CONTRATO'
			when 9 then  'LEI_AUTORIZATIVA'
			when 10 then 'ESTATUTO_SOCIAL'
			when 11 then 'LEI_CRIACAO'
			when 12 then 'DESPACHO'
			when 13 then 'DESPACHO'
			when 14 then 'MEMORANDO'
			when 15 then 'DESPACHO'
			when 16 then 'PROCESSO'
			when 17 then 'PARECER'
			when 18 then 'ATO'
			when 19 then 'DECRETO_LEGISLATIVO'
			when 20 then 'LEI_COMPLEMENTAR'
			when 21 then 'LEI_ORGANICA'
			when 22 then 'EMENDA_MODIFICATIVA'
			when 23 then 'TERMO_RECEBIMENTO'
			when 24 then 'DESPACHO'
			when 25 then 'INSTRUCAO'
			when 26 then 'EMENDA'
			when 27 then 'PARECER'
			when 28 then 'ATA'
			when 29 then 'REQUERIMENTO'
			when 30 then 'MEMORANDO'
			when 31 then 'ATO_ADMINISTRATIVO'
			when 32 then 'TERMO_RECEBIMENTO'
			when 33 then 'ORCAMENTO_BASE'
			when 34 then 'PLANILHA_ORCAMENTARIA_CONTRATADA'
			when 35 then 'PLANILHA_ORCAMENTARIA_ADITIVO'
			when 36 then 'TERMO_PARALISACAO'
			when 37 then 'TERMO_RECEBIMENTO'
			when 38 then 'MEDICAO'
			when 39 then 'JUSTIFICATIVA_CANCELAMENTO_INTERVENCAO'
			when 40 then 'ATO'
			when 41 then 'COMUNICACAO_INTERNA'
			when 42 then 'DECRETO'
			when 43 then 'CONTRATO'
			when 44 then 'CONTRATO'
			when 45 then 'ATO'
			when 46 then 'EDITAL'
			when 47 then 'DECRETO'
			when 48 then 'TERMO_MEDICAO'
			when 49 then 'ATO_CONSORCIO'
			when 50 then 'CONSTITUICAO'
			when 51 then 'ATO_ADMINISTRATIVO'
			when 52 then 'DELIBERACAO'
			when 53 then 'OFICIO'
			when 54 then 'INSTRUCAO'
			when 55 then 'INSTRUCAO'
			when 56 then 'INSTRUCAO'
			when 57 then 'PROJETO'
			when 58 then 'REGISTRO_IMOVEIS'
			when 59 then 'PARECER'
			when 60 then 'MULTIMIDIA'
			when 61 then 'IMAGEM_EXEMPLAR_PUBLICA_ORGAO_OFICIAL'
			when 62 then 'ATO_COMISSAO_EXEC_LEGIS'
			when 63 then 'ATO_ADMINISTRATIVO'
			when 64 then 'EDITAL'
        else 'ATO' end) as tipo_ato,
		(CAST(ato.txjnumero as text) || '/' || CAST(ato.txjano as text)) as numero_oficial,
       	mvto.movdata as data_criacao,
	    mvto.movdata as data_vigorar,
       	mvto.movdata as dataResolucao,
		(case pub.pubdata::text when '1900-01-01' then null else pub.pubdata end) as data_publicacao,
       	left(ato.txjementa, 80) as ementa,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','natureza-texto-juridico', ato.tctcodigo))) as id_natureza_texto_juridico,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','tipo-ato', ato.tctcodigo))) as id_tipo_ato,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat( /* sistema 			*/ '305',
																									/* tipo registro 	*/ 'ato',
																									/* numero ato 		*/ ato.txjnumero,
																									/* anot ato 		*/ ato.txjano,
																									/* tipo ato 		*/ (case cat.tctcodigo
																										when 1 then  'DECRETO'
																										when 2 then  'PORTARIA'
																										when 3 then  'RESOLUCAO'
																										when 4 then  'EDITAL'
																										when 5 then  'LEI'
																										when 6 then  'CONVENIO'
																										when 7 then  'MEDIDA_PROVISORIA'
																										when 8 then  'CONTRATO'
																										when 9 then  'LEI_AUTORIZATIVA'
																										when 10 then 'ESTATUTO_SOCIAL'
																										when 11 then 'LEI_CRIACAO'
																										when 12 then 'DESPACHO'
																										when 13 then 'DESPACHO'
																										when 14 then 'MEMORANDO'
																										when 15 then 'DESPACHO'
																										when 16 then 'PROCESSO'
																										when 17 then 'PARECER'
																										when 18 then 'ATO'
																										when 19 then 'DECRETO_LEGISLATIVO'
																										when 20 then 'LEI_COMPLEMENTAR'
																										when 21 then 'LEI_ORGANICA'
																										when 22 then 'EMENDA_MODIFICATIVA'
																										when 23 then 'TERMO_RECEBIMENTO'
																										when 24 then 'DESPACHO'
																										when 25 then 'INSTRUCAO'
																										when 26 then 'EMENDA'
																										when 27 then 'PARECER'
																										when 28 then 'ATA'
																										when 29 then 'REQUERIMENTO'
																										when 30 then 'MEMORANDO'
																										when 31 then 'ATO_ADMINISTRATIVO'
																										when 32 then 'TERMO_RECEBIMENTO'
																										when 33 then 'ORCAMENTO_BASE'
																										when 34 then 'PLANILHA_ORCAMENTARIA_CONTRATADA'
																										when 35 then 'PLANILHA_ORCAMENTARIA_ADITIVO'
																										when 36 then 'TERMO_PARALISACAO'
																										when 37 then 'TERMO_RECEBIMENTO'
																										when 38 then 'MEDICAO'
																										when 39 then 'JUSTIFICATIVA_CANCELAMENTO_INTERVENCAO'
																										when 40 then 'ATO'
																										when 41 then 'COMUNICACAO_INTERNA'
																										when 42 then 'DECRETO'
																										when 43 then 'CONTRATO'
																										when 44 then 'CONTRATO'
																										when 45 then 'ATO'
																										when 46 then 'EDITAL'
																										when 47 then 'DECRETO'
																										when 48 then 'TERMO_MEDICAO'
																										when 49 then 'ATO_CONSORCIO'
																										when 50 then 'CONSTITUICAO'
																										when 51 then 'ATO_ADMINISTRATIVO'
																										when 52 then 'DELIBERACAO'
																										when 53 then 'OFICIO'
																										when 54 then 'INSTRUCAO'
																										when 55 then 'INSTRUCAO'
																										when 56 then 'INSTRUCAO'
																										when 57 then 'PROJETO'
																										when 58 then 'REGISTRO_IMOVEIS'
																										when 59 then 'PARECER'
																										when 60 then 'MULTIMIDIA'
																										when 61 then 'IMAGEM_EXEMPLAR_PUBLICA_ORGAO_OFICIAL'
																										when 62 then 'ATO_COMISSAO_EXEC_LEGIS'
																										when 63 then 'ATO_ADMINISTRATIVO'
																										when 64 then 'EDITAL'
																							        else 'ATO' end)))) as id_gerado
	from wlg.tbtextojuridico ato
	inner join wlg.tbcategoriatexto cat on (cat.tctcodigo = ato.tctcodigo)
	inner join wlg.tbmovimentotexto mvto on (mvto.txjcodigo = ato.txjcodigo and mvto.movtipo = 2)
	inner join wlg.tbpublicacao pub on (pub.txjcodigo = ato.txjcodigo)
) tab
where id_gerado is null
--limit 2
