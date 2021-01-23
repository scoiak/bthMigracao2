select
	row_number() over() as id,
	'305' as sistema,
	'despesa' as tipo_registro,
	*
from (
	select
		dot.clicodigo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', dot.clicodigo))) as id_entidade,
		dot.loaano,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'parametro-exercicio', dot.loaano))) as id_exercicio,
		dot.dotcodigo,
		left(acao.acodescricao, 100)  as descricao_despesa,
		concat(right('0' || dot.orgcodigo, 2), right('00' || dot.undcodigo , 3)) as organograma,
		dot.tfccodigo as funcao,
		dot.tsfcodigo as subfuncao,
		dot.pgrcodigo as programa,
		right('0000' || dot.acocodigo, 4) as acao,
		left(substring(dot.plncodigo::text, 2, length(dot.plncodigo::text)), 14) as natureza,
		left(dot.plncodigo::text, 14) as natureza_original,
		dot.vincodigo::text as recurso_ipm,
		concat(
			'0',
			substring(dot.vincodigo::text, 1, 1),
			substring(dot.vincodigo::text, 2, 2),
			substring(dot.vincodigo::text, 5, 2),
			substring(dot.vincodigo::text, 7, 4)
		) as recurso_bth,
		concat(
			'0.',
			substring(dot.vincodigo::text, 1, 1), '.',
			substring(dot.vincodigo::text, 2, 2), '.',
			substring(dot.vincodigo::text, 5, 2), '.',
			substring(dot.vincodigo::text, 7, 4)
		) as recurso_formatado,
		rec.vindescricao as desc_recurso,
		concat(
			/* organograma 	*/ concat(right('0' || dot.orgcodigo, 2), right('00' || dot.undcodigo , 3)),
			/* funcao 		*/ '.', right('00' || dot.tfccodigo, 2),
			/* subfuncao	*/ '.', right('000' || dot.tsfcodigo, 3),
			/* programa		*/ '.', right('0000' || dot.pgrcodigo, 4),
			/* acao			*/ '.', right('0000' || dot.acocodigo, 4),
			/* natureza		*/ '.', (substring(dot.plncodigo::text, 2, 1) || '.' || substring(dot.plncodigo::text, 3, 2) || '.00.00'),
			/* recurso		*/ '/', (concat('0', substring(dot.vincodigo::text, 1, 1), substring(dot.vincodigo::text, 2, 2), substring(dot.vincodigo::text, 5, 2),substring(dot.vincodigo::text, 7, 4)))
		) as mascara,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'despesa', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', dot.clicodigo))), dot.loaano, dot.dotcodigo))) as id_gerado
	from wpl.tbdotacao dot
	natural join wpl.tbvinculo rec
	natural join wpl.tbacao acao
	where clicodigo = {{clicodigo}}
	and loaano >= {{ano}}
	order by 1, 2 desc, 3 desc, 5 asc
) as tab
where id_gerado is null
and id_entidade is not null
and id_exercicio is not null
--limit 2