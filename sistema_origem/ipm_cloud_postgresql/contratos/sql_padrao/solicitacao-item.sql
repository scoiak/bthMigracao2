select
	row_number() over() as id,
	'305' as sistema,
	'solicitacao-item' as tipo_registro,
	*,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'solicitacao-item', chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk5))) as id_gerado
from (
	select
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo)))::text as chave_dsk1,
		rqcano as chave_dsk2,
		rqcnro as chave_dsk3,
		'@' as chave_dsk4, -- Esse chave exige um separado para evitar ambiguidade
		row_number() over(partition by clicodigo, rqcano, rqcnro) as chave_dsk5,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'solicitacao', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))), rqcano, rqcnro))) as id_solicitacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', prdcodigo))) as id_material,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', prdcodigo))) as id_especificacao,
	    row_number() over(partition by clicodigo, rqcano, rqcnro) as numero,
		(case rqcsituacao
	   	   when 1 then 'EM_EDICAO'
		   when 2 then 'EM_EDICAO'
		   when 3 then 'EM_EDICAO'
		   when 4 then 'EM_EDICAO'
		   when 6 then 'CANCELADA'
		   when 7 then 'APROVADA'
		   when 8 then 'CANCELADA'
		   when 9 then 'ATENDIDA'
		   when 10 then 'ATENDIDA'
		   when 11 then 'EM_EDICAO'
		   when 12 then 'EM_EDICAO'
		   when 13 then 'EM_EDICAO'
		   when 14 then 'ATENDIDA'
		   when 16 then 'CANCELADA'
		   else 'EM_EDICAO' end ) as status,
		   round((valor_total/quantidade), 4) as valor_unitario,
		*
	from (
		select
			clicodigo,
			rqcano,
			rqcnro,
			(case when lorcodigo is null then 'por_item' else 'por_lote' end) as tipo,
			prdcodigo,
			rqcsituacao,
			false as amostra,
			--avg(coalesce(cast(ircvlrmaxunitario as numeric(15, 4)), 0)) as valor_unitario,
			sum(coalesce(cast(ircquantidade as numeric(14, 3)), 1)) as quantidade,
			sum(coalesce(cast(ircvlrmaxtotal as numeric(16, 2)), 0)) as valor_total
		from wco.tbreqcomp sol
		natural join wco.tbitemreqcomp item
		where rqcano >= {{ano}}
		and clicodigopln = {{clicodigo}}
		--and rqcnro in (10)
		group by 1, 2, 3, 4, 5, 6, 7
		order by 1, 2, 3, 4, 5
	) as itens
	order by chave_dsk1, chave_dsk2 desc, chave_dsk3 desc, chave_dsk5
) as tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'solicitacao-item', chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk5))) is null