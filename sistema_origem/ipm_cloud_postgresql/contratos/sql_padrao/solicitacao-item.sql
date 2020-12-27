select
	row_number() over() as id,
	'305' as sistema,
	'solicitacao-item' as tipo_registro,
	*
from (
	select distinct
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))) as chave_dsk1,
	   rqcano as chave_dsk2,
	   rqcnro as chave_dsk3,
	   ircitem as chave_dsk4,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'solicitacao', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))), rqcano, rqcnro))) as id_solicitacao,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', prdcodigo))) as id_material,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-especificacao', prdcodigo))) as id_especificacao,
	   ircitem as numero,
	   coalesce(cast(ircquantidade as numeric(14, 3)), 1) as quantidade,
	   coalesce(cast(ircvlrmaxunitario as numeric(15, 4)), 0) as valor_unitario,
	   coalesce(cast(ircvlrmaxtotal as numeric(16, 2)), 0) as valor_total,
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
	   false as amostra,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'solicitacao-item', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))), rqcano, rqcnro, ircitem))) as id_gerado
	from wco.tbreqcomp
	natural join wco.tbitemreqcomp
	where rqcano >= {{ano}}
	and clicodigopln = {{clicodigo}}
	order by 2 desc, 3 desc, 4 asc
) as tab
where id_gerado is null
and id_material is not null
and id_especificacao is not null
and id_solicitacao is not null
--limit 5