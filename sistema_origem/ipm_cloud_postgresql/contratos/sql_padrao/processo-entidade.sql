select
	row_number() over() as id,
	'305' as sistema,
	'processo-entidade' as tipo_registro,
	*
from (
	select distinct
		r.clicodigoprc as clicodigo,
		r.pcsano as ano_processo,
		r.pcsnro as nro_processo,
		r.clicodigoreq as entidade_participante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', r.clicodigoreq))) as id_entidade_participante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', r.clicodigoprc, r.pcsano, r.pcsnro))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-entidade', r.clicodigoprc, r.pcsano, r.pcsnro, r.clicodigoreq))) as id_gerado
	from wco.tbreqprocesso r
	where r.clicodigoprc = {{clicodigo}}
	order by 1, 2 desc, 3 desc, 4
) tab
where id_gerado is null
and id_processo is not null
and id_entidade_participante is not null
--limit 1