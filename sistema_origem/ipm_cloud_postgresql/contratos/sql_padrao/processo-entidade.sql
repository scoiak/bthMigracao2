select
	row_number() over() as id,
	'305' as sistema,
	'processo-entidade' as tipo_registro,
	*
from (
	select distinct
		*,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', aux.entidade_participante))) as id_entidade_participante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', aux.clicodigo, aux.ano_processo, aux.nro_processo))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-entidade', aux.clicodigo, ano_processo, aux.nro_processo, aux.entidade_participante))) as id_gerado
	from (
		(select distinct
			r.clicodigoprc as clicodigo,
			r.pcsano as ano_processo,
			r.pcsnro as nro_processo,
			r.clicodigoreq as entidade_participante
		from wco.tbreqprocesso r
		where r.clicodigoprc = {{clicodigo}}
		and r.pcsano = {{ano}}
		--and r.pcsnro = 80
		order by 1, 2 desc, 3 desc, 4)
		union all
		(select distinct
			d.clicodigo as clicodigo,
			d.minano  as ano_processo,
			d.minnro as nro_processo,
			d.clicodigocnv as entidade_participante
		from wco.tbdotmin d
		where d.clicodigo = {{clicodigo}}
		and d.minano = {{ano}}
		--and d.minnro = 80
		and d.dotcodigo is not null
		order by 1, 2 desc, 3 desc, 4)
	) aux
) tab
where id_gerado is null
and id_processo is not null
and id_entidade_participante is not null