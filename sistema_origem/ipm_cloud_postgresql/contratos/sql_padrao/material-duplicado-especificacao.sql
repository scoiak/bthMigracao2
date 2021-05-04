select
	row_number() over() as id,
	'305' as sistema,
	'material-duplicado-especificacao' as tipo_registro,
	'@' as separador,
	*
from (	
	select
		i.clicodigo,
		concat(i.minnro, '/', i.minano) as minuta,
		i.minnro,
		i.minano,
		i.cmiid,
		concat(coalesce(p.prddescdet, p.prddescricao), ' (Minuta ', i.minnro, '/', i.minano, ', Clicodigo ', i.clicodigo, ', CMIID ', i.cmiid, ')') as descricao,
		concat(i.minano, i.minnro, i.cmiid)::integer as codigo_especificacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', i.clicodigo, i.minano, i.minnro))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'unidade-medida', (select up.cnicodigo from wun.tbunipro up where up.prdcodigo = p.prdcodigo limit 1)))) as id_un_medida,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material', p.prdcodigo))) as id_material,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'material-duplicado-especificacao', i.clicodigo, i.minano, i.minnro, '@', i.cmiid))) as id_gerado
	from wco.tbitemin i
	inner join wun.tbproduto p on (p.prdcodigo = i.prdcodigo)
	where i.clicodigo = {{clicodigo}}
	and i.minano >= 2017
	--and i.minano = {{ano}}
	--and i.minnro = 27
	and exists ( -- Verifica a existência de um item duplicado para o processo atual
		select 1
		from (
			select distinct
				i2.prdcodigo,
				count(*) as qtd
			from wco.tbitemin i2
			where i2.clicodigo = i.clicodigo
			and i2.minano = i.minano
			and i2.minnro = i.minnro
			and i2.prdcodigo = i.prdcodigo
			group by 1
		) aux 
		where aux.qtd > 1 
		limit 1)
	order by i.clicodigo, i.minano desc, i.minnro desc, i.cmiid
) tab
where id_gerado is null
and id_un_medida is not null 
and id_material is not null
--and id_processo in (211292)

