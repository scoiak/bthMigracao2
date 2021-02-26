select
	row_number() over() as id,
	'305' as sistema,
	'processo-lote' as tipo_registro,
	*
from (
	select distinct
		l.clicodigo,
	    l.minano as ano_processo,
	    l.minnro as nro_processo,
	    l.lotcodigo,
	    l.lotcotacaomaxima as preco_maximo,
	    l.lotcodigo as numero_lote,
	    l.lotdescricao as descricao_lote,
	    'LIVRE' as tipo_participacao,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', l.clicodigo, l.minano, l.minnro))) as id_processo,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-lote', l.clicodigo, l.minano, l.minnro, l.lotcodigo))) as id_gerado
	from wco.tblotemin l
	where l.clicodigo = {{clicodigo}}
	and l.minano = {{ano}}
	--and l.minnro = 68
	order by 1, 2 desc, 3 desc, 4 asc
) tab
where id_gerado is null
and id_processo is not null
--limit 1