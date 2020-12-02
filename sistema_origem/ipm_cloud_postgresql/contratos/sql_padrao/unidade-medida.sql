select
	row_number() over() as id,
	'305' as sistema,
	'unidade-medida' as tipo_registro,
	*
from (
	select
        cnicodigo AS chave_dsk1,
        cnicodigo,
        cniunidade as simbolo,
        coalesce(cnidescricao, cniunidade) AS descricao,
        'OUTROS' AS grandeza,
        true AS fracionada,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'unidade-medida', cnicodigo))) as id_gerado
   from wun.tbcaduni
) tab
where id_gerado is null
order by chave_dsk1
--limit 2