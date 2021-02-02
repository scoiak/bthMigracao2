select
	row_number() over() as id,
	'305' as sistema,
	'material' as tipo_registro,
	*
from (
	select distinct
       p.prdcodigo AS chave_dsk1,
       p.prdcodigo as identificadorPrimeiro,
       p.grpcodigo AS grupo,
       p.clacodigo AS classe,
       left(p.prddescricao, 500) AS descricao,
       --left(p.prdcodigo || ' - ' || p.prddescricao || '(CMB)', 500) AS descricao,
       (case p.prdclassificacao when 2 then 'PERMANENTE' when 3 then 'SERVICO' else 'MATERIAL' end) as tipo_material,
       (select up.cnicodigo from wun.tbunipro up where up.prdcodigo = p.prdcodigo limit 1) as unidade,
       coalesce(p.prddescdet, p.prddescricao) as especificacao,
       '' AS elemento,
       'N' AS tem_elemento,
       (case prdcategoria when 4 then 'COMBUSTIVEL' else 'OUTROS' end) as classificacao,
       (case p.prdclassificacao	when 3 then false else (case prdcategoria when 3 then true else false end) end) as estocavel,
       'NAO_APLICA' AS tipoCombustivel,
	   true AS ativo,
       null AS dataInativo,
       (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'grupo', p.grpcodigo))) as id_grupo,
       (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'classe', p.grpcodigo, p.clacodigo))) as id_classe,
       (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'unidade-medida', (select up.cnicodigo from wun.tbunipro up where up.prdcodigo = p.prdcodigo limit 1)))) as id_un_medida,
       (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'material', p.prdcodigo))) as id_gerado
    from wun.tbproduto p
    order by p.prdcodigo
) tab
where id_gerado is null
and id_grupo is not null
and id_classe is not null
and id_un_medida is not null
--limit 10