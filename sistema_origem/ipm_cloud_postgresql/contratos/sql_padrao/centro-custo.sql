-- O sistema da IPM permite vincular solicitações com organogramas de outros exercícios. Possivelmente será necessário
-- realizar cadastro manual de organogramas no Betha Cloud para sanar tal situação

select
	row_number() over() as id,
	'305' as sistema,
	'centro-custo' as tipo_registro,
	*
from (
	select distinct
		organo as chave_dsk1,
		replace(cncclassif,'.','') as chave_dsk2,
		cncclassif as organograma,
		(select count(*) FROM regexp_matches(cncclassif, '\.', 'g')) + 1 as nivel,
		left(cncdescricao, 60) as descricao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'configuracoes-organogramas', organo))) as id_configuracao_organograma,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'centro-custo', organo, replace(cncclassif,'.','')))) as id_gerado
	from wun.tbcencus
) tab
where id_configuracao_organograma is not null
and id_gerado is null
and chave_dsk1 in (2005, 2006, 2007)
order by chave_dsk1, nivel, chave_dsk2
--limit 5