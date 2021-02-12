with anos as (select * from generate_series(2005, 2006, 2007) as ano)
select 
	row_number() over() as id,
	ano,
	'##.###.###.###.###' as mascara,
	concat('Configuração Organograma ', ano) as descricao,
	'.' as separador,
	305 as sistema,
	'configuracoes-organogramas' as  tipo_registro,
	1 as nivel1,
	2 as digito1,
	'Órgão' as descricao1,
	2 as nivel2,
	3 as digito2,
	'Unidade' as descricao2,
	3 as nivel3,
	3 as digito3,
	'Centro de Custo' as descricao3,
	4 as nivel4,
	3 as digito4,
	'Centro de Custo Pai' as descricao4,
	5 as nivel5,
	3 as digito5,
	'Secretaria' as descricao5,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'configuracoes-organogramas', ano))) as id_gerado
from anos
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'configuracoes-organogramas', ano))) is null
order by ano desc