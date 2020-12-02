select
	'1' as id,
	'305' as sistema,
	'organogramas' as tipo_registro,
	*
from (
  select distinct
  	  organo as chave_dsk1,
	  right('00'||cast(orgcodigo as text),2) as chave_dsk2,
      1 as nivel ,
      orgdescricao as descricao,
      (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'configuracoes-organogramas', organo))) as id_configuracao_organograma,
      (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'organogramas', organo, right('00'||cast(orgcodigo as text),2)))) as id_gerado
  from wun.tborgao
) tab
where id_gerado is null
and id_configuracao_organograma is not null
and chave_dsk1 in (2015, 2016, 2017, 2018, 2019, 2020)
order by chave_dsk1, chave_dsk2
limit 5
