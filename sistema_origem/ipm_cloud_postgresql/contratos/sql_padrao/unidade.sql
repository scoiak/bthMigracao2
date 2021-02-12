select
	row_number() over() as id,
	'305' as sistema,
	'unidade' as tipo_registro,
	*
from (
	select distinct
       organo as  chave_dsk1,
       right('00'||cast(orgcodigo as text),2)||right('000'||cast(undcodigo as text),3) as chave_dsk2,
       2 as nivel ,
       left(unddescricao, 60) as descricao,
       (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'configuracoes-organogramas', organo))) as id_configuracao_organograma,
       (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305,
       																							  'unidade',
       																							  organo,
       																							  right('00'||cast(orgcodigo as text),2)||right('000'||cast(undcodigo as text),3)))
       																							  ) as id_gerado
  from wun.tbunidade
) tab
where id_configuracao_organograma is not null
and id_gerado is null
and chave_dsk1 in (2005, 2006, 2007)
order by chave_dsk2