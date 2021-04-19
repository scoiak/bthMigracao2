select
	row_number() over() as id,
	'305' as sistema,
	'processo-participante-proposta' as tipo_registro,
	'@' as separador,
	*
from (
  select distinct
  	a.clicodigo,
  	a.minano as ano_processo,
  	a.minnro as nro_processo,
  	a.aprsequencia as sequencial,
  	concat(a.minnro, a.aprsequencia)::integer as nro_ata,
  	left(a.aprdata::varchar, 4)::integer as ano_ata,
  	coalesce (a.aprobservacao, 'MIGRACAO CLOUD - ATA SEM TEXTO') as texto_ata,
  	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', a.clicodigo, a.minano, a.minnro))) as id_processo,
  	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-sessao', a.clicodigo, a.minano, a.minnro))) as id_sessao,
  	386 as tipo_ata,
  	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-sessao-ata', a.clicodigo, a.minano, a.minnro, '@', a.aprsequencia))) as id_gerado
  from wco.tbatalicitacao a
  where a.clicodigo = {{clicodigo}}
  and a.minano = {{ano}}
  and a.minnro in (81)
  order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_processo is not null
and id_sessao is not null
--and id_processo in (213969, 214060, 213812, 213811, 214200, 214180, 213809, 213807, 214166, 213803, 213802, 214107)