select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-sf-recebimento-comprovante' as tipo_registro,
	concat(copnro, '/', copano) as nro_sf,
	'@' as separador,
	*
from (
	(select distinct
		nf.clicodigo,
		nf.copano,
		nf.copnro,
		nf.nfisequencia,
		coalesce((select sum(inf.itnvalortotal) from wco.tbitemnota inf where inf.clicodigo = nf.clicodigo and inf.copano = nf.copano and inf.copnro = nf.copnro and inf.nfisequencia = nf.nfisequencia), 0) as valor,
		coalesce((select ct.ctranosup from wco.tbcontrato ct where ct.clicodigo = c.clicodigoctr and ct.minano = t.minano and ct.minnro = t.minnro and ct.ctridentificador = c.ctridentificador), c.ctrano) as exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'comprovante', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_comprovante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', nf.clicodigo, (select ct.ctranosup from wco.tbcontrato ct where ct.clicodigo = c.clicodigoctr and ct.minano = t.minano and ct.minnro = t.minnro and ct.ctridentificador = c.ctridentificador), (select ct.ctridentsup from wco.tbcontrato ct where ct.clicodigo = c.clicodigoctr and ct.minano = t.minano and ct.minnro = t.minnro and ct.ctridentificador = c.ctridentificador)))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where tipo_registro = 'contratacao-sf' and i_chave_dsk1 = c.clicodigoctr::varchar and i_chave_dsk5 = c.copano::varchar and i_chave_dsk6 = c.copnro::varchar) as id_sf,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf-recebimento', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_recebimento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf-recebimento-comprovante', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_gerado
	from wco.tbnotafiscal nf
	inner join wco.tbcompra c using (clicodigo, copano, copnro)
	inner join wco.tbminuta t using (clicodigo, minano, minnro)
	--inner join wco.tbcontrato ct using (clicodigo, minano, minnro, unicodigo)
	where nf.clicodigo = {{clicodigo}}
	and t.minano = {{ano}}
	and t.minnro = 47
	--and c.copnro = 1541
	and t.mintipoconcorrencia <> 2
	and c.minano is not null
	and c.minnro is not null)
) tab
where id_gerado is null
and id_comprovante is not null
and id_contratacao is not null
and id_sf is not null
and id_recebimento is not null