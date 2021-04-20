select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-sf-recebimento-item' as tipo_registro,
	false as houve_desconto,
	false as houve_retencao,
	'@' as separador,
	*
from (
	(select
		i.clicodigo,
		i.copano,
		i.copnro,
		i.nfisequencia,
		i.itcitem,
		i.itnqtde as quantidade,
		0.00 as desconto,
		i.itnvalortotal as valor_total,
		(select ic.cmiid from wco.tbitemcompra ic where ic.clicodigo = c.clicodigo and ic.copano = c.copano and ic.copnro = c.copnro and ic.itcitem = i.itcitem) as cmiid,
		coalesce((select ct.ctranosup from wco.tbcontrato ct where ct.clicodigo = c.clicodigoctr and ct.minano = t.minano and ct.minnro = t.minnro and ct.ctridentificador = c.ctridentificador), c.ctrano) as exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao', i.clicodigo, (select ct.ctranosup from wco.tbcontrato ct where ct.clicodigo = c.clicodigoctr and ct.minano = t.minano and ct.minnro = t.minnro and ct.ctridentificador = c.ctridentificador), (select ct.ctridentsup from wco.tbcontrato ct where ct.clicodigo = c.clicodigoctr and ct.minano = t.minano and ct.minnro = t.minnro and ct.ctridentificador = c.ctridentificador)))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro where tipo_registro = 'contratacao-sf' and i_chave_dsk1 = c.clicodigoctr::varchar and i_chave_dsk5 = c.copano::varchar and i_chave_dsk6 = c.copnro::varchar) as id_sf,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf-recebimento', i.clicodigo, i.copano, i.copnro, '@', i.nfisequencia))) as id_recebimento,
		(select id_gerado from public.controle_migracao_registro where tipo_registro = 'contratacao-sf-item' and i_chave_dsk1 = i.clicodigo::varchar and i_chave_dsk5 = c.copano::varchar and i_chave_dsk6 = c.copnro::varchar and i_chave_dsk8 = (select ic.cmiid from wco.tbitemcompra ic where ic.clicodigo = c.clicodigo and ic.copano = c.copano and ic.copnro = c.copnro and ic.itcitem = i.itcitem)::varchar) as id_sf_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-sf-recebimento-item', i.clicodigo, i.copano, i.copnro, '@', i.nfisequencia, '@', i.itcitem))) as id_gerado
	from wco.tbitemnota i
	inner join wco.tbcompra c using (clicodigo, copano, copnro)
	inner join wco.tbminuta t using (clicodigo, minano, minnro)
	where i.clicodigo = {{clicodigo}}
	and t.minano = {{ano}}
	and t.minnro = 47
	--and c.copnro = 1541
	and c.minano is not null
	and c.minnro is not null
	and t.mintipoconcorrencia <> 2)
) tab
where id_gerado is null
and id_sf is not null
and id_contratacao is not null
and id_recebimento is not null
and id_sf_item is not null