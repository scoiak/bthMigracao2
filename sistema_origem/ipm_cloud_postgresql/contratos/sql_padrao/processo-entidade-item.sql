select
	row_number() over() as id,
	'305' as sistema,
	'processo-entidade-item' as tipo_registro,
	'@' as separador,
	*,
	(tab.qtd_distribuida + (case when tab.qtd_diferenca < 0 then 0 else tab.qtd_diferenca end)) as qtd_final
from (
	select
		d.*,
		--(select ip.cmiqtde from wco.tbitemin ip where ip.clicodigo = d.clicodigomin and ip.minano = d.ano_processo and ip.minnro = d.nro_processo and coalesce(ip.lotcodigo, 0) = nro_lote and ip.cmiitem = numero_item_original) as qtd_total_item,
		--(select sum(ir.ritqtdeutilizada) from wco.tbreqitemin ir where ir.clicodigomin = d.clicodigomin and ir.minano = d.ano_processo and ir.minnro = d.nro_processo and ir.cmiid = d.cmiid) as qtd_tota_requisitada,
		(case -- O saldo faltante das requisições é adicionado à distribuição da entidade do processo para fechar a quantidade prevista do item
			when d.clicodigomin <> d.clicodigoreq then 0.0
			else coalesce((select ip.cmiqtde from wco.tbitemin ip where ip.clicodigo = d.clicodigomin and ip.minano = d.ano_processo and ip.minnro = d.nro_processo and coalesce(ip.lotcodigo, 0) = nro_lote and ip.cmiitem = numero_item_original), 0) - coalesce((select sum(ir.ritqtdeutilizada) from wco.tbreqitemin ir where ir.clicodigomin = d.clicodigomin and ir.minano = d.ano_processo and ir.minnro = d.nro_processo and ir.cmiid = d.cmiid),0)
		 	end
		) as qtd_diferenca,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', d.clicodigomin, ano_processo, nro_processo))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', d.clicodigomin, ano_processo, nro_processo, '@', nro_lote, '@', numero_item_original))) as id_item,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-entidade', d.clicodigomin, ano_processo, nro_processo, d.clicodigoreq))) as id_entidade_participante,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-entidade-item', d.clicodigomin, ano_processo, nro_processo, '@', nro_lote, '@', numero_item_original, '@', d.clicodigoreq)))  as id_gerado
	from (
		select
			r.clicodigomin,
			r.minano as ano_processo,
			r.minnro as nro_processo,
			r.clicodigoreq,
			i.cmiitem as numero_item_original,
			coalesce(i.lotcodigo, 0) as nro_lote,
			i.cmiid,
			(select count(distinct ep.clicodigoreq) from wco.tbreqprocesso ep where ep.clicodigoprc = r.clicodigomin and ep.pcsano = r.minano and ep.pcsnro = r.minnro) as qtd_entidades_participantes,
			sum(ritqtdeutilizada) as qtd_distribuida
		from wco.tbreqitemin r
		natural join wco.tbitemin i
		where r.clicodigomin = {{clicodigo}}
		and (select count(distinct clicodigoreq)
			 from wco.tbreqprocesso ep
			 where ep.clicodigoprc = r.clicodigomin
			 and ep.pcsano = r.minano
			 and ep.pcsnro = r.minnro) > 1
		group by 1, 2, 3, 4, 5, 6, 7, 8
		order by 1, 2 desc, 3 desc, 5 asc, 6 asc
	) d
) tab
where id_gerado is null
and id_processo is not null
and id_item is not null
and id_entidade_participante is not null