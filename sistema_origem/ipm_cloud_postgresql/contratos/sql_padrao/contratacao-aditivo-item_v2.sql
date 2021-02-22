select
    1 as id,
	891485 as id_contratacao,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-item', tab.clicodigo, tab.minano, tab.nro_contrato, '@', tab.cmiid))) as id_contratacao_item,
	58285 as id_aditivo,
	*,
	'@' as separador,
	trunc(((total_compra - total_est - qcpvlrtotal) /tab.qcpvlrunit), 4) as qtd_aditivada,
	(total_compra - total_est - qcpvlrtotal) as vl_aditivado
from (
	select
		2019 as ano_contrato,
		9 as nro_contrato, -- IDENTIFICADOR!
		q.clicodigo,
		q.minano,
		q.minnro,
		q.unicodigo,
		q.cmiid,
		q.qcpvlrunit,
		q.qcpvlrtotal,
		coalesce((select trunc(sum(auxci.itcvlrtotal), 2)
				  from wco.tbitemcompra auxci
				  where auxci.cmiid = q.cmiid and md5(concat(auxci.clicodigo , '@', auxci.copano, '@', auxci.copnro)) in (
				  		select md5(concat(auxc.clicodigo , '@', copano, '@', copnro)) as hash
				  		from wco.tbcompra auxc
				  		where md5(concat(auxc.clicodigo, '@', auxc.ctrano, '@', auxc.ctridentificador)) in (
				  				select md5(concat(auxct.clicodigo, '@', auxct.ctrano, '@', auxct.ctridentificador)) as h
							  	from wco.tbcontrato auxct
							  	where auxct.clicodigo = q.clicodigo
							  	and auxct.minano = q.minano
							  	and auxct.minnro = q.minnro
							  	and auxct.unicodigo = q.unicodigo)))
	    , 0) as total_compra,
		coalesce((select sum(e.iesvlrtotal + e.iesvlrdesctotal)
				 from wco.tbitemcompraest e where md5(concat(e.clicodigo, '@', e.copano, '@', e.copnro, '@', e.itcitem)) in (
				  select md5(concat(auxci.clicodigo, '@', auxci.copano, '@', auxci.copnro, '@', auxci.itcitem))
				  from wco.tbitemcompra auxci
				  where auxci.cmiid = q.cmiid
				  and md5(concat(auxci.clicodigo , '@', auxci.copano, '@', auxci.copnro)) in (
				  		select md5(concat(auxc.clicodigo , '@', copano, '@', copnro)) as hash
				  		from wco.tbcompra auxc
				  		where md5(concat(auxc.clicodigo, '@', auxc.ctrano, '@', auxc.ctridentificador)) in (
				  				select md5(concat(auxct.clicodigo, '@', auxct.ctrano, '@', auxct.ctridentificador)) as h
							  	from wco.tbcontrato auxct
							  	where auxct.clicodigo = q.clicodigo
							  	and auxct.minano = q.minano
							  	and auxct.minnro = q.minnro
							  	and auxct.unicodigo = q.unicodigo))))
	    , 0) as total_est
	from wco.vw_qcp_vencedor q
	where q.clicodigo = 11968
	and q.minano = 2016
	and q.minnro = 19
	--and cmiid > 1
	--and q.unicodigo = 621617
) tab
--limit 1