select
	row_number() over() as id,
	'305' as sistema,
	'comprovante' as tipo_registro,
	concat(copnro, '/', copano) as nro_sf,
	'@' as separador,
	*
from (
	select
		nf.clicodigo,
		nf.copano,
		nf.copnro,
		nf.nfisequencia,
		nf.nfinro as numero_comprovante,
		nf.nfidataemissao::varchar as data_emissao,
		coalesce((select sum(inf.itnvalortotal) from wco.tbitemnota inf where inf.clicodigo = nf.clicodigo and inf.copano = nf.copano and inf.copnro = nf.copnro and inf.nfisequencia = nf.nfisequencia), 0) as valor_bruto,
		0.00 as valor_desconto,
		coalesce((select sum(inf.itnvalortotal) from wco.tbitemnota inf where inf.clicodigo = nf.clicodigo and inf.copano = nf.copano and inf.copnro = nf.copnro and inf.nfisequencia = nf.nfisequencia), 0)  as valor_liquido,
		nf.nfiserie as serie,
		null as codigo_validacao,
		concat(coalesce(nf.nfiobservacao, ''), '(Migração: Compra ', nf.copnro, '/', nf.copano, ', Seq. ', nf.nfisequencia, ')') as finalidade,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
		848 as id_tipo_comprovante, -- 848: Nota Fiscal
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'comprovante', nf.clicodigo, nf.copano, nf.copnro, '@', nf.nfisequencia))) as id_gerado
	from wco.tbnotafiscal nf
	inner join wco.tbcompra c using (clicodigo, copano, copnro)
	inner join wun.tbunico u using (unicodigo)
	where true
	and nf.clicodigo = {{clicodigo}}
	and nf.copano = {{ano}}
	--and nf.nfinro = '13'
	order by 1, 2 desc, 3 desc, 4
) tab
where id_gerado is null
and id_fornecedor is not null
limit 1