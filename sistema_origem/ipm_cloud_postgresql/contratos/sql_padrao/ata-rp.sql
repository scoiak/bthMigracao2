select
	row_number() over() as id,
	'305' as sistema,
	'ata-rp' as tipo_registro,
	*
from (
	select
	 	rp.clicodigo,
	 	rp.arpano as ano_ata,
	 	rp.arpnro as nro_ata,
	 	rp.minano as ano_processo,
	 	rp.minnro as nro_processo,
	 	rp.arpsequencia,
	 	rp.unicodigo,
	 	left((select lic.licdatahomologacao::varchar from wco.tblicitacao lic where lic.clicodigo = rp.clicodigo and lic.minano = rp.minano and lic.minnro = rp.minnro), 4) as ano_homologacao,
	 	(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_fornecedor,
	 	rp.arpdata::varchar as data_ata,
	 	rp.arpdata::varchar as data_assinatura,
	 	rp.arpdatavigfim::varchar as data_vencimento,
	 	rp.arpobjeto as objeto,
	 	concat('Migração: Ata ', rp.arpnro, '/', rp.arpano, ', Sequencia ',  rp.arpsequencia, ', Minuta ', rp.minnro, '/', rp.minano) as observacao,
	 	'PROCESSO_HOMOLOGADO' AS origem,
	    'ANDAMENTO' AS situacao,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'parametro-exercicio', rp.arpano))) as id_parametro_exercicio,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', rp.clicodigo, rp.minano, rp.minnro))) as id_processo,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-ato-final', rp.clicodigo, rp.minano, rp.minnro))) as id_ato_final,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'ata-rp', rp.clicodigo, rp.arpano, rp.arpnro, rp.unicodigo))) as id_gerado
	 from wco.tbataregpreco rp
	 inner join wun.tbunico u on u.unicodigo = rp.unicodigo
	 where rp.clicodigo = {{clicodigo}}
	 and rp.minano = {{ano}}
	 --and rp.minnro = 166
	 order by 1, 2 desc, 3 asc
) tab
where id_gerado is null
and id_processo is not null
and id_fornecedor is not null
and id_ato_final is not null
--limit 1