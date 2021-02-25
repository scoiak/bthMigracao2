select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-arp' as tipo_registro,
	concat(nro_minuta, '/', ano_minuta) as minuta,
	concat(nro_ata, '/', ano_ata, ' (', arpsequencia, ')') as ata,
	'@' as separador,
	*
from (
	select
		a.clicodigo,
		a.minano as ano_minuta,
		a.minnro as nro_minuta,
		t.pcsano as ano_processo,
		t.pcsnro as nro_processo,
		a.arpsequencia,
		a.arpano as ano_ata,
		a.arpnro as nro_ata,
		u.uninomerazao,
		a.arpdata::varchar as dt_assinatura,
		a.arpdatavigini::varchar as dt_inicio_vigencia,
		a.arpdatavigfim::varchar as dt_fim_vigencia,
		a.arpobjeto as objeto,
		'EXECUCAO' as situacao,
		'PROCESSO_ADMINISTRATIVO' as origem,
		'QUANTIDADE' AS tipo_controle_saldo,
		0 as valor_original,
		20 as id_tipo_instrumento,
		160 as id_fundamentacao_legal,
		9999 as sequencial,
		concat('Migração: Minuta ', a.minnro, '/', a.minano, ', Ata ', a.arpnro, '/', a.arpano, ', Sequencial ', a.arpsequencia) as observacao,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'ata-rp', a.clicodigo, a.arpano, a.arpnro, a.unicodigo))), 0) as id_ata,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', a.clicodigo, a.minano, a.minnro))) as id_processo,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,'tipo-objeto', (case t.mintipoobjeto when 1 then 'Compras e Outros Serviços' when 2 then 'Obras e Serviços de Engenharia' when 3 then 'Concessoes e Permissões de Serviços Públicos' when 4 then 'Alienação de bens' when 5 then 'Concessão e Permissão de Uso de Bem Público' when 6 then 'Aquisição de bens' when 7 then 'Contratação de Serviços'  else 'Compras Outros Serviços' end)))), 10) as id_tipo_objeto,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', a.clicodigo))) as id_entidade,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'contratacao-arp', a.clicodigo, a.arpano, a.arpnro, '@', a.arpsequencia))) as id_gerado
	from wco.tbataregpreco a
	inner join wun.tbunico u on (u.unicodigo = a.unicodigo)
	left join wco.tbminuta t on (t.clicodigo = a.clicodigo and t.minano = a.minano and t.minnro = a.minnro)
	where a.clicodigo = {{clicodigo}}
	and a.minano = {{ano}}
	--and a.minnro = 153
	order by 1, 2 desc, 3 desc, 4
) tab
where id_gerado is null
and id_ata is not null
and id_processo is not null
and id_fornecedor is not null
--limit 1