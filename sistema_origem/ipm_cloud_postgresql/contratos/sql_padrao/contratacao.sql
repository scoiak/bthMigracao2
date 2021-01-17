select
	row_number() over() as id,
	'305' as sistema,
	'contratacao' as tipo_registro,
	*
from (
	select
		c.clicodigo,
		c.ctrano as ano_contrato,
		c.ctridentificador as nro_contrato,
		c.ctrano as ano_termo,
		c.ctridentificador as nro_termo,
		c.ctrnro,
		c.minano as ano_processo,
		c.minnro as nro_processo,
		left(c.ctrobjetotc, 500) as objeto,
		u.unicpfcnpj,
		22 as id_tipo_instrumento,
		t.mintipoobjeto,
		'QUANTIDADE' AS tipo_controle_saldo,
		c.ctrdataassinatura::varchar as dt_assinatura,
		c.ctrdatainivig::varchar as dt_inicio_vigencia,
		c.ctrdatavencto::varchar as dt_fim_vigencia,
		c.ctrvalor as valor_original,
		'PROCESSO_ADMINISTRATIVO' as origem,
		concat('Contratação ', c.ctridentificador, '/', c.ctrano, ' (', c.ctrnro, ')') as observacao,
		(case when c.ctrdataassinatura is null then 'AGUARDANDO_ASSINATURA' else 'EXECUCAO' end) as situacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', c.clicodigo, c.minano, c.minnro))) as id_processo,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,'tipo-objeto', (case t.mintipoobjeto when 1 then 'Compras e Outros Serviços' when 2 then 'Obras e Serviços de Engenharia' when 3 then 'Concessoes e Permissões de Serviços Públicos' when 4 then 'Alienação de bens' when 5 then 'Concessão e Permissão de Uso de Bem Público' when 6 then 'Aquisição de bens' when 7 then 'Contratação de Serviços'  else 'Compras Outros Serviços' end)))), 10) as id_tipo_objeto,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', c.clicodigo))) as id_entidade,
		null as id_gerado
	from wco.tbcontrato c
	inner join wun.tbunico u on (u.unicodigo = c.unicodigo)
	left join wco.tbminuta t on (t.clicodigo = c.clicodigo and t.minano = c.minano and t.minnro = c.minnro)
	order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_processo is not null
and id_fornecedor is not null
limit 1