select
	row_number() over() as id,
	'305' as sistema,
	'processo' as tipo_registro,
	concat(nro_processo, '/', ano_processo) as proc_formatado,
	concat(numero_protocolo, '/', ano_protocolo) as lic_formatado,
	*
from (
	select
		p.clicodigo,
		m.minano as ano_minuta,
		m.minnro as nro_minuta,
		m.minano as ano_processo,
		m.minnro as nro_processo,
		--m.pcsano as ano_processo,
		--m.pcsnro as nro_processo,
		null as numero_protocolo,
		null as ano_protocolo,
		p.pcsdataproc::varchar as data_processo,
		lic.licdatahomologacao::varchar as data_homologacao,
		'QUANTIDADE' as controle_saldo,
		false as previsao_subcontratacao,
		p.pcsano as parametro_exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'parametro-exercicio', p.pcsano))) as id_parametro_exercicio,
		p.loccodigo as local_entrega,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'local-entrega', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', p.clicodigo))), p.loccodigo))), 0) as id_local_entrega,
		m.mintipoobjeto as tipo_objeto,
		(case  -- Regra para garantir que RP seja sempre Compras e Serviços, caso contrário retornará erro na forma de contratação
			when (m.mintipoconcorrencia = 2 and m.mintipoobjeto not in (1, 2)) then 10 -- 10: Compras e Servicos
			else (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,'tipo-objeto', (case m.mintipoobjeto when 1 then 'Compras e Outros Serviços' when 2 then 'Obras e Serviços de Engenharia' when 3 then 'Concessoes e Permissões de Serviços Públicos' when 4 then 'Alienação de bens'  when 5 then 'Concessão e Permissão de Uso de Bem Público' when 6 then 'Aquisição de bens' when 7 then 'Contratação de Serviços' else 'Compras Outros Serviços' end))))
		end) as id_tipo_objeto,
	    concat(coalesce(m.mintipojulgamento, 1), '-', m.mintipocomparacao) as forma_julgamento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'forma-julgamento', 2016, coalesce(m.mintipojulgamento, 1), m.mintipocomparacao))) as id_forma_julgamento,
		e.edtpreventrmat as prazo_entrega,
	    coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'prazo-entrega', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', 2016))), upper(unaccent(left(coalesce(trim(e.edtpreventrmat),'Imediata'), 50)))))), 0) as id_prazo_entrega,
		e.edtcondpgto as forma_pagamento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'forma-pagamento', coalesce(upper(unaccent(trim(e.edtcondpgto))), 'Conforme Edital')))) as id_forma_pagamento,
		--3166 as id_forma_pagamento, -- 10 dias
		m.mintiporegexec as regime_execucao,
        coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'regime-execucao', (case m.mintiporegexec when 1 then 'Empreitada por Preço Global' when 2 then 'Empreitada por Preço Unitário' when 3 then 'Empreitada por Preço Global Integral' when 4 then 'Tarefa' when 5 then 'Execução Direta' when 6 then 'Cessão de Direitos' when 7 then 'Serviços' when 8 then 'Alienação de Bens Móveis' when 9 then 'Alienação de Bens Imóveis'  when 10 then 'Cessão de Direitos' when 11 then 'Cessão de Direito Real de Uso - Bens Públicos' else 'Compras' end)))), 0) id_regime_execucao,
		left(p.pcsfinalidade, 1500) as objeto,
		p.pcsobs as observacao,
		null as justificativa,
		false as destinatario_educacao,
		false as destinatario_saude,
		(case when p.modcodigo in (7, 8) then 'CONTRATACAO_DIRETA' else 'LICITACAO' end) as forma_contratacao,
		'CLASSIFICA' as desclassifica_proposta_invalida,
		(select (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) from wun.tbunico u where u.unicodigo = (select i.unicodigo from wco.tbintegrante i where i.cmlcodigo = m.cmlcodigo and i.mbcatribuicao in(3,6) limit 1)) as cpf_responsavel,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'responsavel', (select (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) from wun.tbunico u where u.unicodigo = (select i.unicodigo from wco.tbintegrante i where i.cmlcodigo = m.cmlcodigo and i.mbcatribuicao in(3,6) limit 1))))) as id_responsavel,
		m.cmlcodigo as cod_comissao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'comissao', m.cmlcodigo))) as id_comissao,
		p.modcodigo as cod_modalidade,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'modalidade-licitacao', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', p.clicodigo))), p.modcodigo, (case when p.modcodigo = 6 then 1 else null end)))) as id_modalidade,
		null as tipo_comissao_invalida,
		'NAO_APLICA' as desclassifica_proposta_invalida_lote,
		(case  when lic.licdatahomologacao is not null then 'PROPOSTA_FINAL_CONFIRMADA' else 'PENDENTE' end) as situacao_lances,
		(case when m.mintipoconcorrencia = 2 then true else false end) as registro_preco,
		(case when m.mintipoconcorrencia = 2 then true else false end) as possui_rp,
		null as data_autorizacao_rp,
		(case when e.edtdataentrprop is null then null else concat(e.edtdataentrprop, 'T', coalesce(e.edthoraentrprop, '00:00:00'), '.000Z') end)::varchar as dh_inicio_recebimento_envelopes,
		(case when e.edtdataentrprop is null then null else concat(e.edtdataentrprop, 'T', coalesce(e.edthoraentrprop, '00:00:00'), '.000Z') end)::varchar as dh_final_recebimento_envelopes,
		(case when e.edtdataentrprop is null then null else concat(e.edtdataentrprop, 'T', coalesce(e.edthoraentrprop, '00:00:00'), '.000Z') end)::varchar as dh_abertura_envelopes,
		false as exclusivo_mpe,
        false as beneficia_mpe_locais,
        false as indica_percent_cota_reservada,
        25 as percent_cota_reservada,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', m.clicodigo, m.minano, m.minnro))) as id_gerado
	from wco.tbminuta m
	left join wco.tbprocesso p on (p.clicodigo = m.clicodigo and p.pcsano = m.pcsano and p.pcsnro = m.pcsnro)
	left join wco.tblicitacao lic on (lic.clicodigo = m.clicodigo and lic.minano = m.minano and lic.minnro = m.minnro)
	left join wco.tbedital e on (e.clicodigo = m.clicodigo and e.minnro = m.minnro and e.minano = m.minano)
	where m.clicodigo = {{clicodigo}}
	and m.minano = {{ano}}
	and m.minnro in (81)
	order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_parametro_exercicio is not null
--limit 1
