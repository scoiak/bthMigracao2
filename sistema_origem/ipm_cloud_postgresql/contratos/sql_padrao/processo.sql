select
	row_number() over() as id,
	'305' as sistema,
	'processo' as tipo_registro,
	concat(nr_processo, '/', ano_processo) as proc_formatado,
	concat(numero_protocolo, '/', ano_protocolo) as lic_formatado,
	*
from (
	select
		p.clicodigo as clicodigo,
		p.pcsano as ano_processo,
		p.pcsnro as nr_processo,
		p.pcsdataproc::varchar as data_processo,
		p.pcsnro as numero_protocolo,
		p.pcsano as ano_protocolo,
		'QUANTIDADE' as controle_saldo,
		false as previsao_subcontratacao,
		null as data_homologacao,
		pcsano as parametro_exercicio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'parametro-exercicio', pcsano))) as id_parametro_exercicio,
		l.locdescricao as local_entrega,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'local-entrega', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', p.clicodigo))), l.loccodigo))) as id_local_entrega,
		m.mintipoobjeto as tipo_objeto,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,'tipo-objeto', (case m.mintipoobjeto when 1 then 'Compras e Outros Serviços' when 2 then 'Obras e Serviços de Engenharia' when 3 then 'Concessoes e Permissões de Serviços Públicos' when 4 then 'Alienação de bens'  when 5 then 'Concessão e Permissão de Uso de Bem Público' when 6 then 'Aquisição de bens' when 7 then 'Contratação de Serviços' else 'Compras Outros Serviços' end)))) as id_tipo_objeto,
		m.mintipojulgamento as forma_julgamento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'forma-julgamento', m.clicodigo, coalesce(m.mintipojulgamento, 1), m.mintipocomparacao))) as id_forma_julgamento,
		e.edtpreventrmat as prazo_entrega,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'prazo-entrega', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', p.clicodigo))), upper(unaccent(left(coalesce(trim(e.edtpreventrmat),'Imediata'), 50)))))) as id_prazo_entrega,
		e.edtcondpgto as forma_pagamento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'forma-pagamento', coalesce(upper(unaccent(trim(e.edtcondpgto))), 'Conforme Edital')))) as id_forma_pagamento,
		m.mintiporegexec as regime_execucao,
        coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'regime-execucao', (case m.mintiporegexec when 1 then 'Empreitada por Preço Global' when 2 then 'Empreitada por Preço Unitário' when 3 then 'Empreitada por Preço Global Integral' when 4 then 'Tarefa' when 5 then 'Execução Direta' when 6 then 'Cessão de Direitos' when 7 then 'Serviços' when 8 then 'Alienação de Bens Móveis' when 9 then 'Alienação de Bens Imóveis'  when 10 then 'Cessão de Direitos' when 11 then 'Cessão de Direito Real de Uso - Bens Públicos' else 'Compras' end)))), 0) id_regime_execucao,
		p.pcsfinalidade as objeto,
		p.pcsjustificativa as justificativa,
		p.pcsobs as observacao,
		false as destinatario_educacao,
		false as destinatario_saude,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', p.clicodigo, p.pcsano, p.pcsnro))) as id_gerado
	from wco.tbprocesso p
	natural join wco.tbminuta m
	left join wco.tblocalentrega l on (l.loccodigo = p.loccodigo)
	left join wco.tbedital e on (e.clicodigo = m.clicodigo and e.minnro = m.minnro and e.minano = m.minano)
	where p.clicodigo = {{clicodigo}}
	and p.pcsano >= {{ano}}
	order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_parametro_exercicio is not null
limit 10