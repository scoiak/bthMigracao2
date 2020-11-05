-- Antes de migrar, criar campos adicionais de cargo para o e-Sfinge
-- create index idx_cargos on wfp.tbcargo (carcodigo, odomesano);
select
	'300' as sistema,
	'cargo' as tipo_registro,
	codigo as chave_dsk1,
	(select string_agg(concat(hist_codigo, hist_descricao, hist_inicio_vigencia, hist_decimo_terceiro, hist_contagem_esp, hist_acumulo_cargos, hist_pcd, hist_extinto, hist_grau_inst, hist_situacao_grau_inst), '%/%') from (
		select distinct
			concat(c.carcodigo, ';') as hist_codigo, -- Código
			concat(c.carcodigo, ' - ', c.cardescricao, ';') as hist_descricao, -- descricao
			concat(c.cardatacriacao, ';') as hist_inicio_vigencia, -- inicioVigencia
			concat(true, ';') as hist_decimo_terceiro, -- pagaDecimoTerceiroSalario
			concat(true, ';') as hist_contagem_esp, -- contagemEspecial
			concat('NAO_ACUMULAVEL', ';') as hist_acumulo_cargos, -- acumuloCargos
			concat(0, ';') as hist_pcd, -- quantidadeVagasPcd
			concat('', ';') as hist_extinto, -- extinto
			concat((case c.gincodigo
				        when 2 then 'NAO_ALFABETIZADO'
				        when 3 then 'ENSINO_FUNDAMENTAL_ANOS_INICIAIS'
				        when 4 then 'ENSINO_FUNDAMENTAL_ANOS_FINAIS'
				        when 5 then 'ENSINO_FUNDAMENTAL_ANOS_FINAIS'
				        when 6 then 'ENSINO_MEDIO'
				        when 7 then 'ENSINO_MEDIO'
				        when 8 then 'ENSINO_SUPERIOR_SEQUENCIAL'
				        when 9 then 'ENSINO_SUPERIOR_SEQUENCIAL'
				        when 10 then 'POS_GRADUACAO_ESPECIALIZACAO'
				        when 11 then 'POS_GRADUACAO_MBA'
				        when 12 then 'POS_GRADUACAO_MESTRADO'
				        when 13 then 'POS_GRADUACAO_DOUTORADO'
				        when 14 then 'POS_DOUTORADO_HABILITACAO'
				    else '' end), ';') as hist_grau_inst, -- grauInstrucao
			concat('', ';') as hist_situacao_grau_inst -- situacaoGrauInstrucao
			from wfp.tbcargo c
			where c.carcodigo = codigo
	) tab) as historico,
	*
from (
	select distinct
		1 as id,
		cargo.carcodigo as codigo,
		concat(cargo.carcodigo, ' - ', cargo.cardescricao) as descricao,
		--(select c.txjcodigocri from wfp.tbcargo c where c.carcodigo = cargo.carcodigo and cardatacriacao is not null order by c.odomesano limit 1) as id_ato_criacao,
		coalesce((select c.cardatacriacao from wfp.tbcargo c where c.carcodigo = cargo.carcodigo and cardatacriacao is not null order by c.odomesano limit 1), '2020-01-01') as inicioVigencia,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cbo', cargo.cbocodigo))),0) as id_cbo,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-cargo', cargo.cartipocargo))), 0) as id_tipo_cargo,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = cargo.txjcodigocri), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = cargo.txjcodigocri limit 1)))),0) as id_ato,
		'MENSALISTA' as unidadePagamento,
		'NAO_ACUMULAVEL' as acumuloCargos,
		cargo.carvagas as quantidadeVagas,
	    case cargo.gincodigo
	        when 2 then 'NAO_ALFABETIZADO'
	        when 3 then 'ENSINO_FUNDAMENTAL_ANOS_INICIAIS'
	        when 4 then 'ENSINO_FUNDAMENTAL_ANOS_FINAIS'
	        when 5 then 'ENSINO_FUNDAMENTAL_ANOS_FINAIS'
	        when 6 then 'ENSINO_MEDIO'
	        when 7 then 'ENSINO_MEDIO'
	        when 8 then 'ENSINO_SUPERIOR_SEQUENCIAL'
	        when 9 then 'ENSINO_SUPERIOR_SEQUENCIAL'
	        when 10 then 'POS_GRADUACAO_ESPECIALIZACAO'
	        when 11 then 'POS_GRADUACAO_MBA'
	        when 12 then 'POS_GRADUACAO_MESTRADO'
	        when 13 then 'POS_GRADUACAO_DOUTORADO'
	        when 14 then 'POS_DOUTORADO_HABILITACAO'
	    else '' end as grauInstrucao,
	    case cargo.gincodigo
	        when 2 then 'INCOMPLETO'
	        when 3 then 'INCOMPLETO'
	        when 4 then 'INCOMPLETO'
	        when 5 then 'INCOMPLETO'
	        when 6 then 'INCOMPLETO'
	        when 7 then 'INCOMPLETO'
	    else 'COMPLETO' end as situacaoGrauInstrucao,
		false as dedicacaoExclusiva,
		'NAO' as contagemEspecial,
		true as pagaDecimoTerceiroSalario,
		0 as quantidadeVagasPcd,
		1 as tceTipoQuadro,
		cargo.carcodigo as tceCodCargo,
		99 as tceTipoCargoAcu,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cargo', cargo.carcodigo))), 0) as id_gerado
	from wfp.tbcargo cargo
	where caremdesuso = 0
	and txjcodigocri is not null
	--and carcodigo = 1
	and cbocodigo is not null
	order by codigo
) tab
where id_gerado = 0
limit 5