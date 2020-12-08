-- Antes de migrar, criar campos adicionais de cargo para o e-Sfinge e configurar no modulo de envio
--  5fa6dc8bed9eb40104372a23 -> TIPO DE QUADRO
--  5fa6dc8bed9eb40104372a24 -> CODIGO TCE
--  5fa6dc8bed9eb40104372a25 -> TICO CARGO ACUMULO
-- Configurar ID padrão da configuração de férias
-- create index idx_cargos on wfp.tbcargo (carcodigo, odomesano);

-- Dropa a tabela temporária caso ela já esteja instanciada
drop table if exists temp_cargos;

-- Gera a tabela temporária atualizada com os dados do histórico
select
	/* 0 */ '300' as sistema,
	/* 1 */'cargo' as tipo_registro,
	/* 2 */coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = nv.txjcodigo), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = nv.txjcodigo limit 1)))), 0) as id_ato,
	/* 3 */coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cbo', cbo))),0) as id_cbo,
	/* 4 */coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-cargo', id_entidade, cartipocargo))), 0) as id_tipo_cargo,
	/* 5 */coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = nv.txjcodigocri), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = nv.txjcodigocri limit 1)))),0) as id_ato_criacao,
	/* 6 */coalesce((case cartemferias when 1 then 492 else null end), 0) as id_conf_ferias,
	*
into temporary table temp_cargos
from (
	select
		/* 7 */nv.cnidatarelaciona,
		/* 8 */nv.txjcodigo,
		/* 9 */nv.carcodigo as carcodigo,
		/* 10 */c.odomesano,
		--concat(c.carcodigo, ' - ', c.cardescricao) as descricao,
		/* 11 */ regexp_replace(concat(c.carcodigo, ' - ', c.cardescricao), '\,', '', 'gi') as descricao,
		/* 12 */nv.cnidatarelaciona as inicio_vigencia,
		/* 13 */c.cbocodigo as cbo,
		/* 14 */c.cartipocargo as cartipocargo,
		/* 15 */c.cartemferias as cartemferias,
		/* 16 */'MENSALISTA' as unidadePagamento,
		/* 17 */'NAO_ACUMULAVEL' as acumuloCargos,
		/* 18 */coalesce(c.carvagas, 0) as quantidadeVagas,
		/* 19 */c.txjcodigocri as txjcodigocri,
		/* 20 */case c.gincodigo
		    when 1 then null
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
	    /* 21 */case c.gincodigo
	        when 1 then null
	        when 2 then null
	        when 3 then 'INCOMPLETO'
	        when 4 then 'INCOMPLETO'
	        when 5 then 'INCOMPLETO'
	        when 6 then 'INCOMPLETO'
	        when 7 then 'INCOMPLETO'
	    else 'COMPLETO' end as situacaoGrauInstrucao,
	    /* 22 */false as dedicacaoExclusiva,
		/* 23 */'NAO' as contagemEspecial,
		/* 24 */true as pagaDecimoTerceiroSalario,
		/* 25 */0 as quantidadeVagasPcd,
		/* 26 */'1' as tceTipoQuadro,
		/* 27 */c.carcodigo::varchar as tceCodCargo,
		/* 28 */'99' as tceTipoCargoAcu,
		/* 39 */ (case coalesce(c.txjcodigoext, 0) when 0 then false else true end) as extinto,
		/* 30 */ regexp_replace((array(select distinct unnest(string_to_array((
			select
				string_agg(concat(nv2.nivcodigo, ': ', coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))), nv2.nivcodigo)))::text, '?')), ', ')
			from wfp.tbcargonivel nv2
			where nv2.carcodigo = nv.carcodigo
			and nv2.cnidatarelaciona < nv.cnidatarelaciona
			and not exists (
				select 1
				from wfp.tbcargonivel nv3
				where nv3.carcodigo = nv2.carcodigo
				and nv3.cnidatarelaciona < nv.cnidatarelaciona
				and nv3.nivcodigo = nv2.nivcodigo
				and nv3.cnitipoatualiza = 2
			)
			group by nv2.carcodigo
		), ', ')) order by 1)::text), '[\,]', '|', 'gi')::text as niveis_vigentes,
		clicodigo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', c.clicodigo))) as id_entidade
	from (
		select
			nv.carcodigo,
			nv.cnidatarelaciona,
			nv.txjcodigo
		from wfp.tbcargonivel nv
		--where nv.carcodigo in (477, 394, 440, 523, 2, 381, 779, 438, 778, 507, 3, 471, 312, 494, 194, 476, 439, 332, 380, 436, 755, 767, 230, 517, 178, 399, 112, 119, 430, 460)
		--where nv.carcodigo = 7
		group by 1, 2, 3
		order by 1, 2
	) as nv
	inner join wfp.tbcargo c on c.carcodigo = nv.carcodigo and c.odomesano = left(regexp_replace(nv.cnidatarelaciona::varchar, '[\-]', '', 'gi'), 6)::integer
	order by nv.carcodigo asc, nv.cnidatarelaciona asc
) nv;

-- Executa consulta com os dados a enviar
select
	'300' as sistema,
	'cargo' as tipo_registro,
	1 as id,
	id_entidade as chave_dsk1,
	carcodigo as chave_dsk2,
	array(select distinct unnest(string_to_array((
			select
				string_agg(concat(n_nivcodigo, ':', coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', id_entidade, n_nivcodigo))))), ', ')
			from (
				select
					nv2.carcodigo as n_carcodigo,
					nv2.nivcodigo as n_nivcodigo
					--string_agg(concat(nv2.nivcodigo, ': ', coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', id_entidade, nv2.nivcodigo)))::text, '?')), ', ')
				from wfp.tbcargonivel nv2
				where nv2.carcodigo = cargos.carcodigo
				and not exists (
					select 1
					from wfp.tbcargonivel nv3
					where nv3.carcodigo = nv2.carcodigo
					and nv3.nivcodigo = nv2.nivcodigo
					and nv3.cnitipoatualiza = 2
					and nv3.cnidatarelaciona >= nv2.cnidatarelaciona
				)
				group by nv2.carcodigo, nv2.nivcodigo
				order by nv2.carcodigo, nv2.nivcodigo
			) n
		), ', ')) order by 1) niveis_vigentes,
	regexp_replace((select string_agg(string_to_array(h.*::text,';')::text, '%/%') as hist from temp_cargos h where h.carcodigo = cargos.carcodigo), '[\"\\\{\}\(\)]', '', 'gi') as historico,
	cargos.*
from (
	select
		c.carcodigo,
		regexp_replace(concat(c.carcodigo, ' - ', c.cardescricao), '\,', '', 'gi') as descricao,
		(case coalesce(c.txjcodigoext, 0) when 0 then false else true end) as extinto,
		c.txjcodigoext as ato_extincao,
		(case coalesce(c.txjcodigoext, 0) when 0 then 0
		else
			coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = c.txjcodigoext), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = c.txjcodigoext limit 1)))),0)
		end) as id_ato_extincao,
		--coalesce((select c2.cardatacriacao from wfp.tbcargo c2 where c2.carcodigo = c.carcodigo and c2.cardatacriacao is not null order by c2.odomesano limit 1), '2020-01-01') as inicioVigencia,
		coalesce((select max(nv.cnidatarelaciona) from wfp.tbcargonivel nv where nv.carcodigo = c.carcodigo), '2020-01-01') as inicioVigencia,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cbo', c.cbocodigo))),0) as id_cbo,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-cargo', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', c.clicodigo))) , c.cartipocargo))), 0) as id_tipo_cargo,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = c.txjcodigocri), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = c.txjcodigocri limit 1)))),0) as id_ato,
		c.cartemferias,
		(case c.cartemferias
			when 1 then 492
		else null end) as id_conf_ferias,
		'MENSALISTA' as unidadePagamento,
		'NAO_ACUMULAVEL' as acumuloCargos,
		coalesce(c.carvagas, 0) as quantidadeVagas,
	    case c.gincodigo
	        when 1 then null
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
	    case c.gincodigo
	        when 1 then null
	        when 2 then null
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
		'1' as tceTipoQuadro,
		c.carcodigo::varchar as tceCodCargo,
		'99' as tceTipoCargoAcu,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cargo', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))) , c.carcodigo))), 0) as id_gerado,
		c.clicodigo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))) as id_entidade
	from wfp.tbcargo c
	where c.odomesano = 202010
	--and c.carcodigo in (477, 394, 440, 523, 2, 381, 779, 438, 778, 507, 3, 471, 312, 494, 194, 476, 439, 332, 380, 436, 755, 767, 230, 517, 178, 399, 112, 119, 430, 460)
	--and c.carcodigo = 7
	order by c.carcodigo
) cargos
where cargos.id_gerado = 0
--limit 5





