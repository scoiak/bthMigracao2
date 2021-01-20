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
	--coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = nv.txjcodigo), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = nv.txjcodigo limit 1)))), 0) as ato,
	/* 0 */coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = nv.txjcodigo), (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','tipo-ato', entidade, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(upper(left((select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = nv.txjcodigo limit 1), 40)),'é','É'),'á','Á'),'ó','Ó'),'ã','Ã'),'õ','Õ'),'ç','Ç'),'â','Â'),'à','À'),'í','Í'),'ê','Ê'))))))),0) as ato,
	/* 1 */coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cbo', cbocodigo))),0) as cbo,
	/* 2 */coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-cargo', entidade, cartipocargo))), 0) as tipoCargo,
	--/*  */coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = nv.txjcodigocri), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = nv.txjcodigocri limit 1)))),0) as atoCriacao,
	/* 3 coalesce((case cartemferias when 1 then (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-ferias',entidade,1))) else 0 end), 0) as configuracaoFerias,*/
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-ferias',entidade,1))) as configuracaoFerias,	
	*
into temporary table temp_cargos
from (
	select
		/* 4 */nv.cnidatarelaciona,
		/* 5 */coalesce(nv.txjcodigo,0) as txjcodigo,
		/* 6 */nv.carcodigo as carcodigo,
		/* 7 */c.odomesano,
		--concat(c.carcodigo, ' - ', c.cardescricao) as descricao,
		/* 8 */ regexp_replace(concat(c.carcodigo, ' - ', c.cardescricao), '\,', '', 'gi') as descricao,
		/* 9 */nv.cnidatarelaciona as inicioVigencia,
		/* 10 */c.cbocodigo as cbocodigo,
		/* 11 */c.cartipocargo as cartipocargo,
		/* 12 */c.cartemferias as cartemferias,
		/* 13 */'MENSALISTA' as unidadePagamento,
		/* 14 */'NAO_ACUMULAVEL' as acumuloCargos,
		/* 15 */coalesce(c.carvagas, 0) as quantidadeVagas,
		/* 16 */coalesce(c.txjcodigocri,0) as txjcodigocri,
		/* 17 */case c.gincodigo
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
	    /* 18 */case c.gincodigo
	        when 1 then ''
	        when 2 then ''
	        when 3 then 'INCOMPLETO'
	        when 4 then 'INCOMPLETO'
	        when 5 then 'INCOMPLETO'
	        when 6 then 'INCOMPLETO'
	        when 7 then 'INCOMPLETO'
	    else 'COMPLETO' end as situacaoGrauInstrucao,
	    /* 19 */'false' as dedicacaoExclusiva,
		/* 20 */'NAO' as contagemEspecial,
		/* 21 */'true' as pagaDecimoTerceiroSalario,
		/* 22 */0 as quantidadeVagasPcd,
		/* 23 */'1' as tceTipoQuadro,
		/* 24 */c.carcodigo::varchar as tceCodCargo,
		/* 25 */'99' as tceTipoCargoAcu,
		/* 26 */ (case coalesce(c.txjcodigoext, 0) when 0 then 'false' else 'true' end) as extinto,
		/* 27 */ regexp_replace((array(select distinct unnest(string_to_array((
			select
				string_agg(concat(nv2.nivcodigo, ': ', coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))), nv2.nivcodigo)))::text, '?')), ', ')
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
		), ', ')) order by 1)::text), '[\,]', '|', 'gi')::text as nivelSalarial,
		clicodigo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', c.clicodigo))) as entidade
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
	row_number() over() as id,
	array(select distinct unnest(string_to_array((
			select
				string_agg(concat(n_nivcodigo, ':', coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', entidade, n_nivcodigo))),0)), ', ')
			from (
				select
					nv2.carcodigo as n_carcodigo,
					nv2.nivcodigo as n_nivcodigo
					--string_agg(concat(nv2.nivcodigo, ': ', coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', id_entidade, nv2.nivcodigo)))::text, '?')), ', ')
				from wfp.tbcargonivel nv2
				where nv2.carcodigo = cargos.codigo
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
		), ', ')) order by 1) nivelSalarial,
	regexp_replace((select string_agg(string_to_array(h.*::text,';')::text, '%/%') as hist from temp_cargos h where h.carcodigo = cargos.codigo), '[\"\\\{\}\(\)]', '', 'gi') as historico,
	cargos.*
from (
	select
		c.carcodigo as codigo,
		regexp_replace(concat(c.carcodigo, ' - ', c.cardescricao), '\,', '', 'gi') as descricao,
		(case coalesce(c.txjcodigoext, 0) when 0 then false else true end) as extinto,
		--c.txjcodigoext as atoExtincao,
		/*
		(case coalesce(c.txjcodigoext, 0) when 0 then 0
		else
			coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = c.txjcodigoext), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = c.txjcodigoext limit 1)))),0)
		end) as id_ato_extincao,
		*/
		--coalesce((select c2.cardatacriacao from wfp.tbcargo c2 where c2.carcodigo = c.carcodigo and c2.cardatacriacao is not null order by c2.odomesano limit 1), '2020-01-01') as inicioVigencia,
		coalesce((select max(nv.cnidatarelaciona) from wfp.tbcargonivel nv where nv.carcodigo = c.carcodigo), '2020-01-01') as inicioVigencia,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cbo', c.cbocodigo))),(select max(id_gerado) from public.controle_migracao_registro where tipo_registro = 'cbo' and id_gerado is not null))::varchar as cbo,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-cargo', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', c.clicodigo))) , c.cartipocargo))), 0) as tipoCargo,
		--(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = c.txjcodigocri), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = c.txjcodigocri limit 1)))) as ato,		
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = c.txjcodigocri), (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','tipo-ato', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', c.clicodigo))), replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(upper(left((select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = c.txjcodigocri limit 1), 40)),'é','É'),'á','Á'),'ó','Ó'),'ã','Ã'),'õ','Õ'),'ç','Ç'),'â','Â'),'à','À'),'í','Í'),'ê','Ê')))))))::varchar as ato,
		c.cartemferias,
		/*
		(case c.cartemferias
			when 1 then (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-ferias',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))),1)))
		else null end)::varchar as configuracaoFerias,
		*/
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-ferias',(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))),1)))::varchar as configuracaoFerias,
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
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cargo', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))) , c.carcodigo))) as idcloud,		
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))) as entidade
	from wfp.tbcargo c
	where c.odomesano = 202012
	--and c.carcodigo in (477, 394, 440, 523, 2, 381, 779, 438, 778, 507, 3, 471, 312, 494, 194, 476, 439, 332, 380, 436, 755, 767, 230, 517, 178, 399, 112, 119, 430, 460)
	--and c.carcodigo = 7
	order by c.carcodigo
) cargos
where cargos.idcloud is null;
--limit 5
