select
    '300' as sistema,
    'cargo' as tipo_registro,
    clicodigo as chave_dsk1,
    carcodigo as chave_dsk2,
    case id_tipo_cargo
    	when 1443 then 449
        when 1442 then 449
        else null
    end as configuracao_ferias,
    *
from (
 select distinct
 		1 as id,
        cargo.clicodigo,
        '2000-01-01' as maiorVigenciaRemun,
        coalesce(cargo.cardatainivenc, '2020-01-01') as inicioVigencia,
        cargo.carcodigo,
        cargo.carcodigo || ' - ' || cargo.cardescricao as descricao,
        '1' as codigoESocial,
        (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = cargo.txjcodigocri) as numero_ato,
        (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = cargo.txjcodigocri limit 1) as tipo_ato,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = cargo.txjcodigocri), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = cargo.txjcodigocri limit 1)))) as id_ato,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-cargo', cargo.cartipocargo))) as id_tipo_cargo,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cbo', cargo.cbocodigo))) as id_cbo,
        'true' as pagaDecimoTerceiroSalario,
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
                            when 4 then 'INCOMPLETO'
                            when 6 then 'INCOMPLETO'
                            when 8 then 'INCOMPLETO'
            else 'COMPLETO' end as situacaoGrauInstrucao,

        'NAO' as contagemEspecial,
        'MENSALISTA' as unidadePagamento,
        'NAO_ACUMULAVEL' as acumuloCargos,
        false as dedicacaoExclusiva,
        null as quadroCargos,
        cargo.carvagas as quantidadeVagas,
        0 as quantidadeVagasPcd,
        null as requisitosNecessarios,
        null as atividadesDesempenhadas,
        null as extinto,
        null as configuracaoLicencaPremio,
        coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'cargos', cargo.clicodigo, cargo.carcodigo))),0) as situacao_registro
    from wfp.tbcargo cargo
    where caremdesuso = 0
    and cbocodigo is not null
    limit 5
) tab
where situacao_registro = 0
order by descricao