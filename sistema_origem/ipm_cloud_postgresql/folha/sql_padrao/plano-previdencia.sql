select * from (select
				tpvcodigo  as id,
				tpvcodigo  as codigo,
                 tpvdescricao as descricao,
                 (case tpvdescricao
                           when 'Plano de Assistência Municipal'     then 'ASSISTENCIA'
                        else 'PREVIDENCIA'
                        end) as tipo,
                 (case tpvdescricao
                               when 'Plano de Previdência Federal SAÚDE'      then 'REGIME_GERAL_PREVIDENCIA_SOCIAL'
                               when 'Plano de Previdência Federal PREFEITURA' then 'REGIME_GERAL_PREVIDENCIA_SOCIAL'
                               when 'Fundo Financeiro Municipal'             then 'REGIME_GERAL_PREVIDENCIA_SOCIAL'
                               when 'Plano de Previdência Municipal'          then 'REGIME_PROPRIO_SERVIDORES_CIVIS'
                               when 'Plano de Previdência Estadual'           then 'REGIME_PROPRIO_SERVIDORES_CIVIS'
                               when 'Plano de Previdência Federal'            then 'REGIME_GERAL_PREVIDENCIA_SOCIAL'
                               when 'Piso Salarial Câmara'            		then 'REGIME_PROPRIO_SERVIDORES_CIVIS'        
                          end) as regime,
                 (case tpvdescricao
                               when 'Plano de Previdência Federal SAÚDE'      then 'FEDERAL'
                               when 'Plano de Previdência Federal PREFEITURA' then 'FEDERAL'
                               when 'Fundo Financeiro Municipal'             then 'FEDERAL'
                               when 'Plano de Previdência Municipal'          then 'MUNICIPAL'
                               when 'Plano de Previdência Estadual'           then 'ESTADUAL'
                               when 'Plano de Previdência Federal'            then 'FEDERAL'
                               when 'Piso Salarial Câmara'            		then 'MUNICIPAL'                               
                          end)as ambitoRegime,
                 null as observacao,
                 'CRIADO' as situacao,
                   date('1900-01-01') as dataAlteracao
            from wfp.tbprevidencia
            where odomesano = '202012') as a
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'plano-previdencia', codigo))) is null            
-- where public.bth_get_situacao_registro('300', 'plano-previdencia', cast(a.codigo as varchar)) in (0)
            