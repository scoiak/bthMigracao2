select 
		row_number() over() as id,
		row_number() over() as codigo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
		* from ( select            
            'PROVISÃO - BASE AUXILIAR DISTORÇÕES 13º SALÁRIO' AS descricao,
            'PRBAAUDI13SA' AS sigla,
            NULL AS classificacaoBaseCalculo           
        union all
		select            
            'PAGA PROPORCIONAL' as descricao,
            'PAGAPROP' as sigla,
            'PAGA_PROPORCIONAL' as classificacaoBaseCalculo            
        union all
        select        
            'SALÁRIO BASE' as descricao,
            'SALBASE' as sigla,
            'SALARIO_BASE' as classificacaoBaseCalculo
        union all
        select        
            'HORAS EXTRAS' as descricao,
            'HORAEXTRA' as sigla,
            'HORAS_EXTRAS' as classificacaoBaseCalculo
        union all
        select        
            'PERICULOSIDADE' as descricao,
            'PERIC' as sigla,
            'PERICULOSIDADE' as classificacaoBaseCalculo
        union all
        select        
            'SINDICATO' as descricao,
            'SIND' as sigla,
            'SINDICATO' as classificacaoBaseCalculo
        union all
        select        
            'F.G.T.S.' as descricao,
            'FGTS' as sigla,
            'FGTS' as classificacaoBaseCalculo
        union all
        select        
            'F.G.T.S. 13º SALÁRIO' as descricao,
            'FGTS13' as sigla,
            'FGTS_13_SALARIO' as classificacaoBaseCalculo
        union all
        select        
            'I.R.R.F.' as descricao,
            'IRRF' as sigla,
            'IRRF' as classificacaoBaseCalculo
        union all
        select        
            'I.R.R.F. 13º SALÁRIO' as descricao,
            'IRRF13' as sigla,
            'IRRF_13_SALARIO' as classificacaoBaseCalculo
        union all
        select        
            'I.R.R.F. FÉRIAS RESCISÃO' as descricao,
            'IRRFFERRESC' as sigla,
            'IRRF_FERIAS_RESCISAO' as classificacaoBaseCalculo
        union all
        select        
            'I.N.S.S.' as descricao,
            'INSS' as sigla,
            'INSS' as classificacaoBaseCalculo
        union all
        select        
            'I.N.S.S. 13º SALÁRIO' as descricao,
            'INSS13' as sigla,
            'INSS_13_SALARIO' as classificacaoBaseCalculo
        union all
        select        
            'PREV. ESTADUAL' as descricao,
            'PREVEST' as sigla,
            'PREV_ESTADUAL' as classificacaoBaseCalculo
        union all
        select        
            'PREV. ESTADUAL 13º SALÁRIO' as descricao,
            'PREVEST13' as sigla,
            'PREV_ESTADUAL_13_SALARIO' as classificacaoBaseCalculo
        union all
        select        
            'FUNDO ASSISTÊNCIA' as descricao,
            'FUNDASS' as sigla,
            'FUNDO_ASSISTENCIA' as classificacaoBaseCalculo
        union all
        select        
            'FUNDO ASSISTÊNCIA 13º SALÁRIO' as descricao,
            'FUNDASS13' as sigla,
            'FUNDO_ASSISTENCIA_13_SALARIO' as classificacaoBaseCalculo
        union all
        select        
            'FUNDO PREVIDÊNCIA' as descricao,
            'FUNDOPREV' as sigla,
            'FUNDO_PREVIDENCIA' as classificacaoBaseCalculo
        union all
        select        
            'FUNDO PREVIDÊNCIA 13º SALÁRIO' as descricao,
            'FUNDPREV13' as sigla,
            'FUNDO_PREVIDENCIA_13_SALARIO' as classificacaoBaseCalculo
        union all
        select        
            'OUTRAS BASES' as descricao,
            'OUTRASBASES' as sigla,
            'OUTRAS_BASES' as classificacaoBaseCalculo
        union all
        select        
            'F.G.T.S. AVISO PRÉVIO' as descricao,
            'FGTSAVISO' as sigla,
            'FGTS_AVISO_PREVIO' as classificacaoBaseCalculo
        union all
        select        
            'ABATIMENTO I.R.R.F.' as descricao,
            'ABATIRRF' as sigla,
            'ABATIMENTO_IRRF' as classificacaoBaseCalculo
        union all
        select        
            'ABATIMENTO I.R.R.F. 13º SAL.' as descricao,
            'ABATIRRF13' as sigla,
            'ABATIMENTO_IRRF_13_SALARIO' as classificacaoBaseCalculo
        union all
        select        
            'DESCONTO I.R.R.F.' as descricao,
            'DESCIRRF' as sigla,
            'DESCONTO_IRRF' as classificacaoBaseCalculo
        union all
        select        
            'DESCONTO I.R.R.F. 13º SALÁRIO' as descricao,
            'DESCIRRF13' as sigla,
            'DESCONTO_IRRF_13_SALARIO' as classificacaoBaseCalculo
        union all
        select        
            'DESCONTO I.R.R.F. FÉRIAS RESC.' as descricao,
            'DESCIRRFERES' as sigla,
            'DESCONTO_IRRF_FERIAS_RESC' as classificacaoBaseCalculo
        union all
        select        
            'EXCEDENTE I.N.S.S.' as descricao,
            'EXCEINSS' as sigla,
            'EXCEDENTE_INSS' as classificacaoBaseCalculo
        union all
        select        
            'EXCEDENTE I.N.S.S. 13º SALÁRIO' as descricao,
            'EXCEINSS13' as sigla,
            'EXCEDENTE_INSS_13_SALARIO' as classificacaoBaseCalculo
        union all
        select        
            'ABATIMENTO I.N.S.S.' as descricao,
            'ABATINSS' as sigla,
            'ABATIMENTO_INSS' as classificacaoBaseCalculo
        union all
        select        
            'DESCONTO 1/3 DE FÉRIAS' as descricao,
            'DESCTERFER' as sigla,
            'DESCONTO_UM_TERCO_DE_FERIAS' as classificacaoBaseCalculo
        union all
        select        
            'SALÁRIO FAMÍLIA NORMAL' as descricao,
            'SALAFAM' as sigla,
            'SALARIO_FAMILIA_NORMAL' as classificacaoBaseCalculo
        union all
        select        
            'I.N.S.S. OUTRAS EMPRESAS' as descricao,
            'INSSOUTRA' as sigla,
            'INSS_OUTRAS_EMPRESAS' as classificacaoBaseCalculo
        union all
        select        
            'I.N.S.S. OUTRAS EMP. 13º SAL' as descricao,
            'INSSOUTRA13' as sigla,
            'INSS_OUTRAS_EMP_13_SAL' as classificacaoBaseCalculo
        union all
        select        
            'I.R.R.F. OUTRAS EMPRESAS' as descricao,
            'IRRFOUTRA' as sigla,
            'IRRF_OUTRAS_EMPRESAS' as classificacaoBaseCalculo
        union all
        select        
            'I.R.R.F. OUTRAS EMP. 13º SAL' as descricao,
            'IRRFOUTRA13' as sigla,
            'IRRF_OUTRAS_EMP_13_SAL' as classificacaoBaseCalculo
        union all
        select        
            'I.R.R.F. FÉRIAS' as descricao,
            'IRRFFER' as sigla,
            'IRRF_FERIAS' as classificacaoBaseCalculo
        union all
        select        
            'CONTRIB. SINDICAL ANUAL' as descricao,
            'CONTSIND' as sigla,
            'CONTRIB_SINDICAL_ANUAL' as classificacaoBaseCalculo
        union all
        select        
            'MÉDIA AUXÍLIO MATERNIDADE' as descricao,
            'MEDIAUXMAT' as sigla,
            'MEDIA_AUXILIO_MATERNIDADE' as classificacaoBaseCalculo
        union all
        select        
            'DESC. 13º SALÁRIO REINTEGRAÇÃO' as descricao,
            'DESC13REINT' as sigla,
            'DESC_13_SALARIO_REINTEGRACAO' as classificacaoBaseCalculo
        union all
        select        
            'COMPÕEM HORAS' as descricao,
            'COMPHORAMES' as sigla,
            'COMPOEM_HORAS_DO_MES' as classificacaoBaseCalculo
        union all
        select        
            '1/3 FÉRIAS VENCIDAS RESC.' as descricao,
            'TERFERVENRES' as sigla,
            'UM_TERCO_FERIAS_VENCIDAS_RESC' as classificacaoBaseCalculo
        union all
        select        
            'FUNDO FINANCEIRO' as descricao,
            'FUNDFIN' as sigla,
            'FUNDO_FINANCEIRO' as classificacaoBaseCalculo
        union all
        select        
            'FUNDO FINANCEIRO 13º SALÁRIO' as descricao,
            'FUNDFIN13' as sigla,
            'FUNDO_FINANCEIRO_13_SALARIO' as classificacaoBaseCalculo
        union all
        select        
            'MÉDIA AUXÍLIO MATERNIDADE PROPORCIONAL' as descricao,
            'MEDAUXMATPR' as sigla,
            'MEDIA_AUXILIO_MATERNIDADE_PROPORCIONAL' as classificacaoBaseCalculo
        union all
        select        
            'I.N.S.S. 13º SALÁRIO DISTORÇÃO' as descricao,
            'IN13SADI' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO PREVIDÊNCIA 13º SALÁRIO PROVISÃO' as descricao,
            'FUPR13SAPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO PREVIDÊNCIA 13º SALÁRIO AJUSTE PROVISÃO' as descricao,
            'FUPR13SAAJPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO PREVIDÊNCIA 13º SALÁRIO ESTORNO PROVISÃO' as descricao,
            'FUPR13SAESPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO PREVIDÊNCIA 13º SALÁRIO DISTORÇÃO' as descricao,
            'FUPR13SADI' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'PREV. ESTADUAL 13º SALÁRIO PROVISÃO' as descricao,
            'PRES13SAPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'PREV. ESTADUAL 13º SALÁRIO AJUSTE PROVISÃO' as descricao,
            'PRES13SAAJPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'PREV. ESTADUAL 13º SALÁRIO ESTORNO PROVISÃO' as descricao,
            'PRES13SAESPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'PREV. ESTADUAL 13º SALÁRIO DISTORÇÃO' as descricao,
            'PRES13SADI' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO ASSISTÊNCIA 13º SALÁRIO PROVISÃO' as descricao,
            'FUAS13SAPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO ASSISTÊNCIA 13º SALÁRIO AJUSTE PROVISÃO' as descricao,
            'FUAS13SAAJPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO ASSISTÊNCIA 13º SALÁRIO ESTORNO PROVISÃO' as descricao,
            'FUAS13SAESPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO ASSISTÊNCIA 13º SALÁRIO DISTORÇÃO' as descricao,
            'FUAS13SADI' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'F.G.T.S. 13º SALÁRIO PROVISÃO' as descricao,
            'FG13SAPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'F.G.T.S. 13º SALÁRIO AJUSTE PROVISÃO' as descricao,
            'FG13SAAJPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'F.G.T.S. 13º SALÁRIO ESTORNO PROVISÃO' as descricao,
            'FG13SAESPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'I.N.S.S. FÉRIAS PROVISÃO' as descricao,
            'INFEPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'I.N.S.S. FÉRIAS AJUSTE PROVISÃO' as descricao,
            'INFEAJPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'I.N.S.S. FÉRIAS ESTORNO PROVISÃO' as descricao,
            'INFEESPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'I.N.S.S. FÉRIAS DISTORÇÃO' as descricao,
            'INFEDI' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO PREVIDÊNCIA FÉRIAS PROVISÃO' as descricao,
            'FUPRFEPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO PREVIDÊNCIA FÉRIAS AJUSTE PROVISÃO' as descricao,
            'FUPRFEAJPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO PREVIDÊNCIA FÉRIAS ESTORNO PROVISÃO' as descricao,
            'FUPRFEESPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO PREVIDÊNCIA FÉRIAS DISTORÇÃO' as descricao,
            'FUPRFEDI' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'PREV. ESTADUAL FÉRIAS PROVISÃO' as descricao,
            'PRESFEPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'PREV. ESTADUAL FÉRIAS AJUSTE PROVISÃO' as descricao,
            'PRESFEAJPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'PREV. ESTADUAL FÉRIAS ESTORNO PROVISÃO' as descricao,
            'PRESFEESPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'PREV. ESTADUAL FÉRIAS DISTORÇÃO' as descricao,
            'PRESFEDI' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO ASSISTÊNCIA FÉRIAS PROVISÃO' as descricao,
            'FUASFEPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO ASSISTÊNCIA FÉRIAS AJUSTE PROVISÃO' as descricao,
            'FUASFEAJPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO ASSISTÊNCIA FÉRIAS ESTORNO PROVISÃO' as descricao,
            'FUASFEESPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'FUNDO ASSISTÊNCIA FÉRIAS DISTORÇÃO' as descricao,
            'FUASFEDI' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'F.G.T.S. FÉRIAS PROVISÃO' as descricao,
            'FGFEPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'F.G.T.S. FÉRIAS AJUSTE PROVISÃO' as descricao,
            'FGFEAJPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'F.G.T.S. FÉRIAS ESTORNO PROVISÃO' as descricao,
            'FGFEESPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            '1/3 DE FÉRIAS PROVISÃO' as descricao,
            '13DEFEPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            '1/3 DE FÉRIAS AJUSTE PROVISÃO' as descricao,
            '13DEFEAJPR' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'PROVISÃO - BASE AUXILIAR MÉDIA HORAS FÉRIAS' as descricao,
            'PRBAAUMEHOFE' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'PROVISÃO - BASE AUXILIAR MÉDIA HORAS 13º SALÁRIO' as descricao,
            'PRBAAUMEHO13' as sigla,
            null as classificacaoBaseCalculo
        union all  
        select        
            'PROVISÃO - BASE AUXILIAR DISTORÇÕES FÉRIAS' as descricao,
            'PRBAAUDIFE' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'DEVOLUÇÃO I.R.R.F.' as descricao,
            'DEVIRRF' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'DEVOLUÇÃO I.N.S.S.' as descricao,
            'DEVINSS' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'PARCELA ISENTA I.R.R.F.' as descricao,
            'PARCISENIRRF' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'PARCELA ISENTA I.R.R.F. 13º SALÁRIO' as descricao,
            'PAISIR13SA' as sigla,
            null as classificacaoBaseCalculo
        union all
        select        
            'INSS DE FÉRIAS' as descricao,
            'INSSFER' as sigla,
            'IRRF_FERIAS' as classificacaoBaseCalculo
            ) as a