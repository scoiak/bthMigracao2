select
  tctcodigo as id,
  tctdescricao as chave_dsk1,
  classificacao as chave_dsk2,
  *
from (
  select '300' as sistema,
       'tipo-ato' as tipo_registro,
       tctcodigo,
       case tctcodigo
        when 1 then  'DECRETO'
        when 2 then  'PORTARIA'
        when 3 then  'RESOLUCAO'
        when 4 then  'EDITAL'
        when 5 then  'LEI'
        when 6 then  'CONVENIO'
        when 7 then  'MEDIDA_PROVISORIA'
        when 8 then  'CONTRATO'
        when 9 then  'LEI_AUTORIZATIVA'
        when 10 then 'ESTATUTO_SOCIAL'
        when 11 then 'LEI_CRIACAO'
        when 12 then 'DESPACHO'
        when 13 then 'DESPACHO'
        when 14 then 'MEMORANDO'
        when 15 then 'DESPACHO'
        when 16 then 'PROCESSO'
        when 17 then 'PARECER'
        when 18 then 'ATO'
        when 19 then 'DECRETO_LEGISLATIVO'
        when 20 then 'LEI_COMPLEMENTAR'
        when 21 then 'LEI_ORGANICA'
        when 22 then 'EMENDA_MODIFICATIVA'
        when 23 then 'TERMO_RECEBIMENTO'
        when 24 then 'DESPACHO'
        when 25 then 'INSTRUCAO'
        when 26 then 'EMENDA'
        when 27 then 'PARECER'
        when 28 then 'ATA'
        when 29 then 'REQUERIMENTO'
        when 30 then 'MEMORANDO'
        when 31 then 'ATO_ADMINISTRATIVO'
        when 32 then 'TERMO_RECEBIMENTO'
        when 33 then 'ORCAMENTO_BASE'
        when 34 then 'PLANILHA_ORCAMENTARIA_CONTRATADA'
        when 35 then 'PLANILHA_ORCAMENTARIA_ADITIVO'
        when 36 then 'TERMO_PARALISACAO'
        when 37 then 'TERMO_RECEBIMENTO'
        when 38 then 'MEDICAO'
        when 39 then 'JUSTIFICATIVA_CANCELAMENTO_INTERVENCAO'
        when 40 then 'ATO'
        when 41 then 'COMUNICACAO_INTERNA'
        when 42 then 'DECRETO'
        when 43 then 'CONTRATO'
        when 44 then 'CONTRATO'
        when 45 then 'ATO'
        when 46 then 'EDITAL'
        when 47 then 'DECRETO'
        when 48 then 'TERMO_MEDICAO'
        when 49 then 'ATO_CONSORCIO'
        when 50 then 'CONSTITUICAO'
        when 51 then 'ATO_ADMINISTRATIVO'
        when 52 then 'DELIBERACAO'
        when 53 then 'OFICIO'
        when 54 then 'INSTRUCAO'
        when 55 then 'INSTRUCAO'
        when 56 then 'INSTRUCAO'
        when 57 then 'PROJETO'
        when 58 then 'REGISTRO_IMOVEIS'
        when 59 then 'PARECER'
        when 60 then 'MULTIMIDIA'
        when 61 then 'IMAGEM_EXEMPLAR_PUBLICA_ORGAO_OFICIAL'
        when 62 then 'ATO_COMISSAO_EXEC_LEGIS'
        when 63 then 'ATO_ADMINISTRATIVO'
        when 64 then 'EDITAL'
        else 'ATO'
        end as classificacao,
       left(tctdescricao, 40) as tctdescricao,
       null as codigo_tce
    from wlg.tbcategoriatexto
  order by tctcodigo
) tab
where
  public.bth_get_situacao_registro('300' , 'tipo-ato' , tctdescricao, classificacao) not in (5, 4, 3)
