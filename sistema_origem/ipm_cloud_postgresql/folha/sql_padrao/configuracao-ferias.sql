select
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))) as id_entidade, -- ID da entidade no cloud, deve ser inserido manualmente
*
from (
    select
        row_number() over() as id,
        row_number() over() as codigo,
        'Configuração Geral' as descricao,
        12 as mesesParaAquisicao,
        12 as mesesParaConcessao,
        12 as mesesParaCritica,
        30 as diasParaAdiquirirNoPeriodo,
        'DIAS' as controleAbono,
        10 as abono,
        'INTEGRAL' as pagamentoFerias,
        false as truncarDias,
        'DATA_ADIMISSAO' as inicioPeriodo,
        null as diaMesInicioPeriodo,
        false as periodoNovoFeriasProporcional,
        null as descontosFaltas,
        null as cancelamentos,
        null as suspensoes
) as a