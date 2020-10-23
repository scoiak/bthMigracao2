select
af.motcodigo as id,
af.motcodigo as codigo,
(case when (select au.motdescricao from wfp.tbmotivoafasta as au where au.motcodigo <> af.motcodigo and au.motdescricao = af.motdescricao limit 1) is not null then substring(af.motdescricao,1,90) || ' ' || cast(af.motcodigo as varchar) else af.motdescricao end) as descricao,
(case af.padcodigo
	when 1 then 'AUXILIO_DOENCA_EMPREGADOR'
	when 2 then 'ACIDENTE_DE_TRABALHO_EMPREGADOR'
	else 'FALTA'
	end
) as classificacao,
null as tipoMovimentacaoPessoal,
af.motdiascarencia as diasPrevistos,
false as perdeTempoServico,
(case when af.cpdcodigo is null then false else true end) as consideraVencimento,
false as justificado
from wfp.tbmotivoafasta as af
where odomesano = '202009'