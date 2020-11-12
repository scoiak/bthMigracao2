-- Configurar o id da lotação física
select distinct
	'300' as sistema,
	'lotacao-fisica' as tipo_registro,
	numero as chave_dsk1,
	*
from (
	select
		 1 as id,
		 clicodigo,
		 right('000' || cast(cltcodigo as text), 3) as numero,
		 1 as nivel,
		 cltdescricao as descricao,
		 --left(cast(odomesano as text), 4) || '-' || right(cast(odomesano as text), 2) || '-01T00:00:00.000Z' as inicio_vigencia,
		 '1900-01-01 00:00:00' as inicio_vigencia,
		 247 as configuracao,
		 null as telefones,
		 null as municipio,
		 null as rua,
		 null as bairro,
		 null as cep,
		 null as numeroEndereco,
		 null as complemento
	from wfp.tblocais
	where odomesano >= 202001
	and odomesano <= 202012
	and cltemdesuso = 0
	order by cltcodigo
) tab
order by chave_dsk1