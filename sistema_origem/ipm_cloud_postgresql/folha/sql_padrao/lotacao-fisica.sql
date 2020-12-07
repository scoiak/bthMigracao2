select distinct
	'300' as sistema,
	'lotacao-fisica' as tipo_registro,
	id_entidade as chave_dsk1,
	numero as chave_dsk2,
	*
from (
	select
		 1 as id,
		 clicodigo,
		 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))) as id_entidade,
		 right('000' || cast(cltcodigo as text), 3) as numero,
		 1 as nivel,
		 cltdescricao as descricao,
		 '1900-01-01 00:00:00' as inicio_vigencia,
		 --(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-lotacao-fisica', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))), '1'))) as configuracao,
		 265 as configuracao,
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
order by chave_dsk1, chave_dsk2