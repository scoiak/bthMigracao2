select distinct
	*
from (
	select
		 row_number() over() as id,		 
		 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))) as entidade,
		 right('000' || cast(cltcodigo as text), 3) as numero,
		 1 as nivel,
		 cltdescricao as descricao,
		 '1900-01-01 00:00:00' as inicioVigencia,
		 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-lotacao-fisica', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'configuracao-lotacao0fisica', 1))), '1'))) as configuracao,		 
		 null as telefones,
		 null as municipio,
		 null as rua,
		 null as bairro,
		 null as cep,
		 null as numeroEndereco,
		 null as complemento
	from wfp.tblocais
	where odomesano = 202012
	and cltemdesuso = 0
	order by cltcodigo
) a
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'lotacao-fisica',entidade,numero))) is null