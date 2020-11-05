select distinct
	*
from (
	select
	regcodigo as id,
	regcodigo as codigo,
	regdescricao as descricao,
	(case cascodigo when 21 then 'REGIME_PROPRIO' when 1  then 'CLT' else 'OUTROS' end) as tipo,
	null as descricaoRegimePrevidenciario,	
	coalesce((select id_gerado 
	   from public.controle_migracao_registro 
	  where hash_chave_dsk = md5(concat('300','categoria-trabalhador',
	  						(select left(c.catdescricao,100) from wfp.tbcategoriatrabalhador as c 
	  						where c.catcodigo = r.catcodigo), catcodigo::varchar))),1671) as categoriaTrabalhador,
	'EMPREGADO' as sefip,
	'true' as geraRais,
	'TRABALHADOR_URBANO_VINCULADO_PESSOA_JURIDICA_CONTRATO_TRABALHO_CLT_PRAZO_INDETERMINADO' as rais,
	(CASE regcodigo 
     when  24 then 'true' 
     when  3 then 'true'
     when  4 then 'true'
     else 'false'
    end ) as vinculoTemporario,
	null as motivoRescisao, -- REFERENCIAR TABELA DE MOTIVO DE RESCISÃO
	(CASE regcodigo 
     when  24 then 'true' 
     when  3 then 'true'
     when  4 then 'true'
     else 'false'
    end ) as dataFinalObrigatoria,
	(case cascodigo when 1  then 'true' else 'false' end) as geraCaged,
	'true' as geraLicencaPremio
	from wfp.tbregime as r
	where odomesano = '202009'
) as a
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','vinculo-empregaticio',codigo::varchar))) is null
--and categoriaTrabalhador is not null