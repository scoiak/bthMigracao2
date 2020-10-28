select distinct
	'300' as sistema,
	'vinculo-empregaticio' as tipo_registro,
	regcodigo as chave_dsk1,
	*
from (
	select distinct
	1 as id,
	regime.regcodigo,
	regime.regdescricao as descricao,
	case   regime.cascodigo
		   when 21 then 'REGIME_PROPRIO'
		   when 1  then 'CLT'
		   else 'OUTROS'
	end as tipo,
	'Sem descr' as descricaoRegimePrevidenciario,
	cat.catcodigo as categoria_codigo,
	cat.catdescricao as categoria_descricao,
	COALESCE((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','categoria-trabalhador', left(cat.catdescricao, 100), cast(cat.catcodigo as text)))), 0) as categoriaTrabalhador,
	'EMPREGADO' as sefip,
	true as geraRais,
	'TRABALHADOR_URBANO_VINCULADO_PESSOA_JURIDICA_CONTRATO_TRABALHO_CLT_PRAZO_INDETERMINADO' as rais,
	false as vinculo_temporario,
	null as motivoRescisao,
	--5148 as motivoRescisao, -- REFERENCIAR TABELA DE MOTIVO DE RESCISÃO
	-- COALESCE((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','motivo-rescisao', CAST(padcodigo as text)))), 0) as situacao_registro
	true as dataFinalObrigatoria,
	true as geraCaged,
	true as geraLicencaPremio,
	'1' as configuracaoAdicional
	from wfp.tbregime regime
	inner join wfp.tbcategoriatrabalhador cat on (cat.catcodigo = regime.catcodigo)
	where odomesano >= 202001
	and odomesano <= 202012
	and regime.catcodigo is not null
) tab