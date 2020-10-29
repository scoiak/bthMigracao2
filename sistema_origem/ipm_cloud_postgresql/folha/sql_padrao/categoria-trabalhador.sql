select
   '300' as sistema,
   'categoria-trabalhador' as tipo_registro,
   descricao as chave_dsk1,
   catcodigo as chave_dsk2,
   *
from (
	select distinct
	 1 as id,
	 left(catdescricao, 100) as descricao,
	 case catgrupo
			   when 1 then 'CLT'
			   when 2 then 'REGIME_PROPRIO'
			   else 'OUTROS'
	  end as tipo,
	 '' as descricaoRegimePrevidenciario,
	 catcodigo,
	 case catcodigo
	   when 101 then 'FUNCIONARIO'
	   when 103 then 'FUNCIONARIO'
	   when 105 then 'FUNCIONARIO'
	   when 106 then 'FUNCIONARIO'
	   when 410 then 'CESSAO'
	   when 901 then 'BOLSISTA'
	   when 902 then 'FUNCIONARIO'
	   when 301 then 'AGENTE_PUBLICO'
	   when 302 then 'AGENTE_PUBLICO'
	   when 303 then 'AGENTE_PUBLICO'
	   when 305 then 'AGENTE_PUBLICO'
	   when 306 then 'AGENTE_PUBLICO'
	   when 309 then 'AGENTE_PUBLICO'
	   when 310 then 'AGENTE_PUBLICO'
	   when 701 then 'CONTRIBUINTE_INDIVIDUAL'
	   when 711 then 'CONTRIBUINTE_INDIVIDUAL'
	   when 712 then 'CONTRIBUINTE_INDIVIDUAL'
	   when 741 then 'CONTRIBUINTE_INDIVIDUAL'
	   when 771 then 'CONTRIBUINTE_INDIVIDUAL'
	   else 'FUNCIONARIO'
	end as grupoTrabalhador	
	from wfp.tbcategoriatrabalhador
) tb
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'categoria-trabalhador', descricao, cast(catcodigo as text)))) is null
