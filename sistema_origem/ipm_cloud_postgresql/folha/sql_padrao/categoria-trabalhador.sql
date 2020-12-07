select
   *
from (
	select distinct
	 row_number() over() as id,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', '2016'))) as id_entidade,
	 left(catdescricao, 100) as descricao,
	 case catgrupo
			   when 1 then 'CLT'
			   when 2 then 'REGIME_PROPRIO'
			   else 'OUTROS'
	  end as tipo,
	 '' as descricaoRegimePrevidenciario,
	 catcodigo as codigoESocial,
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
	union all
	select 41 as id,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', '2016'))) as id_entidade,
	 'MIGRAÇÃO' as descricao,
	 'OUTROS' as tipo,
	 null as descricaoregimeprevidenciario,
	 '987' as codigoesocial,
	 'FUNCIONARIO' as grupotrabalhador
) tb
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'categoria-trabalhador', id_entidade, descricao, codigoESocial))) is null