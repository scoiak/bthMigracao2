select
    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))) as id_entidade,
    *
from (
	select 1 as codigo,1 as id,'Efetivo' as descricao,'EFETIVO' as classificacao
	union
	select 2 as codigo,2 as id,'Comissionado' as descricao,'COMISSIONADO' as classificacao
	union
	select 3 as codigo,3 as id,'Emprego Público' as descricao,'EMPREGO_PUBLICO' as classificacao
	union
	select 4 as codigo,4 as id,'Agente Político' as descricao,'ELETIVO' as classificacao
	union
	select 5 as codigo,5 as id,'Temporário' as descricao,'NAO_CLASSIFICADO' as classificacao
	union
	select 6 as codigo,6 as id,'Estágio' as descricao,'NAO_CLASSIFICADO' as classificacao
	union
	select 7 as codigo,7 as id,'Conselheiro' as descricao,'FUNCAO_PUBLICA' as classificacao
	union
	select 99 as codigo,99 as id,'Outros' as descricao,'NAO_CLASSIFICADO' as classificacao
) as s order by 1