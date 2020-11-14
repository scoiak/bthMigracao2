SELECT 
1 as id,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))) as id_entidade,
1 as codigo,
'PLANO GERAL' AS descricao,
'DATA_ADMISSAO' AS inicio,
'ALFANUMERICA' AS mascaraClasse,
6 AS limiteClasse,
'ALFANUMERICA' AS mascaraReferencia,
6 AS limiteReferencia,
FALSE AS controlaCargaHorariaNiveis,
NULL configuracaoAfastamentos,
NULL faixasProgressao