SELECT 
row_number() over() as id,
--row_number() over() as codigo,
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))) as entidade,
null as orgaoClasse,
null as segurancaTrabalho,
null as ufOrgaoClasse,
null as areasAtuacao,
* FROM (
select 
1 as codigo,
'Ensino Fundamental' AS descricao,
'ENSINO_FUNDAMENTAL' AS nivel
union
select 
2 as codigo,
'Ensino Medio' AS descricao,
'ENSINO_MEDIO' AS nivel
union
select
3 as codigo,
'Ensino Profissionalizante' AS descricao,
'ENSINO_PROFISSIONALIZANTE' AS nivel
union
select
4 as codigo,
'Graduacao' AS descricao,
'GRADUACAO' AS nivel
union
select
5 as codigo,
'Especializacao' AS descricao,
'ESPECIALIZACAO' AS nivel
union
select
6 as codigo,
'MBA' AS descricao,
'MBA' AS nivel
union
select
7 as codigo,
'Mestrado' AS descricao,
'MESTRADO' AS nivel
union
select
8 as codigo,
'Doutorado' AS descricao,
'DOUTORADO' AS nivel
union
select 
9 as codigo,
'Pos Doutorado' AS descricao,
'POS_DOUTORADO' AS nivel
union
select 
10 as codigo,
'Nao Informado' AS descricao,
'ENSINO_FUNDAMENTAL' AS nivel
) AS a
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'formacao', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 2016))), codigo))) is null
