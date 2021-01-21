select
	row_number() over() as id,
	'tipo-administracao' as tipo_registro,
	'305' as sistema,
	*
from (
	select distinct
	    clitipoentidade,
		(case clitipoentidade
			when 1 then 'Administração Executiva Municipal'
			when 2 then 'Administração Legislativa Municipal'
			when 3 then 'Fundo Municipal'
			when 4 then 'Autarquia Municipal (RPPS)'
			else 'Administração Executiva Municipal'
		end) as descricao,
		(case
			when clitipoentidade IN (1,2,4) then 'DIRETO'
			when clitipoentidade IN (3) then 'INDIRETO'
			else null
		end) as tipo_administracao,
		(case
			when clitipoentidade IN (1,3,4) then 'EXECUTIVO'
			when clitipoentidade IN (2) then 'LEGISLATIVO'
			else null
		end) as poder,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,'tipo-administracao', clitipoentidade))) as id_gerado
	from wun.tbcliente
) tab
where id_gerado is null