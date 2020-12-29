select
	row_number() over() as id,
	'305' as sistema,
	'fonte-divulgacao' as tipo_registro,
	descricao as chave_dsk1,
	*
from (
	select distinct
		vpucodigo,
		vpudescricao as descricao,
		vputipo as tipo,
		case vputipo
			when 1 then 'JORNAL_DIVULGACAO_NACIONAL'
			when 2 then 'JORNAL_DIVULGACAO_ESTADUAL'
			when 3 then 'JORNAL_DIVULGACAO_REGIONAL'
			when 4 then 'JORNAL_DIVULGACAO_MUNICIPAL'
			when 5 then 'DIARIO_OFICIAL_UNIAO'
			when 6 then 'DIARIO_OFICIAL_ESTADO'
			when 7 then 'DIARIO_JUSTICA'
			when 8 then 'MURAL_PUBLICO'
			when 9 then 'INTERNET'
			when 10 then 'DIARIO_ASSEMBLEIA'
			when 11 then 'DIARIO_OFICIAL_MUNICIPIO'
		end as meio_comunicacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'fonte-divulgacao', vpudescricao))) as id_gerado
	from wun.tbveiculopublic
	order by 1
) tab
where id_gerado is null