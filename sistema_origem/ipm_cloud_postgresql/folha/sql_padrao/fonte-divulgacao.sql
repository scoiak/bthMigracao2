SELECT	
	*
from (
	select
		vpucodigo as id,
		vpudescricao as descricao,
		vputipo as tipo,
		case vputipo
			when 1 then 'JORNAL_CIRCULACAO_NACIONAL'
			when 2 then 'JORNAL_CIRCULACAO_ESTADUAL'
			when 3 then 'JORNAL_CIRCULACAO_REGIONAL'
			when 4 then 'JORNAL_CIRCULACAO_MUNICIPAL'
			when 5 then 'DIARIO_OFICIAL_UNIAO'
			when 6 then 'DIARIO_OFICIAL_ESTADO'
			when 7 then 'DIARIO_JUSTICA'
			when 8 then 'MURAL_PUBLICO'
			when 9 then 'INTERNET'
			when 10 then 'DIARIO_ASSEMBLEIA'
			when 11 then 'DIARIO_OFICIAL_MUNICIPIO'
		end as meioComunicacao	
	from wun.tbveiculopublic
	order by vputipo
) tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'fonte-divulgacao',descricao))) is null
