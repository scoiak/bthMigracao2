select 
	'300' as sistema, 
	'nivel-salarial' as tipo_registro,
	nivcodigo as chave_dsk1,
	*
from (
select
	clicodigo,
	nivcodigo,
	'2020-01-01' as dataHoraCriacao,
	'2020-01-01' as inicioVigencia,
	nivdescricao,
	nivhoramensal as cargaHoraria,
	nivsalariobase,
	false as coeficiente, 
	null as atoCriacao, 
	586 as planoCargoSalario, -- Plano é configurado manualmente no cloud
	null as motivoAlteracao,
	null as historico,
COALESCE((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', '1'))), 0) as situacao_registro
from 
	wfp.tbnivel
	limit 10
) tab
where situacao_registro = 0 