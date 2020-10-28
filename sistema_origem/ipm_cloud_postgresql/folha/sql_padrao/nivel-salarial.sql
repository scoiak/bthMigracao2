 select
	'300' as sistema,
	'nivel-salarial' as tipo_registro,
	nivcodigo as chave_dsk1,
	*
from (
select
    1 as id,
	clicodigo,
	'2020-01-01 00:00:00' as dataHoraCriacao,
	'2020-01-01' as inicioVigencia,
	nivcodigo,
	nivcodigo || ' - ' || nivdescricao as descricao,
	cast(regexp_replace(nivhoramensal, '\:\d{2}$', '', 'gi') as integer) as cargaHoraria,
	max(nivsalariobase) as valor,
	false as coeficiente,
	null as atoCriacao,
	586 as planoCargoSalario, -- Plano é configurado manualmente no cloud
	null as motivoAlteracao,
	null as historico,
	null as classes,
COALESCE((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', '1'))), 0) as situacao_registro
from wfp.tbnivel
where odomesano = 202009
and nivdesuso = 0
group by id, clicodigo, dataHoraCriacao, inicioVigencia, nivcodigo, descricao, cargaHoraria, coeficiente, atoCriacao, planoCargoSalario, motivoAlteracao,  historico
order by nivcodigo
) tab
where situacao_registro = 0
