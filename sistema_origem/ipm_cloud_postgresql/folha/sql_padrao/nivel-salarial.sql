select 
	*
from (
select	
	nivcodigo as id,	
	nivcodigo as codigo,
	(nivdescricao || ' - ' || row_number() over(partition by nivdescricao order by nivdescricao)) as descricao,
	'2020-01-01' as dataHoraCriacao,
	'2020-01-01' as inicioVigencia,
	nivdescricao,
	nivhoramensal as cargaHoraria,
	nivsalariobase,
	false as coeficiente, 
	null as atoCriacao, 
	586 as planoCargoSalario, -- Plano é configurado manualmente no cloud
	null as motivoAlteracao,
	null as historico
from 
	wfp.tbnivel
	limit 10
order by nivdescricao
) tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'nivel-salarial', codigo))) is null

select * from wfp.tbnivel
