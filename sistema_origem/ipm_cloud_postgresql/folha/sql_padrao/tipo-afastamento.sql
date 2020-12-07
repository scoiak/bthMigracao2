select *
from (
	select
	af.motcodigo as id,
	af.motcodigo as codigo,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))) as id_entidade,
	(case when (select au.motdescricao from wfp.tbmotivoafasta as au where au.motcodigo <> af.motcodigo and au.motdescricao = af.motdescricao limit 1) is not null then substring(af.motdescricao,1,90) || ' ' || cast(af.motcodigo as varchar) else af.motdescricao end) as descricao,
	(case
		when af.padcodigo in (5,6,7,8,25,15,9,33,32,31,14) then 'DEMITIDO'
		when af.padcodigo in (13,34,35,36,37,38,39,40,41,42) then 'APOSENTADO'
		when af.padcodigo in (3,4) then 'FALTA'
		when af.padcodigo in (20,12,11) then 'CEDENCIA'
		when af.padcodigo in (17,59,62,30,29,26,22,21) then 'LICENCA_COM_VENCIMENTOS'
		when af.padcodigo in (18,47,54,53,50,49,48,46,45,43,44) then 'LICENCA_SEM_VENCIMENTOS'
		when af.padcodigo in (61) then 'MANDATO_ELEITORAL_SEM_REMUNERACAO'
		when af.padcodigo in (58,16,10) then 'LICENCA_MATERNIDADE'
		when af.padcodigo in (47) then 'ACOMPANHAR_MEMBRO_DA_FAMILIA_ENFERMO'
		when af.padcodigo in (2) then 'ACIDENTE_DE_TRABALHO_EMPREGADOR'
		when af.padcodigo in (63,60,56) then 'AUXILIO_DOENCA_EMPREGADOR'
		when af.padcodigo in (55,1) then 'AUXILIO_DOENCA_PREVIDENCIA'
		when af.padcodigo in (52) then 'MANDATO_SINDICAL'
		when af.padcodigo in (51) then 'CANDIDATO_A_CARGO_ELETIVO'
		when af.padcodigo in (19) then 'SERVICO_MILITAR'
		else null
		end
	) as classificacao,
	null as tipoMovimentacaoPessoal,
	(case when af.padcodigo in (3,4,19,51,52,55,1,47,58,16,10,61,18,47,54,53,50,49,48,46,45,43,44,17,59,62,30,29,26,22,21,20,12,11,3,4) then af.motdiascarencia::varchar else null end) as diasPrevistos,
	false as perdeTempoServico,
	(case when af.cpdcodigo is null then false else true end) as consideraVencimento,
	false as justificado
	from wfp.tbmotivoafasta as af
	where odomesano = '202009'
	union
	select
	(select max(motcodigo +1 ) from wfp.tbmotivoafasta) as id,
	(select max(motcodigo +1 ) from wfp.tbmotivoafasta) as codigo,
	'6098' as id_entidade,  -- ESSA INFORMAÇÃO DEVE SER INSERIDA MANUALMENTE
	'DEMITIDO' as descricao,
	'DEMITIDO' as classificacao,
	null as tipoMovimentacaoPessoal,
	null as diasPrevistos,
	false as perdeTempoServico,
	false as consideraVencimento,
	false as justificado
) as a
where classificacao is not null
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-afastamento', id_entidade, codigo))) is null