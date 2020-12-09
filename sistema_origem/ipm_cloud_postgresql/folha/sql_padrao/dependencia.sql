select distinct
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','dependencia', cpf_dependente, cpf_responsavel, coalesce(datainiciobeneficio, datainicio)))) as id_gerado,
	 md5(concat('300','dependencia',cpf_dependente, cpf_responsavel, coalesce(datainiciobeneficio, datainicio))) as hash,
	*
from (
select distinct
	 d.unicodigodep as id,
	 d.unicodigodep,
	 d.unicodigores,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = d.unicodigores)))) as pessoa,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = d.unicodigodep)))) as pessoaDependente,
	 null as responsaveis,
	 (case depgrauparentesco   when 6 THEN 'CONJUGE' when 10 THEN 'CONJUGE' when 1 THEN 'CONJUGE' when 2 THEN 'FILHO' when 3 THEN 'PAI_MAE' when 4 THEN 'PAI_MAE' when 8 THEN 'NETO' when 9 THEN 'BISNETO' when 7 THEN 'IRMAO' else null end) as grau, --  when  5 THEN 'OUTROS' when 10 THEN 'EX-CONJUGE'
	 (case when d.depdataregistro < (select to_date(suc.unfdatanascimento::varchar,'YYYY-MM-DD') from wun.tbunicofisica as suc where suc.unicodigo = d.unicodigodep) then (select suc.unfdatanascimento from wun.tbunicofisica as suc where suc.unicodigo = d.unicodigodep) when d.depdataregistro < (select to_date(suc.unfdatanascimento::varchar,'YYYY-MM-DD') from wun.tbunicofisica as suc where suc.unicodigo = d.unicodigores) then (select to_date(suc.unfdatanascimento::varchar,'YYYY-MM-DD') from wun.tbunicofisica as suc where suc.unicodigo = d.unicodigores) else d.depdataregistro end)::varchar as dataInicio,
	 --(select uf.unfdatanascimento from wun.tbunico as u join wun.tbunicofisica as uf on uf.unicodigo = u.unicodigo where u.unicodigo = d.unicodigodep) as dt_nascimento_dependente,
	 (select coalesce(uf.unfdatanascimento + 1, '1990-01-01') from wun.tbunicofisica as uf where uf.unicodigo = d.unicodigodep) as dt_nascimento_dependente,
	 'OUTRO' as motivoInicio,
	 pa.pnsdatafinal::varchar as dataTermino,
	 'OUTRO' as motivoTermino,
	 null as dataCasamento,
	 'false' as estuda,
	 null as dataInicioCurso,
	 null as dataFinalCurso,
	 (case depir when 3 then false else true end) as irrf,
	 (case depsf when 3 then false else true end) as salarioFamilia,
	 (case when pa.unicodigodep is not null then true else false end) as pensao,
	 pa.pnsdatainicio::varchar as dataInicioBeneficio,
	 (case when pa.pnsdatafinal is not null then 'TEMPORARIA' else 'VITALICIA' end) as duracao,
	 pa.pnsdatafinal::varchar as dataVencimento,
	 null as alvaraJudicial,
	 null as dataAlvara,
	 (case when pa.pnsreferencia is not null then 'VALOR_FIXO' when 2 = 1 then 'VALOR_PERCENTUAL' else  null end) as aplicacaoDesconto,
	 pa.pnsreferencia::varchar as valorDesconto,
	 null as percentualDesconto,
	 null as percentualPensaoFgts,
	 null as representanteLegal,
	 (case when (select ucb.ifcnumeroconta from wun.tbunicocontabanco ucb where ucb.unicodigo = d.unicodigores and ucb.ifcsequencia = pa.ifcsequencia) is not null then 'CREDITO_EM_CONTA' else 'DINHEIRO' end) as formaPagamento,
	 (select left(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'),11) from wun.tbunico as u where u.unicodigo = d.unicodigores) as cpf_responsavel,
	 (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = d.unicodigodep) as cpf_dependente,
	 (select ucb.ifcnumeroconta from wun.tbunicocontabanco ucb where ucb.unicodigo = d.unicodigodep and ucb.ifcsequencia = pa.ifcsequencia) as numero_conta,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'conta-bancaria', (select left(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'),11) from wun.tbunico as u where u.unicodigo = d.unicodigodep), (select ucb.ifcnumeroconta from wun.tbunicocontabanco as ucb where ucb.unicodigo = d.unicodigodep and ucb.ifcsequencia = pa.ifcsequencia))))::varchar as contaBancaria
	 from wun.tbdependente as d
	 left join wfp.tbpensaoalimenticia as pa on d.unicodigodep = pa.unicodigodep and pa.odomesano = 202011
	-- and  d.unicodigores  = 687693
	--where d.unicodigores  = 198803
	--where d.unicodigores  = 898090
	--and d.unicodigores = 2446731
) as s
where grau is not null
--and pensao = true
and pessoa is not null
and pessoaDependente is not null
and pessoa != pessoaDependente
--and (irrf is true or salarioFamilia is true)
and numero_conta is null
-- and cpf_responsavel = '00811714985'
and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','dependencia',cpf_dependente, cpf_responsavel, coalesce(datainiciobeneficio, datainicio)))) is not null
--limit 1


