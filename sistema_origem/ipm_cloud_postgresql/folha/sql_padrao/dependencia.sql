select * from (
select
	 d.unicodigodep as id,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = d.unicodigores)))) as pessoa,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = d.unicodigodep)))) as pessoaDependente,
	 null as responsaveis,
	 (case depgrauparentesco      when  1 THEN 'CONJUGE'  when  2 THEN 'FILHO'  when  3 THEN 'PAI_MAE'  when  4 THEN 'PAI_MAE'    when  8 THEN 'NETO'   else null  end) as grau, --  when  5 THEN 'OUTROS' when 10 THEN 'EX-CONJUGE'
	 (case when d.depdataregistro < (select to_date(suc.unfdatanascimento::varchar,'YYYY-MM-DD') from wun.tbunicofisica as suc where suc.unicodigo = d.unicodigodep) then (select suc.unfdatanascimento from wun.tbunicofisica as suc where suc.unicodigo = d.unicodigodep) when d.depdataregistro < (select to_date(suc.unfdatanascimento::varchar,'YYYY-MM-DD') from wun.tbunicofisica as suc where suc.unicodigo = d.unicodigores) then (select to_date(suc.unfdatanascimento::varchar,'YYYY-MM-DD') from wun.tbunicofisica as suc where suc.unicodigo = d.unicodigores) else d.depdataregistro end)::varchar as dataInicio,
	 'OUTRO' as motivoInicio,
	 null as dataTermino,
	 'OUTRO' as motivoTermino,
	 null as dataCasamento,
	 'false' as estuda,
	 null as dataInicioCurso,
	 null as dataFinalCurso,
	 (case depir   when 2 then 'true' else false end) as irrf,
	 (case depsf   when 2 then 'true' else false end) as salarioFamilia,
	 --(case when exists (select pa.unicodigores from wfp.tbpensaoalimenticia as pa where pa.unicodigodep = d.unicodigodep and pa.odomesano = 202010) then true else false end) as pensao,
	 (case when pa.unicodigodep is not null then true else false end) as pensao,
	 pa.pnsdatainicio::varchar as dataInicioBeneficio,
	 (case when pa.pnsdatafinal is not null then 'TEMPORARIA' else 'VITALICIA' end) as duracao,
	 pa.pnsdatafinal::varchar as dataVencimento,
	 null as alvaraJudicial,
	 null as dataAlvara,
	 (case when 1 = 1 then 'VALOR_FIXO' else 'VALOR_PERCENTUAL' end)as aplicacaoDesconto,
	 pa.pnsreferencia::varchar as valorDesconto,
	 null as percentualDesconto,
	 null as percentualPensaoFgts,
	 null as representanteLegal,
	 (case when pa.ifcsequencia is not null then 'CREDITO_EM_CONTA' else 'DINHEIRO' end) as formaPagamento,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'conta-bancaria', (select left(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'),11) from wun.tbunico as u where u.unicodigo = d.unicodigores), (select ucb.ifcnumeroconta from wun.tbunicocontabanco as ucb where ucb.unicodigo = d.unicodigores and ucb.ifcsequencia = pa.ifcsequencia))))::varchar as contaBancaria
from wun.tbdependente as d join wfp.tbpensaoalimenticia as pa on pa.unicodigodep = d.unicodigodep and pa.odomesano = 202010
	where 
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'dependencia', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = d.unicodigores)))),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = d.unicodigodep)))))))  is null	 
	-- and  d.unicodigores  = 687693
) as s
where grau is not null
and pessoa is not null
and pessoaDependente is not null
and pessoa != pessoaDependente