select     
	 unicodigodep as id,
	 unicodigodep as codigo,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = d.unicodigores)))) as pessoa,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = d.unicodigodep)))) as pessoaDependente,
	 null as responsaveis,
	 (case depgrauparentesco      when  1 THEN 'CONJUGE'  when  2 THEN 'FILHO'  when  3 THEN 'PAI_MAE'  when  4 THEN 'PAI_MAE'  when  5 THEN 'OUTROS'   when  8 THEN 'NETO'   when 10 THEN 'EX-CONJUGE'      else null  end) as grau,
	 depdataregistro as dataInicio,
	 'OUTRO' as motivoInicio,
	 null as dataTermino,
	 'OUTRO' as motivoTermino,
	 null as dataCasamento,
	 'false' as estuda,
	 null as dataInicioCurso,
	 null as dataFinalCurso,
	 (case depir                when 1 then 'false'     when 2 then 'true' end) as irrf,
	 (case depsf            when 1 then 'false'     when 2 then 'true' end) as salarioFamilia,
	 null as pensao, --    select * from wfp.tbpensaoalimenticia
	 null as dataInicioBeneficio,
	 null as duracao,
	 null as dataVencimento,
	 null as alvaraJudicial,
	 null as dataAlvara,
	 null as aplicacaoDesconto,
	 null as valorDesconto,
	 null as percentualDesconto,
	 null as percentualPensaoFgts,
	 null as representanteLegal,
	 null as formaPagamento,
	 null as contaBancaria
from wun.tbdependente as d
	 where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'dependencia', unicodigodep::varchar)))  is null
and
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = d.unicodigores)))) is not null
and
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = d.unicodigodep)))) is not null

