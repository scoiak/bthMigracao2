select
	'5fcc090bc54eee00e3d550ea' as id_ca_observacoes, -- INSERIR AQUI O ID DO CAMPO ADICIONAL 'OBSERVAÇÕES'
	*
from (
	select
	u.unicodigo as id,
	u.unicodigo as codigo,
	u.uninomerazao as nome,
	-- replace(replace(replace(u.unicpfcnpj,'/',''),'-',''),'.','') as cpf,
	left(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'),11) as cpf,
	cast(coalesce(uf.unfdatanascimento,'1990-01-01') as varchar) as dataNascimento,
	(case uf.unfestadocivil when 1 then 'SOLTEIRO' when 2 then 'CASADO' when 3 then 'SEPARADO_CONSENSUALMENTE' when 4 then 'DIVORCIADO' when 5 then 'VIUVO' when 6 then 'UNIAO_ESTAVEL' else null end) as estadoCivil,
	(case uf.unfsexo when 1 then 'MASCULINO' when 2 then 'FEMININO' else 'MASCULINO' end) as sexo,
	(case uf.unfcorpele when 1 then 'BRANCA' when 2 then 'PRETA' when 3 then 'AMARELA' when 4 then 'PARDA' when 5 then 'INDIGENA' else null end) as raca,
	(case uf.unfcorolhos when 1 then 'PRETO' when 2 then 'AZUL' when 3 then 'CASTANHO' when 4 then 'VERDE' else null end) as corOlhos,
	replace(cast(uf.unfaltura as varchar),',','.') as estatura,
	replace(cast(uf.unfpeso as varchar),',','.') as peso,
	(case uf.unftiporh when 1 then 'A' when 2 then 'B' when 3 then 'AB' when 4 then 'O' else null end) || (case uf.unffatorrh when 1 then 'P' when 2 then 'N' else null end) as tipoSanguineo,
	null as doador,
	(select cast(id_gerado as varchar) from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','pais', (select painome from wun.tbdominionacionalidade as dn join wun.tbpais as p on dn.paisiglaiso = p.paisiglaiso where dn.dnccodigo = uf.unfnacionalidade limit 1)))) as nacionalidade,
	(select cast(id_gerado as varchar) from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','pais', (select painome from wun.tbdominionacionalidade as dn join wun.tbpais as p on dn.paisiglaiso = p.paisiglaiso where dn.paisiglaiso = uf.paisiglaorigem limit 1)))) as paisNascimento,
	(select cast(id_gerado as varchar) from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','municipio', (select left(c.cidnome,50) from wun.tbcidade as c where c.cidcodigo = uf.cidcodigonatural limit 1),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','estado', (select left(e.estnome,20) from wun.tbestado as e where e.estcodigo = (select c.estcodigo from wun.tbcidade as c where c.cidcodigo = uf.cidcodigonatural limit 1) limit 1))))))) as naturalidade,
	cast((case when uf.unfdatachegada < uf.unfdatanascimento then uf.unfdatanascimento else uf.unfdatachegada end) as varchar) as dataChegada,
	(case uf.unfnaturalizado when 0 then 'false' when 1 then 'true' else null end) as naturalizado,
	null as casadoComBrasileiro,
	null as temFilhosBrasileiros,
	null as situacaoEstrangeiro,
	null as inscricaoMunicipal,
	(case when length(regexp_replace(u.unirgie,'[/.-]|[1]|[ ]|[A-Za-z]','','g')) > 1 then (case when (select suc.unicodigo from wun.tbunico as suc where regexp_replace(suc.unirgie,'[/.-]|[A-Za-z]|[ ]','','g') = regexp_replace(u.unirgie,'[/.-]|[A-Za-z]|[ ]','','g') order by suc.unicodigo limit 1) = u.unicodigo then regexp_replace(u.unirgie,'[/.-]|[A-Za-z]|[ ]','','g') else null end) else null end) as identidade,
	-- replace(replace(replace(u.unirgie,'/',''),'-',''),'.','') as identidade,
	uf.unfrgorgaoemissor as orgaoEmissorIdentidade,
	cast(uf.estcodigoemissaorg as varchar) as ufEmissaoIdentidade,
	cast((case when uf.unfrgdataemissao < uf.unfdatanascimento then uf.unfdatanascimento else uf.unfrgdataemissao end) as varchar) as dataEmissaoIdentidade,
	null as dataValidadeIdentidade,
	cast(uf.unfnrotitulo as varchar) as tituloEleitor,
	left(uf.unfzonatitulo::varchar,3) as zonaEleitoral,
	cast(uf.unfsecaotitulo as varchar) as secaoEleitoral,
	cast(uf.unfnroctps as varchar) as ctps,
	cast(uf.unfseriectps as varchar) as serieCtps,
	uf.estcodigoemissaoctps as ufEmissaoCtps,
	cast((case when uf.unfdataemissaoctps < uf.unfdatanascimento then uf.unfdatanascimento else uf.unfdataemissaoctps end) as varchar) as dataEmissaoCtps,
	null as dataValidadeCtps,
	(case when length(uf.unfpispasep::varchar) > 1 then (case when (select suc.unicodigo from wun.tbunicofisica as suc where suc.unfpispasep = uf.unfpispasep limit 1) = u.unicodigo then uf.unfpispasep::varchar else null end) else null end) as pis,
	null as dataEmissaoPis,
	(case when uf.gincodigo in (2) then 'NAO_ALFABETIZADO' when uf.gincodigo in (3,4) then 'ENSINO_FUNDAMENTAL_ANOS_FINAIS' when uf.gincodigo in (5,6) then 'ENSINO_MEDIO' when uf.gincodigo in (7,8) then 'ENSINO_SUPERIOR_SEQUENCIAL' when uf.gincodigo in (9) then 'POS_GRADUACAO_ESPECIALIZACAO' when uf.gincodigo in (10) then 'POS_GRADUACAO_MESTRADO' when uf.gincodigo in (11) then 'POS_GRADUACAO_DOUTORADO' when uf.gincodigo in (12) then 'ENSINO_FUNDAMENTAL_ANOS_INICIAIS' when uf.gincodigo in (13) then 'ENSINO_FUNDAMENTAL_ANOS_INICIAIS' when uf.gincodigo in (14) then 'ENSINO_FUNDAMENTAL_ANOS_FINAIS' when uf.gincodigo in (15) then 'ENSINO_PROFISSIONALIZANTE' when uf.gincodigo in (16) then 'ENSINO_PROFISSIONALIZANTE' when uf.gincodigo in (17) then 'POS_DOUTORADO_HABILITACAO'else null end) as grauInstrucao,
	(case when uf.gincodigo in (1,2,3,5,7,12,14,15) then 'INCOMPLETO' when uf.gincodigo in (4,6,8,9,10,11,13,16,17) then 'COMPLETO' else null end) as situacaoGrauInstrucao,
	cast(uf.unfnrocreservista as varchar) as certificadoReservista,
	null as ric,
	null as ufEmissaoRic,
	uf.unforgaocreservista as orgaoEmissorRic,
	cast(uf.unfemissaocreser as varchar) as dataEmissaoRic,
	uf.unfcartaosus as cns,
	null as dataEmissaoCns,
	cast(uf.unfcnhnumero as varchar) as cnh,
	cast(uf.unfcnhcategoria as varchar) as categoriaCnh,
	null as dataEmissaoCnh,
	cast(uf.unfcnhdatavalidade as varchar) as dataVencimentoCnh,
	null as dataPrimeiraCnh,
	cast(uf.estcodigoemissaocnh as varchar) as ufEmissaoCnh,
	uf.unfcnhobs as observacoesCnh,
	null as papel,
	(select string_agg('Email' || (case suc.linha when 1 then '' else ' ' || cast(suc.linha as varchar) end) || '%|%' || suc.uncdescricao || '%|%' || (CASE suc.linha when 1 then 'true' else 'false' end),'%||%') from (select row_number() OVER (partition by uc.unicodigo order by uc.unicodigo desc) as linha,* from wun.tbunicocontato as uc where unctipocontato = 5 and length(trim(uc.uncdescricao)) > 0 and uc.unicodigo = u.unicodigo and (case when trim(uc.uncdescricao) !~ '^[A-Za-z0-9._%-]+[A-Za-z]@[A-Za-z0-9.-]+[.][A-Za-z]+$' then false else true end)) as suc  group by suc.unicodigo order by suc.unicodigo desc) as emails,
	(select string_agg('Telefone' || (case suc.linha when 1 then '' else ' ' || cast(suc.linha as varchar) end) || '%|%' || (CASE suc.unctipocontato when 1 then 'FIXO' when 2 then 'CELULAR' when 3 then 'FIXO' when 4 then 'FAX' end) || '%|%' || coalesce(left(replace(replace(replace(replace(replace(suc.uncdescricao,'-',''),'.',''),')',''),'(',''),' ',''),11),'0') || '%|%' || coalesce(suc.unccomplemento,'S/C') || '%|%' || (CASE suc.linha when 1 then 'true' else 'false' end),'%||%') from (select row_number() OVER (partition by uc.unicodigo order by uc.unicodigo desc) as linha,* from wun.tbunicocontato as uc where uc.unctipocontato in (1,2,3,4) and length(trim(uc.uncdescricao)) > 0 and uc.unicodigo = u.unicodigo) as suc group by suc.unicodigo order by suc.unicodigo desc) as telefones,
	(select string_agg('Endereço' || (case suc.linha when 1 then '' else ' ' || cast(suc.linha as varchar) end) || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','logradouro', (select left(upper(l.lognome),50) from wun.tblogradouro as l where l.logcodigo = suc.logcodigo),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','municipio', (select left(c.cidnome,50) from wun.tbcidade as c where c.cidcodigo = suc.cidcodigo limit 1),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','estado', (select left(e.estnome,20) from wun.tbestado as e where e.estcodigo = (select c.estcodigo from wun.tbcidade as c where c.cidcodigo = suc.cidcodigo limit 1) limit 1)))))))))) || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','bairro', (select left(upper(b.bainome),50) from wun.tbbairro as b where b.baicodigo = suc.baicodigo),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','municipio', (select left(c.cidnome,50) from wun.tbcidade as c where c.cidcodigo = suc.cidcodigo limit 1),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','estado', (select left(e.estnome,20) from wun.tbestado as e where e.estcodigo = (select c.estcodigo from wun.tbcidade as c where c.cidcodigo = suc.cidcodigo limit 1) limit 1)))))))))) || '%|%' || coalesce(nullif(trim(suc.cplcep::varchar),''),'00000000') || '%|%' || coalesce(nullif(trim(suc.unenumero),''), 'S/N') || '%|%' || coalesce(nullif(trim(left(suc.unecomplemento,30)),''),'S/C') || '%|%' || (CASE suc.linha when 1 then 'true' else 'false' end),'%||%') from (select row_number() OVER (partition by uc.unicodigo order by uc.unicodigo desc) as linha,* from wun.tbunicoendereco as uc where uc.unicodigo = u.unicodigo) as suc group by suc.unicodigo order by suc.unicodigo desc) as enderecos,
	(select string_agg('Conta Bancaria' || (case suc.linha when 1 then '' else ' ' || cast(suc.linha as varchar) end) || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','agencia-bancaria', cast(suc.bcaagencia as varchar),cast((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','banco', cast(suc.bcocodigo as varchar)))) as varchar)))) || '%|%' || suc.ifcnumeroconta || '%|%' || suc.ifcdigitoconta || '%|%' || (case when suc.ifctipoconta in (1,4) then 'CORRENTE' when suc.ifctipoconta in (2,6) then 'POUPANCA' when suc.ifctipoconta in (3,5) then 'SALARIO' end) || '%|%' || '1990-01-01' || '%|%' || '1990-01-01' || '%|%' || 'ABERTA' || '%|%' || (CASE suc.linha when 1 then 'true' else 'false' end), '%||%') from (select row_number() OVER (partition by uc.unicodigo order by uc.unicodigo desc) as linha,* from wun.tbunicocontabanco as uc where uc.unicodigo = u.unicodigo) as suc group by suc.unicodigo order by suc.unicodigo desc) as contasBancarias,
	(case uf.unftipodeficiencia when 2 then 'FISICA' when 3 then 'AUDITIVA' when 4 then 'MENTAL' when 5 then 'MULTIPLA' when 6 then 'AUTISMO' when 7 then 'REABILITADO' when 8 then 'OUTRA' when 9 then 'VISUAL' when 10 then 'MENTAL' when 11 then 'VISUAL' when 12 then 'MULTIPLA' else null end) as deficiencias,
	((case when uf.unfnomemae is not null then (trim(uf.unfnomemae) || '%|%' || 'MAE%|%BIOLOGICA' || (case when trim(uf.unfnomepai) is not null then '%||%' else null end)) else null end) || (case when trim(uf.unfnomepai) is not null then (trim(uf.unfnomepai) || '%|%' || 'PAI%|%BIOLOGICA') else null end)) as filiacoes,
	u.uniobservacoes as observacoes,
	coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', left(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'),11)))), 0) as id_gerado
from
	wun.tbunico as u  join wun.tbunicofisica as uf on uf.unicodigo = u.unicodigo
where
	u.unitipopessoa = 1
and	u.unisituacao = 1
and length(regexp_replace(u.unicpfcnpj,'[/.-]|[0]|[ ]','','g')) > 0
-- length(replace(replace(replace(replace(u.unicpfcnpj,'/',''),'-',''),'.',''),'0','')) > 0
--and	uf.unfsexo in (1,2)
--and	uf.unfdatanascimento is not null
-- and (select suc.unicodigo from wun.tbunico as suc where suc.unitipopessoa = 1 and suc.unisituacao = 1 and suc.unicpfcnpj = u.unicpfcnpj order by suc.unicodigo asc limit 1) = u.unicodigo
) as a
--where id_gerado <> 0
--where cpf in ('00119278014', '00147423066', '00350794952', '00370717937', '00388604905', '00525351930', '00562991921', '00785263969', '00893016098', '00931924952', '01955544930', '02144393990', '02199910903', '02748980980', '03589981903', '03671906995', '03817231946', '03867163901', '03948766959', '04392014975', '04440238955', '04593018960', '04764270960', '04884502922', '05366361923', '07157907143', '09954543961', '10280415940', '13774325995', '34245979968', '37579703904', '58014497953', '61740756991', '64159094953', '64207269991', '68257880949', '68400110978', '71171827920', '71213708915', '76503747004', '79666230925', '81842554972', '82687218987', '90974689904', '9159077999')
--where cpf = '04935366982'