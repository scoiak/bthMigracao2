select * from
(
	select
	u.unicodigo as id,
	'GERAL' as tipo,
	-- replace(replace(replace(u.unicpfcnpj,'/',''),'-',''),'.','') as cnpj,
	regexp_replace(u.unicpfcnpj,'[/.-]','','g') as cnpj,
	u.uninomerazao as razaoSocial,
	u.uninomefantasia as nomeFantasia,
	null as porte,
	null as numeroRegistro,
	null as dataRegistro,
	null as orgaoRegistro,
	null as inscricaoMunicipal,
	null as isentoInscricaoEstadual,
	(case when (select ssu.unicodigo from wun.tbunico as ssu where regexp_replace(ssu.unirgie, '[/.-]|[A-Za-z]|[0][ ]','','g') = regexp_replace(u.unirgie, '[/.-]|[A-Za-z]|[0][ ]','','g') limit 1) = u.unicodigo then regexp_replace(u.unirgie, '[/.-]|[A-Za-z]|[0][ ]','','g') else null end) as inscricaoEstadual,
	null as optanteSimples,
	null as site,
	null as sindicato,
	null as numeroAns,
	null as numeroInep,
	null as numeroValeTransporte,
	((select to_date(suc.unfdatanascimento::varchar,'YYYY-MM-DD')+1 from wun.tbunicofisica as suc where suc.unicodigo = uj.unicodigores)::varchar || '%|%' || 'null' || '%|%' || 'Responsavel' || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-fisica', (select regexp_replace(suc.unicpfcnpj,'[/.-]','','g') from wun.tbunico as suc where suc.unicodigo = uj.unicodigores))))) as responsaveis,
	(select string_agg('Email' || (case suc.linha when 1 then '' else ' ' || cast(suc.linha as varchar) end) || '%|%' || suc.uncdescricao || '%|%' || (CASE suc.linha when 1 then 'true' else 'false' end),'%||%') from (select row_number() OVER (partition by uc.unicodigo order by uc.unicodigo desc) as linha,* from wun.tbunicocontato as uc where unctipocontato = 5 and length(trim(uc.uncdescricao)) > 0 and uc.unicodigo = u.unicodigo and (case when trim(uc.uncdescricao) !~ '^[A-Za-z0-9._%-]+[A-Za-z]@[A-Za-z0-9.-]+[.][A-Za-z]+$' then false else true end)) as suc  group by suc.unicodigo order by suc.unicodigo desc) as emails,
	--(select string_agg('Telefone' || (case suc.linha when 1 then '' else ' ' || cast(suc.linha as varchar) end) || '%|%' || (CASE suc.unctipocontato when 1 then 'FIXO' when 2 then 'CELULAR' when 3 then 'FIXO' when 4 then 'FAX' end) || '%|%' || left(regexp_replace(suc.uncdescricao,'[/.-]|[()]|[ ]','','g'),11) || '%|%' || coalesce(suc.unccomplemento,'S/C') || '%|%' || (CASE suc.linha when 1 then 'true' else 'false' end),'%||%') from (select row_number() OVER (partition by uc.unicodigo order by uc.unicodigo desc) as linha,* from wun.tbunicocontato as uc where uc.unctipocontato in (1,2,3,4) and length(trim(uc.uncdescricao)) > 0 and uc.unicodigo = u.unicodigo and (select ssuc.unicodigo from wun.tbunicocontato as ssuc where ssuc.unctipocontato in (1,2,3,4) and left(regexp_replace(ssuc.uncdescricao,'[/.-]|[()]|[ ]','','g'),11) = left(regexp_replace(uc.uncdescricao,'[/.-]|[()]|[ ]','','g'),11) limit 1) = u.unicodigo ) as suc group by suc.unicodigo order by suc.unicodigo desc) as telefones,
	null as telefones,
	-- null as telefones,
	(select string_agg('Endereço' || (case suc.linha when 1 then '' else ' ' || cast(suc.linha as varchar) end) || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','logradouro', (select left(upper(l.lognome),50) from wun.tblogradouro as l where l.logcodigo = suc.logcodigo),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','municipio', (select left(c.cidnome,50) from wun.tbcidade as c where c.cidcodigo = suc.cidcodigo limit 1),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','estado', (select left(e.estnome,20) from wun.tbestado as e where e.estcodigo = (select c.estcodigo from wun.tbcidade as c where c.cidcodigo = suc.cidcodigo limit 1) limit 1)))))))))) || '%|%' || (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','bairro', (select left(upper(b.bainome),50) from wun.tbbairro as b where b.baicodigo = suc.baicodigo),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','municipio', (select left(c.cidnome,50) from wun.tbcidade as c where c.cidcodigo = suc.cidcodigo limit 1),(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','estado', (select left(e.estnome,20) from wun.tbestado as e where e.estcodigo = (select c.estcodigo from wun.tbcidade as c where c.cidcodigo = suc.cidcodigo limit 1) limit 1)))))))))) || '%|%' || coalesce(nullif(trim(suc.cplcep::varchar),''),'00000000') || '%|%' || coalesce(nullif(trim(suc.unenumero),''), 'S/N') || '%|%' || coalesce(nullif(trim(left(suc.unecomplemento,30)),''),'S/C') || '%|%' || (CASE suc.linha when 1 then 'true' else 'false' end),'%||%') from (select row_number() OVER (partition by uc.unicodigo order by uc.unicodigo desc) as linha,* from wun.tbunicoendereco as uc where uc.unicodigo = u.unicodigo) as suc group by suc.unicodigo order by suc.unicodigo desc) as enderecos
from
	wun.tbunico as u left join wun.tbunicojuridica as uj on uj.unicodigo = u.unicodigo
where
	u.unitipopessoa = 2
	--and u.unicpfcnpj = '30.127.910/0001-09'
--and	u.unisituacao = 1
and
	length(regexp_replace(u.unicpfcnpj,'[/.-]|[0]','','g')) > 0
and
	((uj.unicodigores is not null and uj.unjmei = 1) or (uj.unicodigores is  null and uj.unjmei = 0))
) as a
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'pessoa-juridica', cnpj))) is null

-- limit 1000