 select
	row_number() over() as id,
	'305' as sistema,
	'processo-sessao' as tipo_registro,
	*
from (
	 select
	 	m.clicodigo,
	 	m.minano as ano_processo,
	 	m.minnro as nro_processo,
	 	(case when p.modcodigo in (7, 8) then 'S' else 'N' end) as compra_direta,
	 	(case when p.modcodigo in (7, 8) then 25 else 18 end) as id_tipo_sessao_julgamento,
	 	(case when p.modcodigo in (7, 8) then 'SEM_SESSAO' else 'ABERTA' end) as situacao,
        concat(coalesce(e.edtdataentrprop, m.mindata, p.pcsdataproc), ' ', coalesce(e.edthoraentrprop, '00:00:00')) as dh_andamento,	 	null as observacao,
	 	m.cmlcodigo,
	 	array(select distinct id_gerado from public.controle_migracao_registro where sistema = 305 and tipo_registro = 'comissao-membros' and i_chave_dsk1 = (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'comissao', m.cmlcodigo)))::varchar) as array_membros,
	 	(select id_gerado from public.controle_migracao_registro  where hash_chave_dsk = md5(concat('305', 'fornecedor', (select (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) from wun.tbunico u where u.unicodigo = (select mi.unicodigo from wco.tbintegrante mi where mi.cmlcodigo = m.cmlcodigo and mi.mbcatribuicao in (3, 6) order by mbcdatainicio desc limit 1))))) id_presidente,
	 	null as participante,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', m.clicodigo, m.minano, m.minnro)))  as id_processo,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-forma-contratacao', m.clicodigo, m.minano, m.minnro))) as id_forma_contratacao,
	 	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-sessao', m.clicodigo, m.minano, m.pcsnro))) as id_gerado
	 from wco.tbminuta m
	 left join wco.tbprocesso p on (m.clicodigo = p.clicodigo and m.pcsano = p.pcsano and m.pcsnro = p.pcsnro)
	 left join wco.tbedital   e on (e.clicodigo = p.clicodigo and e.minano = m.minano and e.minnro = m.minnro)
	 where m.clicodigo = {{clicodigo}}
	 and m.minano = {{ano}}
	 and m.minnro in (20, 15, 10)
	 order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_processo is not null
and id_forma_contratacao is not null
--and id_processo in (212638,212707,212749,213022,213025,212558,213019,212741,212734,212837,212869,212449,212305,212865,212931,212302,212913,212951,213004,212270,212570,212703,212288,212318,212345,212568,212613,212712,212866,212935,212987,213031,212955,212958,212250,212399,212422,212451,212499,212426,212519,212529,212304,212389,212395,212930,212667,212651,212710,212647,213285,213132,213190,213256,213399,213394,213393,213414,213421,213413,213401,213409,213411,213408,213428,213497,213763,213449,213758)
