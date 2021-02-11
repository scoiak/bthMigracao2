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
	 --and m.minnro = 258
	 order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_processo is not null
and id_forma_contratacao is not null