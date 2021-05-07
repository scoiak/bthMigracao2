select
	row_number() over() as id,
	'305' as sistema,
	'processo-participante-documento' as tipo_registro,
	*
from (
	select distinct
		dl.clicodigo,
		dl.minano as ano_processo,
		dl.minnro as nro_processo,
		dl.unicodigo,
		(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_fornecedor,
		dl.doccodigo as cod_documento,
		coalesce(df.dofdataemissao::varchar,'1900-01-01') as dt_emissao,
        (case when (df.dofdatavalidade is null or df.dofdatavalidade <= df.dofdataemissao) then ((df.dofdataemissao + interval '1 day')::date)::varchar else coalesce(df.dofdatavalidade::varchar, '1900-01-02') end) as dt_validade,
        left(df.dofnrodoc, 30) as nro_documento,
		'INDIVIDUAL' as tipo_participacao,
		'VALIDO' as situacao_documento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', dl.clicodigo, dl.minano, dl.minnro))) as id_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante', dl.clicodigo, dl.minano, dl.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_participante,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'tipo-documento', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', 2016))), dl.doccodigo))) as id_tipo_documento,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-documento', dl.clicodigo, dl.minano, dl.minnro, dl.doccodigo))) as id_doc_processo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante-documento', dl.clicodigo, dl.minano, dl.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')), dl.doccodigo))) as id_gerado
	from wco.tbdoclic dl
	left join wco.tbdocfor df on df.unicodigo = dl.unicodigo and df.doccodigo = dl.doccodigo and df.dofsequencia = dl.dofsequencia
	inner join wun.tbunico u on (u.unicodigo = dl.unicodigo)
	where dl.clicodigo = {{clicodigo}}
	and dl.minano = {{ano}}
	and dl.minnro in (20, 15, 10)
	order by 1, 2 desc, 3 desc, 4
) tab
where id_gerado is null
and id_processo is not null
and id_participante is not null
and id_tipo_documento is not null
--and id_processo in (212638,212707,212749,213022,213025,212558,213019,212741,212734,212837,212869,212449,212305,212865,212931,212302,212913,212951,213004,212270,212570,212703,212288,212318,212345,212568,212613,212712,212866,212935,212987,213031,212955,212958,212250,212399,212422,212451,212499,212426,212519,212529,212304,212389,212395,212930,212667,212651,212710,212647,213285,213132,213190,213256,213399,213394,213393,213414,213421,213413,213401,213409,213411,213408,213428,213497,213763,213449,213758)
