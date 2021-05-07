select
	row_number() over() as id,
	'305' as sistema,
	'processo-participante' as tipo_registro,
	*
from (
	select
		p.clicodigo,
	    p.minano as ano_processo,
	    p.minnro as nro_processo,
	    p.unicodigo,
	    (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_fornecedor,
	    'INDIVIDUAL' as tipo_participacao,
	    (case
	    	when e.edtdatacredenc is not null then concat(e.edtdatacredenc, ' 00:00:00')
	    	when e.edtdataaberprop is not null then concat(e.edtdataaberprop, ' ', coalesce(edthoraaberprop, '00:00:00'))
	    	else concat(pr.pcsdataproc, ' 00:00:00')
	    end) as dh_credenciamento,
	    p.prlhabilitacao,
	    (case p.prlhabilitacao when 1 then 'HABILITADO' when 2 then 'INABILITADO' when 3 then 'INABILITADO' else 'NAO_ANALISADO' end) as situacao_documentacao,
	    false as representante_legal,
	    null as nome_representante,
	    null as cpf_representante,
	    false as declaracao_mpe,
	    true as renunciou_recurso,
	    null as observacao,
	    'OUTRAS' as sede_mpe,
	    pr.modcodigo,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', p.clicodigo, p.minano, p.minnro))) as id_processo,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_fornecedor,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante', p.clicodigo, p.minano, p.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_gerado
	from wco.tbparlic p
	left join wco.tbedital e on (e.clicodigo = p.clicodigo and e.minano = p.minano and e.minnro = p.minnro)
	left join wco.tbminuta m on (m.clicodigo = p.clicodigo and m.minano = p.minano and m.minnro = p.minnro)
	left join wco.tbprocesso pr on (pr.clicodigo = p.clicodigo and pr.pcsano = m.pcsano and pr.pcsnro = m.pcsnro)
	natural join wun.tbunico u
	where p.clicodigo = {{clicodigo}}
    and p.minano = {{ano}}
    and p.minnro in (20, 15, 10)
	--and pr.modcodigo <> 1
	order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_processo is not null
and id_fornecedor is not null
--and id_processo in (212638,212707,212749,213022,213025,212558,213019,212741,212734,212837,212869,212449,212305,212865,212931,212302,212913,212951,213004,212270,212570,212703,212288,212318,212345,212568,212613,212712,212866,212935,212987,213031,212955,212958,212250,212399,212422,212451,212499,212426,212519,212529,212304,212389,212395,212930,212667,212651,212710,212647,213285,213132,213190,213256,213399,213394,213393,213414,213421,213413,213401,213409,213411,213408,213428,213497,213763,213449,213758)
