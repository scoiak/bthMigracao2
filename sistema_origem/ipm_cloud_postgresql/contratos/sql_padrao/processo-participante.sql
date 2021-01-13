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
	    	else concat(e.edtdataaberprop, ' ', coalesce(edthoraaberprop, '00:00:00'))
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
	left join wco.tbprocesso pr on (pr.clicodigo = p.clicodigo and pr.pcsano = p.minano and pr.pcsnro = p.minnro)
	natural join wun.tbunico u
	where p.clicodigo = 2016--{{clicodigo}}
	and pr.modcodigo <> 1
	order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_processo is not null
and id_fornecedor is not null