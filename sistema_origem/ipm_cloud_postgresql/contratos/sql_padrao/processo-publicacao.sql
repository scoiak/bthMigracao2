select
	row_number() over() as id,
	'305' as sistema,
	'processo-publicacao' as tipo_registro,
	*
from (
	select
		p.clicodigo,
		p.minano as ano_processo,
		p.minnro as nro_processo,
		p.pblseq as sequencial,
		p.pblnumero as nro_publicacao,
		pbldatapublicacao::varchar as data_publicacao,
		p.pbltipo as tipo_publicacao,
		v.vpudescricao as fonte_divulgacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', p.clicodigo, p.minano, p.minnro))) as id_processo,
		null as id_veiculo_publicacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'tipo-publicacao', p.pbltipo))) as id_tipo_publicacao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'fonte-divulgacao', v.vpudescricao))) as id_fonte_divulgacao,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-forma-contratacao', p.clicodigo, p.minano, p.minnro))) as id_forma_contratacao,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-publicacao', p.clicodigo, p.minano, p.minnro, p.pblseq))) as id_gerado
	from wco.tbpublico p
	inner join wun.tbveiculopublic v on (v.vpucodigo = p.vpucodigo)
	where p.clicodigo = {{clicodigo}}
	and p.minano = {{ano}}
	--and p.minnro = 204
	order by 1, 2 desc, 3 desc, 4
) tab
where id_gerado is null
and id_processo is not null
and id_tipo_publicacao is not null
and id_fonte_divulgacao is not null
and id_forma_contratacao is not null
--limit 1