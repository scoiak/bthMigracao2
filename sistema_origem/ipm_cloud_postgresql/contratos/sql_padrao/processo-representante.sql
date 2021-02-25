select
	row_number() over() as id,
	'305' as sistema,
	'processo-representante' as tipo_registro,
	*
from (
	select
		p.clicodigo,
	    p.minano as ano_processo,
	    p.minnro as nro_processo,
	    p.unicodigorepr,
	    (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_participante,
	    (regexp_replace(r.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_representante,
	    r.uninomerazao as nome_representante,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', p.clicodigo, p.minano, p.minnro))) as id_processo,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante', p.clicodigo, p.minano, p.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_participante,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-sessao', p.clicodigo, p.minano, p.minnro))) as id_sessao,
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-representante', p.clicodigo, p.minano, p.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_gerado
	from wco.tbparlic p
	left join wco.tbprocesso pr on (pr.clicodigo = p.clicodigo and pr.pcsano = p.minano and pr.pcsnro = p.minnro)
	inner join wun.tbunico r on (r.unicodigo = p.unicodigorepr and r.unitipopessoa = 1)
	inner join wun.tbunico u on (u.unicodigo = p.unicodigo)
	where p.clicodigo = {{clicodigo}}
    and p.minano = {{ano}}
    --and p.minnro = 188
	and pr.modcodigo <> 1
	and p.unicodigorepr is not null
	order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_processo is not null
and id_sessao is not null
and id_participante is not null
--limit 5