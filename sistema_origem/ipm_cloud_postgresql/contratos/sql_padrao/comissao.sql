select
	row_number() over() as id,
	'305' as sistema,
	'comissao' as tipo_registro,
	*
from (
	select
		c.cmlcodigo as numero,
		c.cmlfinalidade as finalidade,
		(case c.cmltipocomissao when 1 then 'PERMANENTE' else 'ESPECIAL' end ) as tipo_comissao,
		c.cmldatafimvig::varchar as data_expiracao,
		null as data_exoneracao,
		(select tipo_ato.tctdescricao from wlg.tbcategoriatexto tipo_ato where tipo_ato.tctcodigo = ato.tctcodigo and tipo_ato.tcttipo = ato.tcttipo) as tipo_ato,
		c.txjcodigo as ato,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'tipo-ato', ato.tctcodigo))),0) as id_tipo_ato,
		coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'ato', ato.txjnumero, ato.txjano, (select upper(tipo_ato.tctdescricao) from wlg.tbcategoriatexto tipo_ato where tipo_ato.tctcodigo = ato.tctcodigo and tipo_ato.tcttipo = ato.tcttipo)))),0) as id_ato,
		--79031 as id_ato,
		--2507 as id_tipo_ato,
		array(
			select json_build_object(
				'unicodigo', m.unicodigo,
				'cpf', (regexp_replace(un.unicpfcnpj,'[/.-]|[ ]','','g')),
				'id_responsavel', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'responsavel', (regexp_replace(un.unicpfcnpj,'[/.-]|[ ]','','g'))))),
				'atribuicao', (case m.mbcatribuicao when 1 then 'LEILOEIRO' when 2 then 'MEMBRO' when 3 then 'PRESIDENTE' when 4 then 'SECRETARIO' when 6 then 'PREGOEIRO' else 'MEMBRO' end)
			) from wco.tbintegrante m
			natural join wun.tbunico un
			where cmlcodigo = c.cmlcodigo
		) as array_membros,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'comissao', c.cmlcodigo))) as id_gerado
	from wco.tbcomissao c
	left join wlg.tbtextojuridico ato on (ato.txjcodigo = c.txjcodigo)
	order by 1 asc
) tab
where id_gerado is null
--limit 1