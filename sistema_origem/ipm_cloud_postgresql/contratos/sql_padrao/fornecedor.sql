select
	row_number() over() as id,
	'305' as sistema,
	'fornecedor' as tipo_registro,
	*
from (
	select
		p.*,
		'ATIVO' as situacao,
		coalesce((select rhadatahora::date from wun.tbunicohistalt where unicodigo = p.unicodigo and rhasequencia = 1)::text, '1900-01-01') as dataInclusao,
		pj.unicodigores as responsavel,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'responsavel', cpf_cnpj))) as id_responsavel,
		'NAO_CLASSIFICADA' as porte_empresa,
		false as optante_simples,
		null as natureza_juridica,
		array(
			select e.uncdescricao
			from wun.tbunicocontato e
			where e.unctipocontato = 5
			and e.uncdescricao ~ '^\w{1}.+@.+\..+$'
			and e.unicodigo = p.unicodigo
		) as array_emails,
		array(
			select
				right(regexp_replace(t.uncdescricao, '\W', '', 'g'), 10) as numero
			from wun.tbunicocontato t
			where t.unicodigo = p.unicodigo
			and t.unctipocontato in (1, 2, 3, 4) -- 1: FIXO, 2:CELULAR, 3:COMERCIAL, 4:FAX
			and length(regexp_replace(t.uncdescricao, '\W', '', 'g')) between 10 and 11
		) as array_telefones,
		array(
			select json_build_object(
				'cep', cep,
				'logradouro', logradouro,
				'id_logradouro', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,
																													'logradouro',
																													logradouro,
																													(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,
																																																			   'municipio',
																																																			   municipio,
																																																			   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'estado', estado))))))))),
				'numero', numero,
				'complemento', complemento,
				'bairro', bairro,
				'id_bairro', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,
																														'bairro',
																														bairro,
																														(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,
																																																				   'municipio',
																																																				   municipio,
																																																				   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'estado', estado))))))))),
				'municipio', municipio,
				'id_municipio', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'municipio', municipio, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'estado', estado)))))),
				'estado', estado,
				'id_estado', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'estado', estado))))
			from (
				select
					e.unicodigo,
					e.cplcep as cep,
					l.lognome as logradouro,
					e.unenumero as numero,
					e.unecomplemento as complemento,
					b.bainome as bairro,
					c.cidnome as municipio,
					est.estnome as estado
				from wun.tbunicoendereco e
				join wun.tbbairro b on (b.baicodigo = e.baicodigo)
				join wun.tbcidade c on (c.cidcodigo = e.cidcodigo)
				join wun.tblogradouro  l on (l.logcodigo = e.logcodigo)
				join wun.tbestado est on (est.estcodigo = c.estcodigo)
				where e.unicodigo = p.unicodigo
				order by 1
			) as aux
		) as array_enderecos,
		null as contas_bancarias,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'fornecedor', cpf_cnpj))) as id_gerado
	from (
		-- Coleta dados de todas as empresas do cadastro único
		(select distinct
			'empresa' as origem,
			f.unicodigo,
			f.uninomerazao as nome,
			f.uninomefantasia as nome_fantasia,
			(regexp_replace(f.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_cnpj,
			'JURIDICA' as tipo_pessoa,
			null as rg,
			f.unirgie as inscricao_estadual
		from wun.tbunico f
		where f.unicpfcnpj not in ('00.000.000/0000-00', '000.000.000-00')
		and f.unitipopessoa = 2
		order by 3)
		union all
		-- Coleta dados de pessoas fisicas como participantes de licitação
		(select distinct
			'participante_licitacao' as origem,
			u.unicodigo,
			u.uninomerazao as nome,
			u.uninomefantasia  as nome_fantasia,
			(regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_cnpj,
			'FISICA' as tipo_pessoa,
			u.unirgie as rg,
			null as inscricao_estadual
		from wco.tbparlic p
		inner join wun.tbunico u on u.unicodigo = p.unicodigo
		where u.unicpfcnpj not in ('00.000.000/0000-00', '000.000.000-00')
		and u.unitipopessoa = 1
		order by 3)
	) as p
	join wun.tbunicojuridica pj on pj.unicodigo = p.unicodigo
	order by nome
) as tab
where id_gerado is null
limit 2
