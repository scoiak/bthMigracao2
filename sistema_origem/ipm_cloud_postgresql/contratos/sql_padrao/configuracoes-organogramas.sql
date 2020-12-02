select * from (
	select 'codigo da entidade' as entidade,
			2020 as ano,
			'##.###.###.###.###' as mascara,
			'Configuração Organograma 2020' as descricao,
			'.' as separador,
			305 as sistema,
			'configuracoes-organogramas' as  tipo_registro,
			1 as nivel1,
			2 as digito1,
			'Órgão' as descricao1,
			2 as nivel2,
			3 as digito2,
			'Unidade' as descricao2,
			3 as nivel3,
			3 as digito3,
			'Centro de Custo' as descricao3,
			4 as nivel4,
			3 as digito4,
			'Centro de Custo Pai' as descricao4,
			5 as nivel5,
			3 as digito5,
			'Secretaria' as descricao5,
			(select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'configuracoes-organogramas', 2020))) as id_gerado
	union
	select 'codigo da entidade' as entidade,
			2019 as ano,
			'##.###.###.###.###' as mascara,
			'Configuração Organograma 2019' as descricao,
			'.' as separador,
			305 as sistema,
			'configuracoes-organogramas' as  tipo_registro,
			1 as nivel1,
			2 as digito1,
			'Órgão' as descricao1,
			2 as nivel2,
			3 as digito2,
			'Unidade' as descricao2,
			3 as nivel3,
			3 as digito3,
			'Centro de Custo' as descricao3,
			4 as nivel4,
			3 as digito4,
			'Centro de Custo Pai' as descricao4,
			5 as nivel5,
			3 as digito5,
			'Secretaria' as descricao5,
			(select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'configuracoes-organogramas', 2019))) as id_gerado
	union
	select 'codigo da entidade' as entidade,
			2018 as ano,
			'##.###.###.###.###' as mascara,
			'Configuração Organograma 2018' as descricao,
			'.' as separador,
			305 as sistema,
			'configuracoes-organogramas' as  tipo_registro,
			1 as nivel1,
			2 as digito1,
			'Órgão' as descricao1,
			2 as nivel2,
			3 as digito2,
			'Unidade' as descricao2,
			3 as nivel3,
			3 as digito3,
			'Centro de Custo' as descricao3,
			4 as nivel4,
			3 as digito4,
			'Centro de Custo Pai' as descricao4,
			5 as nivel5,
			3 as digito5,
			'Secretaria' as descricao5,
			(select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'configuracoes-organogramas', 2018))) as id_gerado
	union
	select 'codigo da entidade' as entidade,
			2017 as ano,
			'##.###.###.###.###' as mascara,
			'Configuração Organograma 2017' as descricao,
			'.' as separador,
			305 as sistema,
			'configuracoes-organogramas' as  tipo_registro,
			1 as nivel1,
			2 as digito1,
			'Órgão' as descricao1,
			2 as nivel2,
			3 as digito2,
			'Unidade' as descricao2,
			3 as nivel3,
			3 as digito3,
			'Centro de Custo' as descricao3,
			4 as nivel4,
			3 as digito4,
			'Centro de Custo Pai' as descricao4,
			5 as nivel5,
			3 as digito5,
			'Secretaria' as descricao5,
			(select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'configuracoes-organogramas', 2017))) as id_gerado
	union
	select 'codigo da entidade' as entidade,
			2016 as ano,
			'##.###.###.###.###' as mascara,
			'Configuração Organograma 2016' as descricao,
			'.' as separador,
			305 as sistema,
			'configuracoes-organogramas' as  tipo_registro,
			1 as nivel1,
			2 as digito1,
			'Órgão' as descricao1,
			2 as nivel2,
			3 as digito2,
			'Unidade' as descricao2,
			3 as nivel3,
			3 as digito3,
			'Centro de Custo' as descricao3,
			4 as nivel4,
			3 as digito4,
			'Centro de Custo Pai' as descricao4,
			5 as nivel5,
			3 as digito5,
			'Secretaria' as descricao5,
			(select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'configuracoes-organogramas', 2016))) as id_gerado
	union
	select 'codigo da entidade' as entidade,
			2015 as ano,
			'##.###.###.###.###' as mascara,
			'Configuração Organograma 2015' as descricao,
			'.' as separador,
			305 as sistema,
			'configuracoes-organogramas' as  tipo_registro,
			1 as nivel1,
			2 as digito1,
			'Órgão' as descricao1,
			2 as nivel2,
			3 as digito2,
			'Unidade' as descricao2,
			3 as nivel3,
			3 as digito3,
			'Centro de Custo' as descricao3,
			4 as nivel4,
			3 as digito4,
			'Centro de Custo Pai' as descricao4,
			5 as nivel5,
			3 as digito5,
			'Secretaria' as descricao5,
			(select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'configuracoes-organogramas', 2015))) as id_gerado
) tab
where id_gerado is null