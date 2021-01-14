select
	row_number() over() as id,
	'305' as sistema,
	'logradouro' as tipo_registro,
	*
from (
	select
		logradouros.*,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','tipo-logradouro', tipo_logradouro))) as id_tipo_logradouro,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','estado', nome_estado))) as id_estado,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','municipio', nome_municipio, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','estado', nome_estado)))))) as id_municipio,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','logradouro', nome_logradouro, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','municipio', nome_municipio, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','estado', nome_estado)))))))))  as id_gerado
	from (
		select distinct
			l.logcodigo,
			left(upper(l.lognome),50) as nome_logradouro,
			initcap((select tplnome from wun.tbtipolograd as tl where tl.tplcodigo = l.tplcodigo limit 1)) as tipo_logradouro,
			(select c.cidnome from wun.tbcidade as c where c.cidcodigo = l.cidcodigo limit 1) as nome_municipio,
			(select left(e.estnome,20) from wun.tbestado as e where e.estcodigo = (select c.estcodigo from wun.tbcidade as c where c.cidcodigo = l.cidcodigo limit 1) limit 1) as nome_estado
		from wun.tblogradouro l
		order by 2
	) as logradouros
) as tab
where id_gerado is null
and id_municipio is not null
--limit 2