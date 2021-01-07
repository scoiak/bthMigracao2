select
	row_number() over() as id,
	'305' as sistema,
	'bairro' as tipo_registro,
	*
from (
	select
		bairros.*,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','municipio', nome_municipio, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','estado', nome_estado)))))) as id_cidade,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','estado', nome_estado))) as id_estado,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','bairro', upper(nome_bairro), (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','municipio', nome_municipio, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305','estado', nome_estado))))))))) as id_gerado
	from (
		select
			b.baicodigo as codigo,
			left(upper(b.bainome), 50) as nome_bairro,
			(select c.cidnome from wun.tbcidade as c where c.cidcodigo = b.cidcodigo limit 1) as nome_municipio,
			(select left(e.estnome,20) from wun.tbestado as e where e.estcodigo = (select c.estcodigo from wun.tbcidade as c where c.cidcodigo = b.cidcodigo limit 1) limit 1) as nome_estado
		from wun.tbbairro as b
	) as bairros
) tab
where id_gerado is null
and id_cidade is not null
--limit 2