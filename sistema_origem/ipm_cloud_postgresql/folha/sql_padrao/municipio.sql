update wun.tbcidade set cidnome = 'Araióses' where cidnome = 'Araioses';
update wun.tbcidade set cidnome = 'Bom Retiro',estcodigo = 24 where cidnome = 'Bom Retiro do Sul' and estcodigo = 23;
update wun.tbcidade set cidnome = 'Embu das Artes'where cidnome = 'Embú das Artes';
update wun.tbcidade set cidnome = 'Espigão do Oeste' where cidnome = 'Espigão D''Oeste';
update wun.tbcidade set cidnome = 'Governador Edson Lobão' where cidnome = 'Governador Edison Lobão';
update wun.tbcidade set cidnome = 'Jequiriçá' where cidnome = 'Jiquiriçá';
update wun.tbcidade set cidnome = 'São Domingos de Pombal' where cidnome = 'São Domingos' and estcodigo = 15;
update wun.tbcidade set cidnome = 'São João do Sóter' where cidnome = 'São João do Soter';
update wun.tbcidade set cidnome = 'Senador La Roque' where cidnome = 'Senador La Rocque';
update wun.tbcidade set cidnome = 'Fortaleza do Tabocão' where cidnome = 'Tabocão';
update wun.tbcidade set cidnome = 'Tomé-Açú' where cidnome = 'Tomé-Açu';
update wun.tbcidade set cidnome = 'Trajano de Morais' where cidnome = 'Trajano de Moraes'; 
update wun.tbcidade set cidnome = 'Vítor Meireles' where cidnome = 'Vitor Meireles';
update wun.tbcidade set cidnome = 'Brasópolis' where cidnome = 'Brazópolis';

select * from (
select
	 c.cidcodigo as id,
	 -- public.bth_get_id_gerado('300', 'estado', e.estnome) as estado,	 
	 c.estcodigo ,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','estado', (select left(e.estnome,20) from wun.tbestado as e where e.estcodigo = c.estcodigo limit 1)))) as estado,
     left(c.cidnome,50) as nome,
	 cast(c.cidcep as varchar) as cep
	from wun.tbcidade as c
	where c.cidtipolocalidade = 1
) as tab
-- where public.bth_get_situacao_registro('300', 'municipio', nome, cast(estado as varchar)) in (0)
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'municipio', nome, cast(estado as varchar)))) is null
and estado is not null;