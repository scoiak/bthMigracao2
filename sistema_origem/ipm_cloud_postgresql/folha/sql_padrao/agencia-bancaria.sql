select * from (
select
	 row_number() OVER () as id,
	 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','banco', cast(bcocodigo as varchar)))) as banco,
	 bcanome as nome,
	 bcaagencia as numero,
	 bcadigito as digito
	from wun.tbbancoagencia
) as a
where public.bth_get_situacao_registro('300', 'agencia-bancaria', cast(numero as varchar), cast(banco as varchar)) in (0)
and banco is not null