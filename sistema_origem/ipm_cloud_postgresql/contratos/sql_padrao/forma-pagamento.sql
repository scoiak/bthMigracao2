-- update public.controle_migracao_registro set id_gerado = 2912 where tipo_registro = 'forma-pagamento' and id_gerado is null and i_chave_dsk1 = 'CONFORME EDITAL'

select
	row_number() over() as id,
	'305' as sistema,
	'forma-pagamento' as tipo_registro,
	*
from (
	select distinct
       coalesce(upper(unaccent(trim(edtcondpgto))), 'Conforme Edital') as chave_dsk1,
       coalesce(upper(unaccent(trim(edtcondpgto))), 'Conforme Edital') as descricao,
       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'forma-pagamento', coalesce(upper(unaccent(trim(edtcondpgto))), 'Conforme Edital')))) as id_gerado
 	from wco.tbedital
 	order by 1
) as tab
where id_gerado is null