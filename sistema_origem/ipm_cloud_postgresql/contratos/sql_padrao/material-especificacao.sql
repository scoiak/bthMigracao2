select
    row_number() over() as id,
	cmr1.id_gerado as id_material, 
	cmr1.i_chave_dsk1 as codigo_produto 
from public.controle_migracao_registro cmr1
where cmr1.tipo_registro = 'material'
and not exists (
	select 1 
	from public.controle_migracao_registro cmr2 
	where cmr2.tipo_registro = 'material-especificacao' 
	and cmr2.i_chave_dsk1 = cmr1.i_chave_dsk1 
)
order by cmr1.i_chave_dsk1::integer desc
--limit 10