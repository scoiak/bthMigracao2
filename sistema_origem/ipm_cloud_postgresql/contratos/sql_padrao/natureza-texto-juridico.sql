select
	row_number() over() as id,
	'305' as sistema,
	'	' as tipo_registro,
	*
from (
	select distinct
    	tctcodigo,
        tctdescricao as descricao,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'natureza-texto-juridico', tctcodigo))) as id_gerado
   from wlg.tbcategoriatexto
) tab
where id_gerado is null