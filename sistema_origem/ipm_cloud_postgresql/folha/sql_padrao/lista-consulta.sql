select * from wfp.tbrubricaautonomoplano
select * from wfp.tbrubricaautonomo
select * from wfp.tbgpsgcencusautonomo

select * from public.controle_migracao_lotes where tipo_registro = 'municipio';
select * from public.controle_migracao_registro where tipo_registro = 'municipio';
select * from public.controle_migracao_registro_ocor where tipo_registro = 'ato';

select * from public.controle_migracao_registro where tipo_registro = 'configuracao-lotacao-fisica';

select * from wfp.tbfuncontrato where regcodigo in (15) and odomesano = 202009 and fcncodigo = 9236

SELECT id_lote, url_consulta FROM public.controle_migracao_lotes WHERE status not in (3, 4, 5) AND tipo_registro = 'pessoa-fisica'