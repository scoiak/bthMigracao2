select * from wfp.tbrubricaautonomoplano
select * from wfp.tbrubricaautonomo
select * from wfp.tbgpsgcencusautonomo

select * from public.controle_migracao_lotes where tipo_registro = 'cbo';
select * from public.controle_migracao_registro where tipo_registro = 'cbo';
select * from public.controle_migracao_registro_ocor where tipo_registro = 'cbo';

select * from wfp.tbfuncontrato where regcodigo in (15) and odomesano = 202009 and fcncodigo = 9236