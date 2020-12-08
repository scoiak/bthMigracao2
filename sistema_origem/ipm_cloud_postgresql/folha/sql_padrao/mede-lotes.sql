select 1 as id, url_consulta
from public.controle_migracao_lotes
where id_lote <> ''
--and data_hora_env::date = now()::date
and data_hora_env::date = '2020-11-23'