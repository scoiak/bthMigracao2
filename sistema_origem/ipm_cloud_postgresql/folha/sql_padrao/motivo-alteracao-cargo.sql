select * from (
	select
       alccodigo as id,	   
       alcdescricao as descricao	   
  from wfp.tbaltcargo
  where odomesano = 202009
order by alccodigo
) tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'motivo-alteracao-cargo',descricao))) is null