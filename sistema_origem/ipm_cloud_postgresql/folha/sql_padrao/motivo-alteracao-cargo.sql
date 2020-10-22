select * from (
	select distinct '300' as sistema,
       'motivo-alteracao-cargo' as tipo_registro,
       alccodigo as id,
	   alcdescricao as chave_dsk1,
       alcdescricao as descricao,
	   public.bth_get_situacao_registro('300' , 'motivo-alteracao-cargo',  alcdescricao) as situacao_registro
  from wfp.tbaltcargo
order by alccodigo
) tab
where situacao_registro in (0)