select * from (
	select '300' as sistema,
       'natureza-texto-juridico' as tipo_registro,
    	tctcodigo as id,
		tctdescricao as chave_dsk1,
        tctdescricao as nome,
		public.bth_get_situacao_registro('300', 'natureza-texto-juridico', tctdescricao) as situacao_registro
   from wlg.tbcategoriatexto
) tab
where situacao_registro in (0)