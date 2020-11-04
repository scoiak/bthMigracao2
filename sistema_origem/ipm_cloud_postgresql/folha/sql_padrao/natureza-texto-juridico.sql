select * from (
	select 
    	tctcodigo as id,
		tctdescricao as descricao
   from wlg.tbcategoriatexto
) tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'natureza-texto-juridico',descricao))) is null