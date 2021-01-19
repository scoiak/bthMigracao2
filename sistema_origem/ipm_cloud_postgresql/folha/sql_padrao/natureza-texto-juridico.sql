select * from (
	select 
    	tctcodigo as id,
    	tctcodigo as codigo,
		replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(upper(tctdescricao),'é','É'),'á','Á'),'ó','Ó'),'ã','Ã'),'õ','Õ'),'ç','Ç'),'â','Â'),'à','À'),'í','Í'),'ê','Ê') as descricao
   from wlg.tbcategoriatexto
) tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'natureza-texto-juridico',descricao))) is null