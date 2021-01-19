select		
row_number() over() as id,	
*
from (
	select
		distinct
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))) as entidade,
		(CAST(ato.txjnumero as text) || '/' || CAST(ato.txjano as text)) as numeroOficial,		 
       	coalesce(mvto.movdata,pub.pubdata) as dataCriacao,
	    mvto.movdata as dataVigorar,
       	mvto.movdata as dataResolucao,
		pub.pubdata as dataPublicacao,
       	left(ato.txjementa,32000) as ementa,
       	replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(upper(left(cat.tctdescricao, 40)),'é','É'),'á','Á'),'ó','Ó'),'ã','Ã'),'õ','Õ'),'ç','Ç'),'â','Â'),'à','À'),'í','Í'),'ê','Ê'),
	    (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','natureza-texto-juridico', replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(upper(cat.tctdescricao),'é','É'),'á','Á'),'ó','Ó'),'ã','Ã'),'õ','Õ'),'ç','Ç'),'â','Â'),'à','À'),'í','Í'),'ê','Ê')))) as naturezaTextoJuridico,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300','tipo-ato', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', clicodigo))), replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(upper(left(cat.tctdescricao, 40)),'é','É'),'á','Á'),'ó','Ó'),'ã','Ã'),'õ','Õ'),'ç','Ç'),'â','Â'),'à','À'),'í','Í'),'ê','Ê')))) as tipo		
	from wlg.tbtextojuridico ato
	left join wlg.tbcategoriatexto cat on (cat.tctcodigo = ato.tctcodigo)
	left join wlg.tbmovimentotexto mvto on (mvto.txjcodigo = ato.txjcodigo and mvto.movtipo = 2)
	left join wlg.tbpublicacao pub on (pub.txjcodigo = ato.txjcodigo)
) tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', numeroOficial, tipo))) is null
and datacriacao is not null
