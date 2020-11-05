SELECT schemaname as esquema,relname as tabela FROM pg_stat_user_tables where relname like '%enca%' order by 1,2;


SELECT schemaname as esquema,relname as tabela,n_live_tup as tamanho FROM pg_stat_user_tables order by 1,2;


DO $$ DECLARE
    r RECORD;    
   	q INTEGER := 0;  
BEGIN	
    FOR r IN (SELECT schemaname,relname,n_live_tup FROM pg_stat_user_tables WHERE n_live_tup = 0) LOOP
    	q := q + 1;
    	RAISE NOTICE 'Removido: % [%][%]',r.relname,q,r.n_live_tup;
   		-- EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.schemaname) || '.' || quote_ident(r.relname) || ' CASCADE';
   		COMMIT;   		
    END LOOP;	
END $$;


select count(*),tipo_registro,sistema from public.controle_migracao_registro group by tipo_registro,sistema
update public.controle_migracao_registro set tipo_registro = 'pais',hash_chave_dsk = md5(concat('300','pais',i_chave_dsk1,i_chave_dsk2,i_chave_dsk3)) where tipo_registro = 'pais'
update public.controle_migracao_registro set tipo_registro = 'estado',hash_chave_dsk = md5(concat('300','estado',i_chave_dsk1,i_chave_dsk2,i_chave_dsk3)) where tipo_registro = 'estado'
update public.controle_migracao_registro set tipo_registro = 'municipio',hash_chave_dsk = md5(concat('300','municipio',i_chave_dsk1,i_chave_dsk2,i_chave_dsk3)) where tipo_registro = 'municipio'
update public.controle_migracao_registro set tipo_registro = 'banco',hash_chave_dsk = md5(concat('300','banco',i_chave_dsk1,i_chave_dsk2,i_chave_dsk3)) where tipo_registro = 'banco'
commit;


DO $$ DECLARE
    tipo_registro alias for 'nivel-salarial';
BEGIN
	delete from public.controle_migracao_lotes where tipo_registro = 'nivel-salarial';
	delete from public.controle_migracao_registro where tipo_registro = 'nivel-salarial';
	delete from public.controle_migracao_registro_ocor where tipo_registro = 'nivel-salarial';
END $$;


-- Total de registros enviados/erros por assunto
SELECT tipo_registro,
       COUNT(tipo_registro) AS total,
	   SUM(CASE COALESCE(id_gerado, 0)
	        WHEN 0 THEN 1
	        ELSE 0
	        END) AS erros
FROM public.controle_migracao_registro
GROUP BY tipo_registro
ORDER BY 3 DESC, 2 DESC;


-- Verifica qual o erro vinculado a cada registro
select distinct
	reg.tipo_registro,
	reg.id_gerado,
	reg.i_chave_dsk1,
	reg.i_chave_dsk2,
	reg.i_chave_dsk3,
	ocor.mensagem_erro
from public.controle_migracao_registro reg
left join public.controle_migracao_registro_ocor ocor on (ocor.hash_chave_dsk = reg.hash_chave_dsk)
where reg.tipo_registro = 'vinculo-empregaticio'
and reg.id_gerado is null;