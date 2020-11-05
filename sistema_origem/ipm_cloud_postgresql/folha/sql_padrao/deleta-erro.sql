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

DO $$ DECLARE
    r RECORD; 
BEGIN       
	FOR r in (select distinct schemaname from pg_catalog.pg_tables where schemaname not like 'pg_catalog') loop
		RAISE NOTICE 'Removido: %',r.schemaname;
		-- EXECUTE 'DROP SCHEMA ' || r.schemaname || ' CASCADE'; 
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
begin
	delete from public.controle_migracao_registro where tipo_registro = 'tipo-ato';

	delete from public.controle_migracao_lotes where tipo_registro = 'pessoa-fisica';	
	delete from public.controle_migracao_registro_ocor where tipo_registro = 'pessoa-fisica';
END $$;
