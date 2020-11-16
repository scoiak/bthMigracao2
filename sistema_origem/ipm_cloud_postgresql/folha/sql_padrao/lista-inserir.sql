COPY public.controle_migracao_registro
FROM 'C:\Users\thiago.julio\Downloads\csv_cmr.csv'
DELIMITER '|'
CSV HEADER;

SELECT tipo_registro, 
       count(tipo_registro) as total_registros,
	   sum(case coalesce(id_gerado, 0) when 0 then 1 else 0 end) as erros
FROM public.controle_migracao_registro
GROUP BY tipo_registro
ORDER BY 3 DESC, 1 asc
