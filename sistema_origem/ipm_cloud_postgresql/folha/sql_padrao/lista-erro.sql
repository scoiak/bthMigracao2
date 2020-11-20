SELECT
    reg.tipo_registro AS tipoRegistro,
    COUNT(*) AS totalErros
FROM 
    public.controle_migracao_registro as reg
left join 
    public.controle_migracao_registro_ocor as ocor on ocor.hash_chave_dsk = reg.hash_chave_dsk 
WHERE 
    reg.id_gerado IS NULL
OR
    ocor.mensagem_erro IS NOT NULL
GROUP BY 
    reg.tipo_registro
ORDER BY
    COUNT(*) DESC


SELECT 
    reg.tipo_registro AS tipoRegistro,
    reg.i_chave_dsk1 AS primeiraChave,
    reg.i_chave_dsk2 AS segundaChave,   
    reg.i_chave_dsk3 AS terceiraChave,   
    reg.i_chave_dsk4 AS quartaChave,   
    ocor.hash_chave_dsk as codigo,
    reg.id_gerado AS idGerado,
    ocor.situacao AS estadoItem,
    ocor.mensagem_erro AS mensagemErro,
    --ocor.json_enviado AS jsonEnviado,
    reg.json_enviado AS jsonEnviado
    ,lot.conteudo_json AS jsonLote    
FROM 
    public.controle_migracao_registro as reg
left JOIN 
    public.controle_migracao_registro_ocor as ocor on ocor.hash_chave_dsk = reg.hash_chave_dsk 
left JOIN 
    public.controle_migracao_lotes as lot on lot.id_lote = ocor.id_integracao 
WHERE 
	--ocor.hash_chave_dsk = 'a63ed5889527c9a01211c50e277646f6' AND
	--reg.hash_chave_dsk = 'a63ed5889527c9a01211c50e277646f6' AND
    ocor.id_gerado IS null
and 
	reg.id_gerado is null
AND
    --reg.tipo_registro = 'afastamento'
    reg.tipo_registro = 'matricula'
AND 
    ocor.i_sequencial = 
        (
            SELECT 
                max(aux.i_sequencial) 
            FROM 
                public.controle_migracao_registro_ocor as aux 
            WHERE 
                aux.hash_chave_dsk = reg.hash_chave_dsk
        )
ORDER by
	length(reg.json_enviado) asc,
      reg.i_chave_dsk1  ASC,
      reg.i_chave_dsk2 ASC,
    reg.id_gerado  ASC