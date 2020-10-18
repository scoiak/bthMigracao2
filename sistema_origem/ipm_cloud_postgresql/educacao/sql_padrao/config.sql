-- ATENÇÃO! UTILIZAR O SEPARADOR INDICADO NA DOCUMENTAÇÃO ENTRE OS COMANDOS

--
--  1 - CRIA TABELA controle_migracao_registro
--
CREATE TABLE public.controle_migracao_registro
(
    sistema integer NOT NULL,
    tipo_registro text NOT NULL,
    hash_chave_dsk text NOT NULL,
    descricao_tipo_registro text NOT NULL,
    id_gerado text,
    i_chave_dsk1 text NOT NULL,
    i_chave_dsk2 text,
    i_chave_dsk3 text,
    i_chave_dsk4 text,
    i_chave_dsk5 text,
    i_chave_dsk6 text,
    i_chave_dsk7 text,
    i_chave_dsk8 text,
    i_chave_dsk9 text,
    i_chave_dsk10 text,
    i_chave_dsk11 text,
    i_chave_dsk12 text
);
%/%

ALTER TABLE public.controle_migracao_registro
    OWNER to postgres;
%/%
--
--  2 - CRIA TABELA controle_migracao_registro_ocor
--
CREATE TABLE public.controle_migracao_registro_ocor
(
    i_sequencial integer NOT NULL,
    hash_chave_dsk text NOT NULL,
    sistema integer NOT NULL,
    tipo_registro text NOT NULL,
    id_gerado text,
    origem smallint NOT NULL,
    situacao smallint NOT NULL,
    resolvido smallint NOT NULL,
    i_sequencial_lote numeric,
    id_integracao text NOT NULL,
    mensagem_erro text,
    mensagem_ajuda text,
    json_enviado text,
    id_existente text
);
%/%

ALTER TABLE public.controle_migracao_registro_ocor
    OWNER to postgres;
%/%

--
--  3 - CRIA TABELA controle_migracao_lotes
--
CREATE TABLE public.controle_migracao_lotes
(
    i_sequencial integer NOT NULL,
    sistema integer NOT NULL,
    tipo_registro text NOT NULL,
    data_hora_env timestamp without time zone NOT NULL,
    data_hora_ret timestamp without time zone,
    usuario text  NOT NULL,
    url_consulta text NOT NULL,
    Status smallint,
    id_lote text,
    conteudo_json text
);
%/%

ALTER TABLE public.controle_migracao_lotes
    OWNER to postgres;
%/%

--
--  4 - CRIA FUNCTION  bth_get_hash_chave_dsk
--
CREATE OR REPLACE FUNCTION bth_get_hash_chave_dsk
(
	a_sistema smallint, a_tipo_registro text, a_i_chave_dsk1 char, a_i_chave_dsk2 text, a_i_chave_dsk3 text,
	a_i_chave_dsk4 text, a_i_chave_dsk5 text, a_i_chave_dsk6 text, a_i_chave_dsk7 text, a_i_chave_dsk8 text,
	a_i_chave_dsk9 text, a_i_chave_dsk10 text, a_i_chave_dsk11 text, a_i_chave_dsk12 text
)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	w_chave text = '' ;
	w_separador text = '#&@';

	w_vl_1 text = '';
	w_vl_2 text = '';
	w_vl_3 text = '';
	w_vl_4 text = '';
	w_vl_5 text = '';
	w_vl_6 text = '';
	w_vl_7 text = '';
	w_vl_8 text = '';
	w_vl_9 text = '';
	w_vl_10 text = '';
	w_vl_11 text = '';
	w_vl_12 text = '';
BEGIN
	IF a_i_chave_dsk1 IS NULL THEN w_vl_1 := ''; ELSE w_vl_1 := CONCAT(a_i_chave_dsk1, w_separador); END IF;
	IF a_i_chave_dsk2 IS NULL THEN w_vl_2 := ''; ELSE w_vl_2 := CONCAT(a_i_chave_dsk2, w_separador); END IF;
	IF a_i_chave_dsk3 IS NULL THEN w_vl_3 := ''; ELSE w_vl_3 := CONCAT(a_i_chave_dsk3, w_separador); END IF;
	IF a_i_chave_dsk4 IS NULL THEN w_vl_4 := ''; ELSE w_vl_4 := CONCAT(a_i_chave_dsk4, w_separador); END IF;
	IF a_i_chave_dsk5 IS NULL THEN w_vl_5 := ''; ELSE w_vl_5 := CONCAT(a_i_chave_dsk5, w_separador); END IF;
	IF a_i_chave_dsk6 IS NULL THEN w_vl_6 := ''; ELSE w_vl_6 := CONCAT(a_i_chave_dsk6, w_separador); END IF;
	IF a_i_chave_dsk7 IS NULL THEN w_vl_7 := ''; ELSE w_vl_7 := CONCAT(a_i_chave_dsk7, w_separador); END IF;
	IF a_i_chave_dsk8 IS NULL THEN w_vl_8 := ''; ELSE w_vl_8 := CONCAT(a_i_chave_dsk8, w_separador); END IF;
	IF a_i_chave_dsk9 IS NULL THEN w_vl_9 := ''; ELSE w_vl_9 := CONCAT(a_i_chave_dsk9, w_separador); END IF;
	IF a_i_chave_dsk10 IS NULL THEN w_vl_10 := ''; ELSE w_vl_10 := CONCAT(a_i_chave_dsk10, w_separador); END IF;
	IF a_i_chave_dsk11 IS NULL THEN w_vl_11 := ''; ELSE w_vl_11 := CONCAT(a_i_chave_dsk11, w_separador); END IF;
	IF a_i_chave_dsk12 IS NULL THEN w_vl_12 := ''; ELSE w_vl_12 := CONCAT(a_i_chave_dsk12, w_separador); END IF;
	w_chave := CONCAT(w_vl_1, w_vl_2, w_vl_3, w_vl_4, w_vl_5, w_vl_6, w_vl_7, w_vl_8, w_vl_9, w_vl_10, w_vl_11, w_vl_12);
	return md5(CONCAT(a_sistema, w_separador, a_tipo_registro, w_separador, w_chave));
END $$
%/%

--
--  5 - CRIA FUNCTION  bth_get_id_gerado
--
CREATE OR REPLACE FUNCTION bth_get_id_gerado
(
	a_sistema smallint, a_tipo_registro text, a_i_chave_dsk1 text, a_i_chave_dsk2 text, a_i_chave_dsk3 text,
	a_i_chave_dsk4 text, a_i_chave_dsk5 text, a_i_chave_dsk6 text, a_i_chave_dsk7 text, a_i_chave_dsk8 text,
	a_i_chave_dsk9 text, a_i_chave_dsk10 text, a_i_chave_dsk11 text, a_i_chave_dsk12 text
)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	w_id_gerado text = '' ;
	w_hash_chave_dsk text = '';
BEGIN
	w_hash_chave_dsk := bth_get_hash_chave_dsk(a_sistema, a_tipo_registro, a_i_chave_dsk1, a_i_chave_dsk2, a_i_chave_dsk3,
				  							a_i_chave_dsk4, a_i_chave_dsk5, a_i_chave_dsk6, a_i_chave_dsk7, a_i_chave_dsk8,
					 						a_i_chave_dsk9, a_i_chave_dsk10, a_i_chave_dsk11, a_i_chave_dsk12);

	SELECT FIRST id_gerado
	INTO w_id_gerado
	FROM controle_migracao_registro
	WHERE hash_chave_dsk = w_hash_chave_dsk;

	RETURN id_gerado;
END $$
%/%

--
--  6 - CRIA FUNCTION  bth_get_id_gerado
--
CREATE OR REPLACE FUNCTION bth_get_situacao_registro
(
	a_sistema smallint, a_tipo_registro text, a_i_chave_dsk1 text, a_i_chave_dsk2 text, a_i_chave_dsk3 text,
	a_i_chave_dsk4 text, a_i_chave_dsk5 text, a_i_chave_dsk6 text, a_i_chave_dsk7 text, a_i_chave_dsk8 text,
	a_i_chave_dsk9 text, a_i_chave_dsk10 text, a_i_chave_dsk11 text, a_i_chave_dsk12 text
)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	w_situacao text = '' ;
	w_hash_chave_dsk text = '';
BEGIN
	w_hash_chave_dsk := bth_get_hash_chave_dsk(a_sistema, a_tipo_registro, a_i_chave_dsk1, a_i_chave_dsk2, a_i_chave_dsk3,
				  							a_i_chave_dsk4, a_i_chave_dsk5, a_i_chave_dsk6, a_i_chave_dsk7, a_i_chave_dsk8,
					 						a_i_chave_dsk9, a_i_chave_dsk10, a_i_chave_dsk11, a_i_chave_dsk12);

	IF EXISTS(SELECT id_gerado
			  FROM controle_migracao_registro
			  WHERE hash_chave_dsk = w_hash_chave_dsk
			  AND id_gerado IS NOT NULL)
	THEN
		w_situacao := 4;
	ELSE
		w_situacao := (
			SELECT CASE situacao
				WHEN situacao = 1 AND resolvido = 1 THEN 1
				WHEN situacao = 4 THEN 2
				WHEN situacao = 2 AND resolvido = 1 THEN 3
				ELSE 0
			END
			FROM controle_migracao_registro_ocor
			WHERE hash_chave_dsk = w_hash_chave_dsk
			AND ((situacao = 1 AND resolvido = 1) OR situacao = 4 OR (situacao = 2 AND resolvido = 1))
		);

	RETURN w_situacao;
END $$
%/%