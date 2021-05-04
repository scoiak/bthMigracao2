-- ATENÇÃO! UTILIZAR O SEPARADOR INDICADO NA DOCUMENTAÇÃO ENTRE OS COMANDOS

--
--  1 - CRIA TABELA controle_migracao_registro
--
CREATE TABLE public.controle_migracao_registro
(
    sistema integer NOT NULL,
    tipo_registro text NOT NULL,
    hash_chave_dsk text NOT NULL UNIQUE,
    descricao_tipo_registro text NOT NULL,
    id_gerado integer,
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
    i_chave_dsk12 text,
    json_enviado text
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
--  4 - CRIA TABELA bth_indicadores_migracao
--
CREATE TABLE public.bth_indicadores_migracao
(
    sistema integer,
    dh_registro timestamp without time zone,
    tipo_registro text,
    qtd_registros integer,
    tempo_consulta_cloud numeric,
    tempo_extracao numeric,
    tempo_registro_controle numeric,
    tempo_registro_ocor numeric,
    tempo_montagem_lotes numeric,
    tempo_envio numeric,
    tempo_processamento_lotes numeric,
    tempo_desempacotamento_lotes numeric,
    tempo_total numeric

);

ALTER TABLE public.bth_indicadores_migracao
    OWNER to postgres;
%/%

CREATE OR REPLACE FUNCTION bth_delete_proc
(
	sis 		integer default 0,
	clicodigo 	text default '',
	ano_proc 	text default '',
	nro_proc 	text default ''
)
RETURNS text
LANGUAGE plpgsql AS $$
declare
	a text;
begin
	delete from public.controle_migracao_registro cmr
	where sistema = sis
	and i_chave_dsk1 = clicodigo
	and i_chave_dsk2 = ano_proc
	and i_chave_dsk3 = nro_proc
	and tipo_registro in (
		'processo', 'processo-forma-contratacao', 'processo-documento', 'processo-entidade', 'processo-despesa', 'processo-item', 'processo-lote',
		'processo-lote-item', 'processo-entidade-item', 'processo-convidado', 'processo-publicacao', 'processo-impugnacao', 'processo-sessao',
		'processo-participante', 'processo-participante-documento', 'processo-participante-proposta', 'processo-sessao-ata', 'processo-interposicao',
		'processo-ato-final', 'processo-revogacao', 'processo-proposta-pendente'
	);
	a := concat('Excluido processo ', nro_proc, '/' , ano_proc, ' (', clicodigo, ') das tabelas de controle.');
	return a;
END $$;
%/%

CREATE OR REPLACE FUNCTION bth_insert_cmr
(
	sis 			integer default 0,
	tipo_reg 		text default '',
	desc_tipo_reg 	text default '',
	id_gerado 		integer default 0,
	chave1 			text default '',
	chave2 			text default null,
	chave3 			text default null,
	chave4 			text default null,
	chave5 			text default null,
	chave6 			text default null,
	chave7 			text default null,
	chave8 			text default null,
	chave9 			text default null,
	chave10			text default null,
	chave11			text default null,
	chave12			text default null
)
RETURNS text
LANGUAGE plpgsql AS $$
declare
	h text;
begin
	h = md5(concat(sis, tipo_reg, chave1, chave2, chave3, chave4, chave5, chave6, chave7, chave8, chave9, chave10, chave11, chave12));
	insert into public.controle_migracao_registro values (sis, tipo_reg, h, desc_tipo_reg, id_gerado, chave1, chave2, chave3, chave4, chave5, chave6, chave7, chave8, chave9, chave10, chave11, chave12) on conflict do nothing;
	return h;
END $$;
%/%