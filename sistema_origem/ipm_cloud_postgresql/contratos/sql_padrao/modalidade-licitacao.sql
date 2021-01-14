-- Insere template nas tabelas de controle
/*
insert into public.controle_migracao_registro (sistema, tipo_registro, descricao_tipo_registro, id_gerado, i_chave_dsk1, i_chave_dsk2, i_chave_dsk3, hash_chave_dsk) values
(305, 'modalidade-licitacao', 'Modalidade Licitação (Padrão)', 7,  (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', {{clicodigo}}))), 1, null, '1'),
(305, 'modalidade-licitacao', 'Modalidade Licitação (Padrão)', 8,  (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', {{clicodigo}}))), 2, null, '2'),
(305, 'modalidade-licitacao', 'Modalidade Licitação (Padrão)', 9,  (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', {{clicodigo}}))), 3, null, '3'),
(305, 'modalidade-licitacao', 'Modalidade Licitação (Padrão)', 11, (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', {{clicodigo}}))), 5, null, '5'),
(305, 'modalidade-licitacao', 'Modalidade Licitação (Padrão)', 12,  (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', {{clicodigo}}))), 6, 1, '61'),
(305, 'modalidade-licitacao', 'Modalidade Licitação (Padrão)', 13,  (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', {{clicodigo}}))), 6, 2, '62'),
(305, 'modalidade-licitacao', 'Modalidade Licitação (Padrão)', 14,  (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', {{clicodigo}}))), 7, null, '7'),
(305, 'modalidade-licitacao', 'Modalidade Licitação (Padrão)', 15,  (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', {{clicodigo}}))), 8, null, '8');

-- Atualiza chaves dos registros do template
update public.controle_migracao_registro
set hash_chave_dsk = md5(concat(sistema, tipo_registro, i_chave_dsk1, i_chave_dsk2, i_chave_dsk3))
where tipo_registro = 'modalidade-licitacao'
and sistema = '305';
*/

-- Envia itens que não estão contidos no template
select
	row_number() over() as id,
	'305' as sistema,
	'modalidade-licitacao' as tipo_registro,
	*
from (
	select distinct
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', wco.tbprocesso.clicodigo))) as chave_dsk1, 
         modcodigo as chave_dsk1,         
         mintipopregao as chave_dsk3, 
         case modcodigo
            when 1 then 'Convite' 
            when 2 then 'Tomada de Preço' 
            when 3 then 'Concorrência'
            when 4 then 'Concurso'
            when 5 then 'Leilão'
            when 6 then (case mintipopregao when 1 then 'Pregão Presencial' when 2 then 'Pregão Eletrônico' else null end) 
            when 7 then 'Dispensa de Licitação'
            when 8 then 'Inexigibilidade de Licitação'
            else 'Outras Modalidades'
         end as descricao,
        case modcodigo
            when 1 then 'CONVITE' 
            when 2 then 'TOMADA_PRECO' 
            when 3 then 'CONCORRENCIA'
            when 4 then 'CONCURSO'
            when 5 then 'LEILAO'
            when 6 then (case mintipopregao when 1 then 'PREGAO_PRESENCIAL' when 2 then 'PREGAO_ELETRONICO' else null end) 
            when 7 then 'DISPENSA_LICITACAO'
            when 8 then 'INEXIGIBILIDADE'
            else 'OUTRO'
         end as modalidadeLegal,
         case modcodigo
            when 1 then 'CV' 
            when 2 then 'TP' 
            when 3 then 'CC'
            when 4 then 'CP'
            when 5 then 'LE'
            when 6 then (case mintipopregao when 1 then 'PR' when 2 then 'PE' else null end) 
            when 7 then 'DL'
            when 8 then 'IL'
            else 'OU'
         end as sigla,
         0 as valorCompras,
         0 as valorObras,
         (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 
         																							'modalidade-licitacao', 
         																							(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade',  wco.tbprocesso.clicodigo))), 
         																							modcodigo, 
         																							mintipopregao))) as id_gerado
   from wco.tbprocesso, wco.tbminuta
   where wco.tbprocesso.clicodigo = wco.tbminuta.clicodigo
   and wco.tbprocesso.pcsnro = wco.tbminuta.pcsnro
   and wco.tbprocesso.pcsano = wco.tbminuta.pcsano
   and  wco.tbprocesso.clicodigo = {{clicodigo}}
   order by 2 
) as tab 
where id_gerado is null