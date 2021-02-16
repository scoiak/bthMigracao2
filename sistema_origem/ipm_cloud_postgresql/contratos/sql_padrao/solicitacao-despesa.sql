select
	row_number() over() as id,
	'305' as sistema,
	'solicitacao-despesa' as tipo_registro,
	id_entidade as chave_dsk1,
	rqcano as chave_dsk2,
	rqcnro as chave_dsk3,
	'@' as chave_dsk4, -- Necessário esse separado para evitar ambiguidade nas chaves
	dotcodigo as chave_dsk5,
	*
from (
	select distinct
        clicodigo,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))) as id_entidade,
        rqcano,
        rqcnro,
        dotcodigo,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'solicitacao', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))), rqcano, rqcnro))) as id_solicitacao,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'parametro-exercicio', rqcano))) as id_exercicio,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'despesa', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', clicodigo))), rqcano, dotcodigo))) as id_despesa,
        0.01 as valor_estimado,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(/* sistema 		*/ '305',
        																						   /* tipo_registro */ 'solicitacao-despesa',
        																						   /* id_entidade 	*/ (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))),
        																						   /* ano 			*/ rqcano,
        																						   /* nroSolic.		*/ rqcnro,
        																						   /* cod. dot. 	*/ dotcodigo))) as id_gerado
	from  wco.tbdotreq
	where rqcano = {{ano}}
	and clicodigo = {{clicodigo}}
	order by clicodigo, rqcano desc, rqcnro desc, dotcodigo asc
) as tab
where id_gerado is null
and id_solicitacao is not null
and id_exercicio is not null
and id_despesa is not null
--limit 1