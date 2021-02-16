select
	row_number() over() as id,
	'305' as sistema,
	'solicitacao' as tipo_registro,
	*
from (
	select
       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))) as chave_dsk1,
       rqcano as chave_dsk2,
       rqcnro as chave_dsk3,
       (select cncclassif from wun.tbcencus where wun.tbcencus.cnccodigo = wco.tbreqcomp.cnccodigo) as mask,
       wco.tbreqcomp.cnccodigo as codigo_organograma,
       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'centro-custo', rqcano, replace((select cncclassif from wun.tbcencus where wun.tbcencus.cnccodigo = wco.tbreqcomp.cnccodigo),'.','')))) as id_organograma,                    
       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'local-entrega', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))), loccodigo))) as id_local_entrega,
       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))) as id_entidade_gestora,      
       (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305, 'parametro-exercicio', rqcano))) as id_parametro_exercicio,     
       rqcnro as codigo,
       rqcdata as data_solicitacao,
       'Não informado' as nome_solicitante,
       left(coalesce(trim(rqcobservacao),'Não informado'), 100) as assunto,
       'MATERIAL' as tipo_necessidade,
       left(coalesce(trim(rqcobservacao),'Não informado'), 500) as objeto,
       trim(left(coalesce(rqcobservacao,''),500)) as justificativa,
       trim(left(coalesce(rqcobservacao,''),500)) as observacao,
       0 as existe_pc,
	   0 as existe_cotacao,
	   0 as existe_compra_dir,
       'EM_EDICAO' as status_solicitacao,
	   (select count(*) from wco.tbitemreqcomp where wco.tbitemreqcomp.rqcano = wco.tbreqcomp.rqcano and wco.tbitemreqcomp.clicodigo = wco.tbreqcomp.clicodigo and wco.tbitemreqcomp.rqcnro = wco.tbreqcomp.rqcnro) as quantidade_itens,
	   'OK' as situacao_cadastral,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'solicitacao', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))), rqcano, rqcnro))) as id_gerado
	from wco.tbreqcomp
	where clicodigo = {{clicodigo}}
	and rqcano = {{ano}}
	and date_part('year', rqcdata) = rqcano
	order by 2 desc, 3 desc
) tab
where id_gerado is null
and id_organograma is not null
and id_local_entrega is not null
and id_entidade_gestora is not null
and id_parametro_exercicio is not null
--limit 1