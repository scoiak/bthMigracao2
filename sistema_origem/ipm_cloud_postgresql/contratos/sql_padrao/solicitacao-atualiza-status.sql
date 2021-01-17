select
	row_number() over() as id,
	'305' as sistema,
	'solicitacao-atualiza-status' as tipo_registro,
	*
from (
	select
       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))) as chave_dsk1,
       rqcano as chave_dsk2,
       rqcnro as chave_dsk3,
       rqcsituacao,
       (case
       		when rqcsituacao in (1, 2, 3, 4, 11, 12, 13) then 'EM_EDICAO'
       		when rqcsituacao in (6, 8, 16) then 'CANCELADA'
       		when rqcsituacao in (7) then 'APROVADA'
       		when rqcsituacao in (9, 10, 14) then 'ATENDIDA'
       		else 'EM_EDICAO'
       end) as status_solicitacao,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'solicitacao', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))), rqcano, rqcnro))) as id_solicitacao,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'solicitacao-atualiza-status', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo))), rqcano, rqcnro)))  as id_gerado
	from wco.tbreqcomp
	where clicodigo = {{clicodigo}}
	and rqcano >= {{ano}}
	and date_part('year', rqcdata) = rqcano
	and not exists ( -- Evita finalizar solicitação com itens pendentes de migração
		select 1
		from public.controle_migracao_registro
		where tipo_registro = 'solicitacao-item'
		and i_chave_dsk1 = (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'entidade', clicodigo)))::varchar
		and i_chave_dsk2 = rqcano::varchar
		and i_chave_dsk3 = rqcnro::varchar
		and id_gerado is null
	)
	order by 2 desc, 3 desc
) tab
where id_gerado is null
and id_solicitacao is not null
and status_solicitacao <> 'EM_EDICAO'
--limit 3