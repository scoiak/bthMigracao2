/* CADASTRO DO TEMPLATE
insert into public.controle_migracao_registro (sistema, tipo_registro, descricao_tipo_registro, id_gerado, i_chave_dsk1, hash_chave_dsk) values
('305', 'tipo-sancao', 'Cadastro de Tipos de Sanção', 12, '1', md5(concat('305', 'tipo-sancao', '1'))), --Suspensão
('305', 'tipo-sancao', 'Cadastro de Tipos de Sanção', 11, '2', md5(concat('305', 'tipo-sancao', '2'))), --Inidoneidade
('305', 'tipo-sancao', 'Cadastro de Tipos de Sanção', 13, '4', md5(concat('305', 'tipo-sancao', '4'))) 	--Multa
on conflict do nothing;
*/

select
	row_number() over() as id,
	'305' as sistema,
	'tipo-sancao' as tipo_registro,
	*
from (
	select distinct
	sustipo,
	   (case sustipo
	       when 1 then 'Suspensão'
		   when 2 then 'Inidoneidade'
		   when 3 then 'Advertência'
		   else 'Multa' end) as classificacao,
	    (case sustipo
	       when 1 then 'SUSPENSAO'
		   when 2 then 'INIDONEIDADE'
		   when 3 then 'ADVERTENCIA'
		   else 'MULTA' end) as descricao,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,'tipo-sancao', sustipo))) as id_gerado
	from wco.tbsuspensao
	where sustipo is not null
) tab
where id_gerado is null