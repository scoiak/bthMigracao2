insert into public.controle_migracao_registro (sistema, tipo_registro, descricao_tipo_registro, id_gerado, i_chave_dsk1, hash_chave_dsk) values
('305', 'tipo-responsavel-contrato', 'Cadastro de Tipos de Responsáveis', 5, '2', md5(concat('305', 'tipo-responsavel-contrato', '2'))),
('305', 'tipo-responsavel-contrato', 'Cadastro de Tipos de Responsáveis', 7, '3', md5(concat('305', 'tipo-responsavel-contrato', '3'))),
('305', 'tipo-responsavel-contrato', 'Cadastro de Tipos de Responsáveis', 13, '4', md5(concat('305', 'ttipo-responsavel-contrato', '4'))),
('305', 'tipo-responsavel-contrato', 'Cadastro de Tipos de Responsáveis', 7, '5', md5(concat('305', 'tipo-responsavel-contrato', '5')))
on conflict do nothing;

select
	row_number() over() as id,
	'305' as sistema,
	'tipo-responsavel-contrato' as tipo_registro,
	*
from (
	select distinct
	tfscodigo,
	   (case tfscodigo
	       when 1 then 'Membro'
		   when 2 then 'Gestor'
		   else 'Fiscal' end) as descricao,
	    (case tfscodigo
	       when 1 then 'OUTROS'
		   when 2 then 'GESTOR'
		   else 'FISCAL' end) as classificacao,
		md5(concat(305,'tipo-responsavel-contrato', tfscodigo)) as hash,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305,'tipo-responsavel-contrato', tfscodigo))) as id_gerado
	from wco.tbfiscal where tfscodigo is not null
) tab
where id_gerado is null