select
	row_number() over() as id,
	'305' as sistema,
	'tipo-objeto' as tipo_registro,
	*
from (
	select distinct
	   (case mintipoobjeto
	       when 1 then 'COMPRAS_SERVICOS'
		   when 2 then 'OBRAS_SERVICOS'
		   when 3 then 'CONCESSAO'
		   when 4 then 'ALIENACAO'
		   when 5 then 'PERMISSAO'
		   when 6 then 'AQUISICAO_BENS'
		   when 7 then 'PRESTACAO_SERVICOS'
		   else 'COMPRAS_SERVICOS' end) as tipo,
	   (case mintipoobjeto
	       when 1 then 'Compras e Outros Serviços'
		   when 2 then 'Obras e Serviços de Engenharia'
		   when 3 then 'Concessoes e Permissões de Serviços Públicos'
		   when 4 then 'Alienação de Bens'
		   when 5 then 'Concessão e Permissão de Uso de Bem Público'
		   when 6 then 'Aquisição de Bens'
		   when 7 then 'Contratação de Serviços'
		   else 'Compras Outros Serviços' end) as descricao,
	   false as bem_publico,
	   (select id_gerado from public.controle_migracao_registro	where hash_chave_dsk = md5(concat(305,'tipo-objeto', (case mintipoobjeto
																												       when 1 then 'Compras e Outros Serviços'
																													   when 2 then 'Obras e Serviços de Engenharia'
																													   when 3 then 'Concessoes e Permissões de Serviços Públicos'
																													   when 4 then 'Alienação de bens'
																													   when 5 then 'Concessão e Permissão de Uso de Bem Público'
																													   when 6 then 'Aquisição de bens'
																													   when 7 then 'Contratação de Serviços'
																													   else 'Compras Outros Serviços' end)))) as id_gerado
	from wco.tbminuta
) tab
where id_gerado is null