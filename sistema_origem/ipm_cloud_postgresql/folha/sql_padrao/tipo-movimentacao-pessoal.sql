select 
(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-movimentacao-pessoal', entidade, descricao, classificacao))) as idCloud,
*
from (
	select
		codigo as id,
		codigo as codigo,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', 11968))) as entidade,
		(case  codigo
			when 1  then 'Movimento de Admissão'
			 when 2  then 'Movimento de Afastamento'
			 when 3  then 'Concessão de Licença-Prêmio'
			 when 4  then 'Movimento de Demissão'
			 when 5  then 'Movimento de Alteração Salarial'
			 when 6  then 'Movimento de Proc. Aposent. e Pensões'
			 when 7  then 'Movimento de Pensionistas'
			 when 8  then 'Movimento de Transferência(Cedido)'
			 when 9  then 'Movimento de Transferência(Recebido)'
			 when 10 then 'Movimento de Férias'
			 when 11 then 'Movimento de Falecimento'
			 when 12 then 'Movimento de Função Gratificada'
			 when 13 then 'Movimento de Aposentadoria'
			 when 14 then 'Movimento de Adicionais'
			 when 15 then 'Movimento de Reintegração'
			 when 16 then 'Movimento de Reversão de Aposentadoria'
			 when 17 then 'Movimento de Usufruto Licença-Prêmio'
			 when 18 then 'Movimento de Advertência'
			 when 19 then 'Movimento de Elogio'
			 when 20 then 'Movimento de Suspensão'
			 when 21 then 'Movimento de Prorrogação de Contrato'
			 when 22 then 'Movimento de Demissão (Contrato Temporário)'
			 when 23 then 'Movimento de Averbação de tempo de serviço'
			 when 24 then 'Movimento de Averbação de adicional'
			 when 25 then 'Movimento de Averbação de licença-prêmio'
			 when 26 then 'Movimento de Saída do cargo'
		     else 'Nenhuma movimentação de pesssoal'
	   end) as descricao,
	   (case  codigo
			 when 1  then 'ADMISSAO'
			 when 2  then 'AFASTAMENTOS'
			 when 3  then 'CONCESSAO_LICENCA_PREMIO'
			 when 4  then 'DEMISSAO'
			 when 5  then 'ALTERACAO_SALARIAIS'
			 when 6  then 'PROC_APOSENT_PENSOES'
			 when 7  then 'PENSIONISTAS'
			 when 8  then 'TRANSFERENCIA_CEDIDO'
			 when 9  then 'TRANSFERENCIA_RECEBIDO'
			 when 10 then 'FERIAS'
			 when 11 then 'FELECIMENTO'
			 when 12 then 'FUNCAO_GRATIFICADA'
			 when 13 then 'APONSENTADORIA'
			 when 14 then 'ADICIONAIS'
			 when 15 then 'REINTEGRACAO'
			 when 16 then 'REVISAO_APOSENTADORIA'
			 when 17 then 'USUFRUTO_LICENCA_PREMIO'
			 when 18 then 'ADVERTENCIA'
			 when 19 then 'ELOGIO'
			 when 20 then 'SUSPENSAO'
			 when 21 then 'PRORROGACAO_CONRATO'
			 when 22 then 'DEMISSAO_CONTRATO_TEMPORARIO'
			 when 23 then 'AVERBACAO_TEMPO_SERVICO'
			 when 24 then 'AVERBACAO_ADICIONAL'
			 when 25 then 'AVERBACAO_LICENCA_PREMIO'
			 when 26 then 'SAIDA_CARGO'
		     else 'NENHUMA'
		  end ) as classificacao
	  from (select codigo from generate_series (1,26) as codigo) as aux) as xua
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'tipo-movimentacao-pessoal', entidade, descricao, classificacao))) is null