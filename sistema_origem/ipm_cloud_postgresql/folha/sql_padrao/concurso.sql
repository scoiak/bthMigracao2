select
	'300' as sistema,
	'concurso' as tipo_registro,
	*
from (
	select distinct  '300' as sistema,
		 'concurso' as tipo_registro,
		 --clicodigo as chave_dsk1,
		 --txtcodigo as chave_dsk2,
		 (select txjementa from wlg.tbtextojuridico where tbconcurso.txjcodigo = tbtextojuridico.txjcodigo) as descricao,
		 CASE (select asscodigo from wlg.tbtextojuridico where tbconcurso.txjcodigo = tbtextojuridico.txjcodigo)
			 WHEN 60 THEN 'PROCESSO_SELETIVO'
			 WHEN 58 THEN 'CONCURSO_PUBLICO'
		 END as tipoRecrutamento,
		 tcodataedital as dataInicialInscricao,
		 tcodataedital as dataFinalInscricao,
		 tcodataedital as dataProrrogacao,
		 tcodatahomolog as dataHomologacao,
		 tcodatavalidade as dataValidade,
		 tcodatavalidade as dataProrrogacaoValidade,
		 tcodataedital as dataInicialInscricaoPcd,
		 tcodataedital as dataFinalInscricaoPcd,
		 tcodatahomolog as dataEncerramento,
		 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (CAST(ato.txjnumero as text) || '/' || CAST(ato.txjano as text))))) as ato,
		 tcopercendef as percentualPcd
	from wfp.tbconcurso
	inner join wlg.tbtextojuridico ato on (ato.txjcodigo = tbconcurso.txjcodigo)
) tab