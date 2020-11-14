select
	*
from (
    select distinct
		 1 as id,
		 tbconcurso.clicodigo,
		 (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'entidade', {{clicodigo}}))) as id_entidade,
		 --(select txjementa from wlg.tbtextojuridico where tbconcurso.txjcodigo = tbtextojuridico.txjcodigo) as descricao,
		 CASE (select asscodigo from wlg.tbtextojuridico where tbconcurso.txjcodigo = tbtextojuridico.txjcodigo)
			 WHEN 60 THEN 'PROCESSO_SELETIVO'
			 WHEN 58 THEN 'CONCURSO_PUBLICO'
		 END as tipoRecrutamento,
		 (CASE (select asscodigo from wlg.tbtextojuridico where tbconcurso.txjcodigo = tbtextojuridico.txjcodigo)
			 WHEN 60 THEN 'Processo seletivo '
			 WHEN 58 THEN 'Concurso público '
		 END) || (CAST(ato.txjnumero as text) || '/' || CAST(ato.txjano as text)) as descricao,
		tcodataedital as dataInicialInscricao,
		 tcodataedital as dataFinalInscricao,
		 tcodataedital as dataProrrogacao,
		 tcodatahomolog as dataHomologacao,
		 tcodatavalidade as dataValidade,
		 tcodatavalidade as dataProrrogacaoValidade,
		 tcodataedital as dataInicialInscricaoPcd,
		 tcodataedital as dataFinalInscricaoPcd,
		 tcodatahomolog as dataEncerramento,
		 (CAST(ato.txjnumero as text) || '/' || CAST(ato.txjano as text)) as numeroEdital,
		 coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'ato', (select concat(tj.txjnumero, '/', tj.txjano) from wlg.tbtextojuridico tj where tj.txjcodigo = ato.txjcodigo), (select cat.tctdescricao from wlg.tbtextojuridico tj inner join wlg.tbcategoriatexto cat on cat.tctcodigo = tj.tctcodigo where tj.txjcodigo = ato.txjcodigo limit 1)))),0) as ato,
		 tcopercendef as percentualPcd
	from wfp.tbconcurso
	inner join wlg.tbtextojuridico ato on (ato.txjcodigo = tbconcurso.txjcodigo)
	where odomesano = (select distinct max(odomesano) from wfp.tbconcurso)
) tab
where (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('300', 'concurso', id_entidade, numeroEdital, tipoRecrutamento))) is null