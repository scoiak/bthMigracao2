select
    row_number() over() as id,
    '305' as sistema,
    'processos-forma-contratacao' as tipo_registro,
    concat(nro_minuta, '/', ano_minuta) as minuta_formatado,
    concat(nro_processo, '/', ano_processo) as proc_formatado,
    concat(nro_sequencial, '/', ano_sequencial) as lic_formatado,
    *
from (
    select
        p.clicodigo,
        m.minano as ano_minuta,
        m.minnro as nro_minuta,
        m.pcsano as ano_processo,
        m.pcsnro as nro_processo,
        coalesce(lic.licano, m.minano) as ano_sequencial,
        coalesce(lic.licnro, m.minnro) as nro_sequencial,
        p.modcodigo,
        (select mo.moddescricao from wco.tbmodalidade mo where mo.modcodigo = p.modcodigo) as desc_modalidade,
        p.pcsano as parametro_exercicio,
        (case when p.modcodigo in (7, 8) then 'CONTRATACAO_DIRETA' else 'LICITACAO' end) as forma_contratacao,
        'CLASSIFICA' as desc_prop_invalida,
        'CLASSIFICA' as desc_prop_invalida_lote,
        p.pcsfundamentolegal,
        (case when p.pcsfundamentolegal is null then 0
              when p.modcodigo in (7) then 48 --Dispensa de Licitação
              when p.modcodigo in (8) then 81 -- Inegibilidade
              when p.pcsfundamentolegal ~ '13.979' then 159 -- RP
              when (p.pcsfundamentolegal ~ '10.520' or p.modcodigo = 6) then 160 -- Pregão
              else 0 end
        ) as id_fundamento_legal,
        (select (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) from wun.tbunico u where u.unicodigo = (select i.unicodigo from wco.tbintegrante i where i.cmlcodigo = m.cmlcodigo and i.mbcatribuicao in(3,6) limit 1)) as cpf_responsavel,
        coalesce((select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('305', 'responsavel', (select (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) from wun.tbunico u where u.unicodigo = (select i.unicodigo from wco.tbintegrante i where i.cmlcodigo = m.cmlcodigo and i.mbcatribuicao in(3,6) limit 1))))), 0) as id_responsavel,
        m.cmlcodigo as cod_comissao,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'comissao', m.cmlcodigo))) as id_comissao,
        (case
            when p.modcodigo = 5 then (select i_chave_dsk2 from public.controle_migracao_registro where sistema = '305' and tipo_registro = 'comissao-membros' and i_chave_dsk1 = (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'comissao', m.cmlcodigo)))::varchar and i_chave_dsk3 in ('LEILOEIRO') limit 1)
            else (select i_chave_dsk2 from public.controle_migracao_registro where sistema = '305' and tipo_registro = 'comissao-membros' and i_chave_dsk1 = (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'comissao', m.cmlcodigo)))::varchar and i_chave_dsk3 in ('PRESIDENTE', 'PREGOEIRO') limit 1)
        end) as cpf_membro_comissao,
        (case
            when p.modcodigo = 5 then coalesce((select id_gerado from public.controle_migracao_registro where sistema = '305' and tipo_registro = 'comissao-membros' and i_chave_dsk1 = (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'comissao', m.cmlcodigo)))::varchar and i_chave_dsk3 in ('LEILOEIRO') limit 1), 0)
            else coalesce((select id_gerado from public.controle_migracao_registro where sistema = '305' and tipo_registro = 'comissao-membros' and i_chave_dsk1 = (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'comissao', m.cmlcodigo)))::varchar and i_chave_dsk3 in ('PRESIDENTE', 'PREGOEIRO') limit 1), 0)
        end) as id_membro_comissao,
        p.modcodigo as cod_modalidade,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'modalidade-licitacao', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'entidade', p.clicodigo))), p.modcodigo, m.mintipopregao))) as id_modalidade,
        (case when e.edtdataentrprop is null then null else concat(e.edtdataentrprop, ' ', coalesce(e.edthoraentrprop, '00:00:00')) end) as dh_inicio_recebimento_envelopes,
        (case when e.edtdataentrprop is null then null else concat(e.edtdataentrprop, ' ', coalesce(e.edthoraentrprop, '00:00:00')) end) as dh_final_recebimento_envelopes,
        (case when e.edtdataentrprop is null then null else concat(e.edtdataentrprop, ' ', coalesce(e.edthoraentrprop, '00:00:00')) end) as dh_abertura_envelopes,
        (case when m.mintipoconcorrencia = 2 then true else false end) as registro_preco,
        (case when m.mintipoconcorrencia = 2 then true else false end) as possui_rp,
        null as data_autorizacao_rp,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', p.clicodigo, m.minano, m.minnro))) as id_processo,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-forma-contratacao', p.clicodigo, m.minano, m.minnro))) as id_gerado
    from wco.tbminuta m
    left join wco.tbprocesso p          on (p.clicodigo = m.clicodigo and p.pcsano = m.pcsano and p.pcsnro = m.pcsnro)
    left join wco.tblicitacao lic       on (lic.clicodigo = m.clicodigo and lic.minano = m.minano and lic.minnro = m.minnro)
    left join wco.tbedital e            on (e.clicodigo = m.clicodigo and e.minnro = m.minnro and e.minano = m.minano)
    where m.clicodigo = {{clicodigo}}
    and p.pcsano = {{ano}}
    and p.pcsnro = 258
    order by 1, 2 desc, 3 desc
) tab
where id_gerado is null
and id_processo is not null
and id_responsavel is not null
--limit 1