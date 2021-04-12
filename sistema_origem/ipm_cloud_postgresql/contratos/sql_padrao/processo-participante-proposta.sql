select
    row_number() over() as id,
    '305' as sistema,
    'processo-participante-proposta' as tipo_registro,
    *
from (
    select
        qcp.clicodigo,
        qcp.minano as ano_processo,
        qcp.minnro as nro_processo,
        qcp.cmiid,
        qcp.unicodigo,
        (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')) as cpf_participante,
        qcp.qcpvlrunit as valor_unitario,
        i.cmiqtde as quantidade,
        left(mc.mardescricao, 20) as marca,
        qcp.qcpvencedor,
        p.modcodigo,
        (case
            when (select 1 from wco.vw_qcp_vencedor v where v.clicodigo = qcp.clicodigo and v.minano = qcp.minano and v.minnro = qcp.minnro and v.cmiid = qcp.cmiid and v.unicodigo = qcp.unicodigo) is not null then 'VENCEU'
            else 'PERDEU'
        end) as situacao,
        (case
            when (select 1 from wco.vw_qcp_vencedor v where v.clicodigo = qcp.clicodigo and v.minano = qcp.minano and v.minnro = qcp.minnro and v.cmiid = qcp.cmiid and v.unicodigo = qcp.unicodigo) is not null then 1
            else (case
                    when qcp.qcpposicao <> 0 and qcp.qcpposicao <> 1 then qcp.qcpposicao
                    else 2
                  end)
        end) as colocacao,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo', qcp.clicodigo, qcp.minano, qcp.minnro))) as id_processo,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante', qcp.clicodigo, qcp.minano, qcp.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g'))))) as id_participante,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', qcp.clicodigo, qcp.minano, qcp.minnro, '@', coalesce(i.lotcodigo, 0) , '@', i.cmiitem))) as id_item,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-participante-proposta', qcp.clicodigo, qcp.minano, qcp.minnro, (regexp_replace(u.unicpfcnpj,'[/.-]|[ ]','','g')), qcp.cmiid))) as id_gerado
    from wco.tbcadqcp qcp
    left join wco.tbitemin i on (i.clicodigo = qcp.clicodigo and i.minano = qcp.minano and i.minnro = qcp.minnro and i.cmiid = qcp.cmiid)
    left join wun.tbmarca mc on (mc.marcodigo = qcp.marcodigo)
    left join wco.tbprocesso p on (p.clicodigo = qcp.clicodigo and p.pcsano = qcp.minano and p.pcsnro = qcp.minnro)
    inner join wun.tbunico u on (u.unicodigo = qcp.unicodigo)
    where qcp.clicodigo = {{clicodigo}}
    and qcp.minano = {{ano}}
    and qcp.minnro = 31
    --and qcp.minnro not in (35, 41, 65, 81, 88, 90, 92, 99, 100)
    order by 1, 2 desc, 3 desc, 4 asc
) tab
where id_gerado is null
and id_processo is not null
and id_participante is not null
and id_item is not null