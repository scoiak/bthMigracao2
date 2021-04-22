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
    --and qcp.minano = {{ano}}
    and qcp.minano >= 2010
    --and qcp.minnro in (81)
    --and qcp.minnro not in (35, 41, 65, 81, 88, 90, 92, 99, 100)
    order by 1, 2 desc, 3 desc, 4 asc
) tab
where id_gerado is null
and id_processo is not null
and id_participante is not null
and id_item is not null
and situacao = 'VENCEU'
--and id_processo in (212638,212707,212749,213022,213025,212558,213019,212741,212734,212837,212869,212449,212305,212865,212931,212302,212913,212951,213004,212270,212570,212703,212288,212318,212345,212568,212613,212712,212866,212935,212987,213031,212955,212958,212250,212399,212422,212451,212499,212426,212519,212529,212304,212389,212395,212930,212667,212651,212710,212647,213285,213132,213190,213256,213399,213394,213393,213414,213421,213413,213401,213409,213411,213408,213428,213497,213763,213449,213758)
