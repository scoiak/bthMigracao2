select
    0 as id,
    hash_chave_dsk,
	cmr.i_chave_dsk1 as clicodigo,
	cmr.i_chave_dsk2 as ano_ata,
	cmr.i_chave_dsk3 as nro_ata,
	cmr.i_chave_dsk4 as cpf_participante,
	cmr.i_chave_dsk5 as cmiid,
	coalesce(m.lotcodigo, 0),
	m.cmiitem,
	(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat(305, 'processo-item', cmr.i_chave_dsk1, cmr.i_chave_dsk2, cmr.i_chave_dsk3, '@', coalesce(m.lotcodigo, 0), '@', m.cmiitem))) as id_item,
	(select i.prdcodigo from wco.tbitemin i where i.clicodigo = cmr.i_chave_dsk1::int and i.minano = cmr.i_chave_dsk2::int and i.minnro = cmr.i_chave_dsk3::int and i.cmiid = cmr.i_chave_dsk5::int) as prdcodigo,
	(select aux.id_gerado from public.controle_migracao_registro aux where aux.hash_chave_dsk = md5(concat(305, 'processo', cmr.i_chave_dsk1, cmr.i_chave_dsk2, cmr.i_chave_dsk3))) as id_processo,
	(select aux.id_gerado from public.controle_migracao_registro aux where aux.hash_chave_dsk = md5(concat(305, 'material', (select i.prdcodigo from wco.tbitemin i where i.clicodigo = cmr.i_chave_dsk1::int and i.minano = cmr.i_chave_dsk2::int and i.minnro = cmr.i_chave_dsk3::int and i.cmiid = cmr.i_chave_dsk5::int)))) as id_material
from public.controle_migracao_registro cmr
inner join wco.tbitemin m on (m.clicodigo = cmr.i_chave_dsk1::integer and m.minano = cmr.i_chave_dsk2::integer and m.minnro = cmr.i_chave_dsk3::integer and m.cmiid = cmr.i_chave_dsk5::integer)
where cmr.tipo_registro = 'processo-participante-proposta'
and cmr.id_gerado is null
and cmr.i_chave_dsk1 = '{{clicodigo}}'
--and cmr.i_chave_dsk2 = '{{ano}}'
--limit 1
