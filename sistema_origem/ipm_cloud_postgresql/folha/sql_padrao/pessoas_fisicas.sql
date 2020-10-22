select * from
(
	select
	    u.unicodigo as id,
		u.unicodigo as codigo,
		u.uninomerazao as nome,
		regexp_replace(u.unicpfcnpj, '[\.|\-|\/]', '', 'gi') as cpf,
		upper((select s.dsenome from wun.tbdominiosexo as s where s.dsecodigo = uf.unfsexo)) as sexo,
		uf.unfdatanascimento as data_nascimentom,
		public.bth_get_id_gerado('1', 'folha', 'teste') as funcao
	from
		wun.tbunico as u join wun.tbunicofisica as uf on uf.unicodigo = u.unicodigo
	where
		u.unitipopessoa = 1
	and
		u.unisituacao = 1
	and
		length(replace(replace(replace(replace(u.unicpfcnpj,'/',''),'-',''),'.',''),'0','')) > 0
	and
		uf.unfsexo is not null
	and
		uf.unfdatanascimento  is not null
) as tab
limit 10 offset 11000