select
				logcodigo as id,
                 logcodigo as codigo,
                 lognome as descricao,
                 null as cep,
                 public.bth_get_id_gerado('300', 'tipo-logradouro', cast(tplcodigo as varchar)) as tipoLogradouro,
                 public.bth_get_id_gerado('300', 'municipio', cast(cidcodigo as varchar)) as municipio
            from wun.tblogradouro;