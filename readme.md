# bthMigracao2 - Em desenvolvimento

Betha Sistemas / Sucesso do Cliente - Implantação (Delivery)

### Instruções de uso

Procedimento para utilização da ferramenta.

##### Configurando ambiente

```
1 - Realizar a instalação do Python versão 3.7 ou superior.    

2 - Baixar o projeto do repositório atual e abrir com a aplicação (VSCode,PyCharm,...).

3 - Realizar a instalação de todos os pacotes contidos no arquivo 'requirements.txt'
    Recomenda-se a utilização de ambiente virtual do python.

4 - Realizar a configuração do arquivo settings.py (Conexão com o Banco de Dados).

5 - Analisar o modulo enviar.py e executar a aplicação.
```

##### Configurações do arquivo 'settings.py'

```
Após configurar o ambiente, deve-se realizar a configuração do arquivo settings.py contém as configurações básicas 
para a execução da aplicação. O arquivo possui as seguitnes variáveis:

- BASE_ORIGEM: Indica qual o sistema de origem a ser considerado para executar o processo de extração de dados.
Opções disponíveis: [padrao]

SISTEMA_ORIGEM: Indica qual linha de produtos Betha Cloud se refere a migração.
Opções disponíveis [contabil|contratos|folha|educacao|livro|saude|tributos]

DB_HOST: Indica qual o endereço da rede do servidor que se encontra o banco de dados que serão extraidas as informações
para realizar a migração. Padrão: localhost

DB_PORT: Indica qual o número da porta do servidor que se encontra o banco de dados.
Padrão: 5432

DB_NAME: Indica qual o nome do database do servidor de dados. Padrão: 'migracao'

DB_USER: Indica qual o usuário que será utilizado para acessar o banco. Padrão: 'postgres'

DB_PW: Indica qual a senha do usuário que irá acessar o banco. Padrão: 'admin'
```

##### Organização dos módulos na árvores de projeto

```

    
```