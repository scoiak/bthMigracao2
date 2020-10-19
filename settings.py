BASE_ORIGEM = 'ipm_cloud_postgresql'
SISTEMA_ORIGEM = 'contabil'
DB_HOST = 'localhost'
DB_PORT = '5433'
DB_NAME = 'ipm_bigua'
DB_USER = 'postgres'
DB_PW = 'admin'


def iniciar_migracao():
    start_logging()
    path = f'sistema_origem.{BASE_ORIGEM}.{SISTEMA_ORIGEM}.enviar'
    modulo = __import__(path, globals(), locals(), ['iniciar'], 0)
    modulo.iniciar()


def start_logging():
    import logging
    from datetime import datetime
    nome_arquivo = datetime.now().strftime("%d_%m_%y_%H_%M_%S")
    logging.basicConfig(filename=f'log/LOG_{nome_arquivo}.log',
                        format="%(levelname)s %(asctime)s  %(message)s",
                        level=logging.INFO)
    logging.info('Execução iniciada.')
