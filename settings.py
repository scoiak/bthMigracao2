BASE_ORIGEM = 'ipm_cloud_postgresql'
SISTEMA_ORIGEM = 'contabil'
DB_HOST = 'localhost'
DB_PORT = '5433'
DB_NAME = 'ipm_bigua'
DB_USER = 'postgres'
DB_PW = 'admin'


def iniciar_migracao():
    path = f'sistema_origem.{BASE_ORIGEM}.{SISTEMA_ORIGEM}.enviar'
    modulo = __import__(path, globals(), locals(), ['iniciar'], 0)
    modulo.iniciar()
