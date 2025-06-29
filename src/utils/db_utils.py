# src/main.py (ou src/ingestion/ingest_data.py)
import os
import mysql.connector
from mysql.connector import Error
from pyspark.sql import DataFrame
# src/utils/db_utils.py


# Exemplo de uso:
# hoje = dia_de_hoje()
# print(hoje) # Irá imprimir algo como "20250629" se for executado hoje

def get_connection_details():
    """
    Constrói e retorna a URL JDBC do MySQL e um dicionário de propriedades de conexão
    (contendo user e password) a partir das variáveis de ambiente.

    Retorna:
        tuple: Uma tupla contendo (jdbc_url, connection_properties_dict) se todas as variáveis
               de ambiente necessárias forem encontradas.
               Retorna (None, None) e imprime um erro caso contrário.
    """
    mysql_host = os.getenv("MYSQL_HOST")
    mysql_port = os.getenv("MYSQL_PORT")
    mysql_database = os.getenv("MYSQL_DATABASE")
    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")

    # Verificação básica para garantir que as variáveis de ambiente estão presentes
    if not all([mysql_host, mysql_port, mysql_database, mysql_user, mysql_password]):
        print("Erro: Uma ou mais variáveis de ambiente do MySQL (MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD) não foram definidas.")
        return None, None

    jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"
    
    connection_properties = {
        "user": mysql_user,
        "password": mysql_password
    }
    
    print(f"Detalhes de conexão JDBC para '{mysql_database}' carregados.")
    return jdbc_url, connection_properties

def get_mysql_connection(): # Nova função para obter a conexão real
    """
    Cria e retorna uma conexão com o banco de dados MySQL usando as variáveis de ambiente.

    Retorna:
        mysql.connector.connection.MySQLConnection: A conexão MySQL se bem-sucedida.
        None: Se houver um erro ao conectar ou variáveis de ambiente ausentes.
    """
    # Primeiro, verifique se os detalhes da conexão existem usando get_connection_details
    # Esta chamada também imprime erros se as variáveis não estiverem definidas.
    jdbc_url, connection_properties = get_connection_details()
    
    if not jdbc_url or not connection_properties:
        # get_connection_details já imprimiu a mensagem de erro
        return None

    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            database=os.getenv("MYSQL_DATABASE"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD")
        )
        print(f"Conexão com o banco de dados '{os.getenv('MYSQL_DATABASE')}' estabelecida com sucesso.")
        return connection
    except Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

def query_string(connection, query: str):
    """
    Executa um comando SQL genérico no MySQL usando uma conexão existente.

    Args:
        connection (mysql.connector.connection.MySQLConnection): A conexão MySQL já aberta.
        query (str): O comando SQL a ser executado.
    Returns:
        tuple: (bool, list) - True e resultados (para SELECT) ou True e lista vazia (para outros comandos) se bem-sucedido.
               (False, []) se houver um erro.
    """
    if not connection or not connection.is_connected():
        print("Erro: Conexão MySQL não está aberta ou é inválida.")
        return False, []

    cursor = None
    results = []

    print(f"--- Executando comando SQL ---")
    try:
        cursor = connection.cursor()
        print("Em execução...")
        cursor.execute(query)
        
        if query.strip().upper().startswith("SELECT"):
            results = cursor.fetchall()
            print(f"Comando SELECT executado. {len(results)} linhas retornadas.")
        else:
            connection.commit() # Confirma as alterações para DDL/DML
            print("Comando executado e commit realizado.")
            
        return True, results
    except Error as e:
        print(f"Erro ao executar o comando: {e}")
        return False, []
    finally:
        if cursor:
            cursor.close()
        # A conexão NÃO é fechada aqui, pois ela foi passada.
        # A responsabilidade de fechar a conexão é do chamador.
        print("Execução do comando finalizada. Cursor fechado.")

def load_dataframe_to_mysql(spark_df: DataFrame, table_name: str) -> bool:
    """
    Carrega um PySpark DataFrame para uma tabela MySQL usando o conector JDBC.

    Args:
        spark_df (DataFrame): O DataFrame PySpark a ser carregado.
        table_name (str): O nome da tabela no MySQL onde os dados serão inseridos.

    Returns:
        bool: True se o carregamento for bem-sucedido, False caso contrário.
    """
    jdbc_url, connection_properties = get_connection_details()

    if not jdbc_url or not connection_properties:
        print("Erro: Detalhes de conexão JDBC não disponíveis. Não foi possível carregar dados.")
        return False

    print(f"\n--- Iniciando o carregamento de dados do DataFrame para a tabela '{table_name}' no MySQL ---")
    try:
        spark_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", connection_properties["user"]) \
            .option("password", connection_properties["password"]) \
            .mode("overwrite") \
            .save()
        print(f"Dados do DataFrame carregados com sucesso na tabela '{table_name}' no MySQL.")
        return True
    except Exception as e:
        print(f"Erro ao carregar dados na tabela '{table_name}' no MySQL: {e}")
        print("Verifique se o schema do DataFrame corresponde ao da tabela MySQL.")
        print("Verifique também as permissões do usuário do banco de dados.")
        return False

