from dotenv import load_dotenv
import os
import glob
import findspark
findspark.init()
from pyspark.sql import SparkSession

from src.utils.db_utils import get_mysql_connection, query_string, load_dataframe_to_mysql 
from src.utils.utils import read_csv, gerar_e_salvar_create_table_mysql
load_dotenv() 

if __name__ == "__main__":
    print("Iniciando o script main.py...")
    
    spark_app_session = None 
    db_connection = None     
    
    try:
        # --- Configuração e Início da SparkSession ---
        print("\n--- Etapa 1/5: Inicializando SparkSession ---")
        spark_app_session = (SparkSession.builder
                 .appName("ChegadasTuristasPrincipalApp")
                 .config("spark.jars", "lib/mysql-connector-j-9.3.0.jar")
                 .config("spark.driver.extraClassPath", "lib/mysql-connector-j-9.3.0.jar")
                 .config("spark.executor.extraClassPath", "lib/mysql-connector-j-9.3.0.jar")
                 .getOrCreate())
        print("SparkSession inicializada com sucesso!")

        # --- Leitura e Processamento dos DataFrames ---
        print("\n--- Etapa 2/5: Lendo e processando DataFrames CSV ---")
        
        base_path = '/home/ander/Documentos/projetos/chegadas_turistas_internacionais_brasil/data/raw/'
        all_dfs = {}
        all_create_schemas = {}

        # Ajustando para iterar sobre os anos e carregar os dados
        for year in range(1989, 2026): # Inclui 2025
            file_path = os.path.join(base_path, f'chegadas_{year}.csv')
            df_name = f'df_{year}'
            create_schema_name = f'create_schema_table_{year}'
            table_mysql_name = f'tab_chegadas_turistas_{year}'
            
            print(f"Lendo CSV para o ano {year}: {file_path}")
            df = read_csv(spark_app_session, file_path)
            all_dfs[year] = df # Armazena o DataFrame em um dicionário
            
            print(f"Gerando schema SQL para {year}...")
            create_sql = gerar_e_salvar_create_table_mysql(df) 
            all_create_schemas[year] = create_sql # Armazena a string SQL
        
        print("Todos os CSVs lidos e schemas SQL gerados com sucesso!")

        #--- Conexão com o Banco de Dados MySQL (para DDL - CREATE TABLE) ---
        print("\n--- Etapa 3/5: Conectando ao Banco de Dados MySQL (para DDL) ---")
        db_connection = get_mysql_connection() 

        if db_connection and db_connection.is_connected():
            print("Conexão bem-sucedida ao banco de dados MySQL.")

            # --- Criação das Tabelas no MySQL ---
            print("\n--- Etapa 4/5: Criando tabelas no MySQL ---")
            for year, create_sql_query in all_create_schemas.items():
                print(f"Executando CREATE TABLE para o ano {year}...")
                success_create = query_string(db_connection, create_sql_query)
                if success_create:
                    print(f"Tabela 'tab_chegadas_turistas_{year}' criada com sucesso.")
                else:
                    print(f"ATENÇÃO: Falha ao criar tabela para o ano {year}.")
            print("Processo de criação de tabelas concluído.")

            # --- Carregar DataFrames para MySQL ---
            print("\n--- Etapa 5/5: Carregando DataFrames para o MySQL ---")
            for year, df_to_load in all_dfs.items(): # type: ignore
                table_mysql_name = f'tab_chegadas_turistas_{year}'
                print(f"Carregando dados para a tabela '{table_mysql_name}' (Ano: {year})...")
                # A função load_dataframe_to_mysql espera o DataFrame PySpark, não a conexão DB.
                # A conexão JDBC é configurada internamente pela função load_dataframe_to_mysql.
                success_load = load_dataframe_to_mysql(df_to_load, table_mysql_name) 
                if success_load:
                    print(f"Dados para 'tab_chegadas_turistas_{year}' carregados com sucesso.")
                else:
                    print(f"ATENÇÃO: Falha ao carregar dados para a tabela 'tab_chegadas_turistas_{year}'.")
            print("Todos os DataFrames processados e carregados para o MySQL.")

        else:
            print("Erro: Não foi possível conectar ao banco de dados MySQL. Verifique as credenciais e o status do servidor.")

    except Exception as e:
        print(f"\nOcorreu um erro crítico durante a execução: {e}")
        # Você pode adicionar um sys.exit(1) aqui se quiser que o script termine com erro
        import sys
        sys.exit(1)
    finally:
        # --- Fechamento da Conexão com o Banco de Dados MySQL ---
        if db_connection:
            if db_connection.is_connected():
                db_connection.close()
                print("\nConexão com o banco de dados MySQL fechada com sucesso.")
            else:
                print("\nConexão com o banco de dados MySQL já estava fechada ou não estabelecida.")

        # --- Parada da SparkSession ---
        if spark_app_session:
            spark_app_session.stop() # type: ignore
            print("SparkSession encerrada com sucesso.")
        else:
            print("SparkSession não foi inicializada, nada para encerrar.")
        
        print("\nScript main.py finalizado.")