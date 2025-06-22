import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType
)

def read_csv(spark_session: SparkSession, caminho_do_arquivo_csv: str): # <<< Agora a função recebe o parâmetro spark_session
    """
    Lê um arquivo CSV específico usando PySpark e retorna o DataFrame.

    Args:
        spark_session (SparkSession): A SparkSession já inicializada a ser utilizada.
        caminho_do_arquivo_csv (str): O caminho completo para o arquivo CSV a ser lido.

    Returns:
        pyspark.sql.DataFrame: O DataFrame contendo os dados do CSV, ou None em caso de erro.
    """
    # Parâmetros fixos para a leitura do CSV
    DELIMITER = ";"
    CHARSET = "windows-1252"

    print(f"--- Iniciando leitura do CSV: {os.path.basename(caminho_do_arquivo_csv)} ---")

    try:
        # AGORA, use o parâmetro 'spark_session' que foi passado para a função
        df = spark_session.read.format("csv") \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .option("delimiter", DELIMITER) \
          .option("charset", CHARSET) \
          .load(caminho_do_arquivo_csv)

        print(f"CSV '{os.path.basename(caminho_do_arquivo_csv)}' lido com sucesso.")
        return df

    except Exception as e: # Removi o NameError específico pois ele não deveria mais ocorrer aqui
        print(f"Erro ao ler o CSV '{caminho_do_arquivo_csv}': {e}")
        return None
    
import os
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType, DecimalType
)

# Caminho fixo para salvar os schemas
DEFAULT_SCHEMA_OUTPUT_DIR = "/home/ander/Documentos/projetos/chegadas_turistas_internacionais_brasil/data/schemas"


def gerar_e_salvar_create_table_mysql(df: DataFrame):
    """
    Gera um script SQL 'CREATE TABLE' para MySQL a partir do schema de um DataFrame PySpark
    e o salva em um arquivo.

    O caminho de saída é fixo para DEFAULT_SCHEMA_OUTPUT_DIR.
    O nome do arquivo e o nome da tabela são baseados no ano extraído da coluna 'Ano'
    (ou 'year_col_name' se especificado) do próprio DataFrame.

    Args:
        df (DataFrame): O DataFrame PySpark cujo esquema será usado para gerar o DDL.
                        Deve conter uma coluna com o ano (ex: 'Ano', 'YEAR').
    
    Returns:
        str: A string SQL do comando CREATE TABLE gerado.
    """
    mysql_types_map = {
        StringType: "VARCHAR(255)",
        IntegerType: "INT",
        LongType: "BIGINT",
        FloatType: "FLOAT",
        DoubleType: "DOUBLE",
        BooleanType: "BOOLEAN",
        DateType: "DATE",
        TimestampType: "DATETIME",
    }

    # --- Lógica para extrair o ano do DataFrame ---
    # Tentativa de inferir o nome da coluna do ano
    year_col_names_to_try = ["Ano", "ano", "Year", "year", "DATA_ANO"]
    year_column_found = None
    df_identifier = "geral" # Default

    for col_name in year_col_names_to_try:
        if col_name in df.columns:
            year_column_found = col_name
            break

    if year_column_found:
        try:
            # Pega o primeiro valor da coluna de ano.
            first_row_year = df.select(year_column_found).first()[year_column_found]
            df_identifier = str(first_row_year)
            if not df_identifier.isdigit() or len(df_identifier) != 4:
                print(f"Aviso: Valor '{df_identifier}' da coluna '{year_column_found}' não parece um ano válido de 4 dígitos. Usando 'geral'.")
                df_identifier = "geral"
        except Exception as e:
            print(f"Aviso: Erro ao extrair o ano da coluna '{year_column_found}'. Usando 'geral'. Erro: {e}")
            df_identifier = "geral" # Garante que df_identifier é definido mesmo em caso de erro
    else:
        print(f"Aviso: Nenhuma das colunas de ano esperadas ({', '.join(year_col_names_to_try)}) foi encontrada no DataFrame. Usando 'geral'.")
    
    
    table_name = f"tab_chegadas_turistas_{df_identifier}" # Nome da tabela no SQL
    sql_file_name = f"schema_chegadas_turistas_{df_identifier}.sql" # Nome do arquivo SQL

    print(f"--- Gerando script CREATE TABLE para '{table_name}' ---")

    columns = []
    for field in df.schema.fields:
        col_name = field.name
        col_type = field.dataType

        sql_type = None

        if isinstance(col_type, StringType):
            sql_type = "VARCHAR(255)"
        elif isinstance(col_type, IntegerType):
            sql_type = "INT"
        elif isinstance(col_type, LongType):
            sql_type = "BIGINT"
        elif isinstance(col_type, (FloatType, DoubleType)):
            sql_type = "DOUBLE"
        elif isinstance(col_type, BooleanType):
            sql_type = "BOOLEAN"
        elif isinstance(col_type, DateType):
            sql_type = "DATE"
        elif isinstance(col_type, TimestampType):
            sql_type = "DATETIME"
        elif isinstance(col_type, DecimalType):
            sql_type = f"DECIMAL({col_type.precision},{col_type.scale})"
        else:
            print(f"Aviso: Tipo PySpark desconhecido '{col_type}' para a coluna '{col_name}', usando VARCHAR(255).")
            sql_type = "VARCHAR(255)"

        nullability = "NULL" if field.nullable else "NOT NULL"
        columns.append(f"`{col_name}` {sql_type} {nullability}")

    columns_sql = ",\n    ".join(columns)
    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n    {columns_sql}\n);"

    output_dir = DEFAULT_SCHEMA_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)

    sql_file_path = os.path.join(output_dir, sql_file_name)

    try:
        with open(sql_file_path, "w") as sql_file:
            sql_file.write(create_table_sql)
        print(f"Script CREATE TABLE salvo com sucesso em: {sql_file_path}")
    except Exception as e:
        print(f"Erro ao salvar o script SQL para '{table_name}': {e}")
    
    # <<< AQUI ESTÁ A MUDANÇA: RETORNAR A STRING SQL >>>
    return create_table_sql