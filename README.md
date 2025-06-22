CHEGADAS_TURISTAS_BRASIL/  # Nome completo e descritivo do projeto
├── data/
│   ├── raw/                 # Dados brutos, intocados (ex: seus CSVs originais)
│   │   └── chegadas_1989.csv
│   ├── processed/           # Dados transformados, prontos para consumo (ex: parquet, otimizados)
│   ├── schemas/             # Schemas inferidos (JSONs, YMLs)
│   │   └── chegadas_turistas_schema.json # Se você salvar os schemas em JSON
│   └── docs/                # Qualquer documentação específica sobre os dados
├── sql/                     # Novo diretório para scripts SQL (Create Table, Insert, Queries, DDL/DML)
│   ├── ddl/                 # Data Definition Language (CREATE TABLE, ALTER TABLE)
│   │   └── chegadas_turistas_1989_create_table.sql # Seu script gerado viria para cá
│   ├── dml/                 # Data Manipulation Language (INSERT, UPDATE, DELETE)
│   └── queries/             # Consultas comuns para análise
├── src/
│   ├── ingestion/           # Módulos para ingestão de dados (ler_csv_pyspark)
│   │   └── csv_reader.py
│   ├── transformations/     # Módulos para transformações PySpark
│   │   └── data_transformations.py
│   ├── schema_management/   # Módulos para gerenciamento de schemas e DDL (gerar_e_salvar_create_table_mysql)
│   │   └── mysql_schema_generator.py
│   ├── main.py              # Orquestrador principal do pipeline
│   ├── utils/               # Funções utilitárias genéricas (logger, helpers)
│   │   └── common_utils.py
│   └── notebooks/           # Mantenha os notebooks aqui, ou mova data.ipynb para cá
│       └── data_exploration.ipynb
├── tests/                   # Novo diretório para testes unitários/de integração (fundamental para qualidade)
│   └── test_ingestion.py
├── config/                  # Novo diretório para arquivos de configuração (ex: YAML, TOML)
│   └── settings.yaml        # Configurações de DB, paths, etc. (se não estiver no .env)
├── .env                     # Variáveis de ambiente (credenciais de DB, AWS keys)
├── .gitignore
├── README.md
└── requirements.txt