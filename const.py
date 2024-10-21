import os 

# URL da página da B3 para coleta de dados
B3_URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br" 

# Nome do arquivo CSV que será gerado
CSV_FILE_NAME = "b3_data.csv" 

# Caminho completo para salvar o arquivo CSV
CSV_OUTPUT_DIR = "files"
CSV_PATH = os.path.join(CSV_OUTPUT_DIR, CSV_FILE_NAME) 

# Nomes dos buckets S3
S3_BUCKET_RAW = "b3-raw"
S3_BUCKET_CLEANSED = "b3-cleansed"