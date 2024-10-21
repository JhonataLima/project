from scraping import upload, scrapper
from const import B3_URL, S3_BUCKET_RAW, S3_BUCKET_CLEANSED, CSV_PATH

def main():
    """Função principal para realizar o scraping de dados e upload para os buckets S3."""
    
    # Etapa 1: Realiza o scraping de dados no site da B3 e gera um arquivo .csv
    print("Iniciando o scraping de dados...")
    scrapper.get_values_b3(url=B3_URL, csv_path=CSV_PATH)
    print(f"Scraping de dados concluído. CSV salvo em: {CSV_PATH}")
    
    # Etapa 2: Faz o upload do arquivo CSV gerado para o bucket "raw"
    print(f"Fazendo upload do CSV para o bucket raw: {S3_BUCKET_RAW}...")
    upload.upload_file_to_s3(csv_path=CSV_PATH, bucket_name=S3_BUCKET_RAW)
    print(f"CSV enviado com sucesso para o bucket raw: {S3_BUCKET_RAW}")
    
    # Etapa 3: Faz o upload do mesmo arquivo CSV para o bucket "cleansed"
    print(f"Fazendo upload do CSV para o bucket cleansed: {S3_BUCKET_CLEANSED}...")
    upload.upload_file_to_s3(csv_path=CSV_PATH, bucket_name=S3_BUCKET_CLEANSED)
    print(f"CSV enviado com sucesso para o bucket cleansed: {S3_BUCKET_CLEANSED}")

if __name__ == "__main__":
    main()