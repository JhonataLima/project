import os
from datetime import datetime
import boto3
import pandas as pd
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv(override=True)

# Obtém as credenciais da AWS a partir das variáveis de ambiente
key_id = 'ASIAYGMJGMKXYRQYIPQA' #os.getenv("AWS_ACCESS_KEY_ID")
secret_key = 'gFXBCH43uIgtNJVGw0AwkSf/z3N/oBuZRZ9L433F' #os.getenv("AWS_SECRET_ACCESS_KEY")
session_token ='IQoJb3JpZ2luX2VjEBkaCXVzLXdlc3QtMiJHMEUCIQDHDlapRtmHH6S8FJBXuJUmPIA/Ov8Nrp6iw3lutXD4WQIgDpkX+5Zm2Y+3c57G6LipWZB7uNNw9p5Tnr5DmjfadlgqwAIIgv//////////ARABGgw1NjM0NjUzMTQ5OTEiDDjB6Zz3pWQSlzrUCyqUAiD7Ulg1oV7ssfXUFvy9mtajr6f3JnAPDBzkxfIeoAW3le9NrH646/9NE9PhXUG3/MBeUL3TzMy7Un8Fq4SMQLHe101R9vjXt8tUZMmxVN3NaiitWe400obun4UoqN/0nk1WiJwttbPsVuOKsQVV2s0CvkWsuGpM0nxWAdc+VNxtSErRwGRAUCAKDttKaTOCibJStRa/4t86xzpoEwDI4LlC3IWJv3oxkQRsmElJ9IMUcnxlpiK12SWg9CHAboT9DRMnRNLMx9iHRLOHoO2VTSI+dQcpnCa28RaMYyKY3iLwM9KJovGjc6lviknkTK3NwnmN9kC9bWAPZ8tdtV2uAOcRZlOFHIWnMvhZ0S8cBARYjc4xyzDA0Na4BjqdAQAIbfNO5ocjJ8nyhKVclJNgx704KKMbc3PC9XCvdHO+6A1C0F4ymQHQUCoKN+UV5unujN5S1wig59Q+pwYhVVGLo4SotZKUy4bC0AlZuW0pZklTAWGlb99JDsfAqN8WP/SzDJGXNKPFgOviLBPZJJ5NGCfBsMyrThk+95YqGal+3CQ/76dKM7nQcvSJCOXXiu48rRV+zw6RnEX2Mb8='
 #os.getenv("AWS_SESSION_TOKEN")


def upload_file_to_s3(csv_path: str, bucket_name: str) -> None:
    """
    Faz o upload de um arquivo CSV convertido para Parquet em um bucket S3.

    Parâmetros:
        csv_path (str): Caminho do arquivo CSV com os dados da B3.
        bucket_name (str): Nome do bucket S3 de destino.
    """
    # Inicializa o cliente S3 com as credenciais da AWS
    s3 = boto3.client(
        service_name="s3",
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name="us-east-1",
    )

    # Converte o arquivo CSV para Parquet
    parquet_file_name = "b3_data.parquet"
    parquet_path = os.path.join("files", parquet_file_name)
    convert_to_parquet(csv_path=csv_path, parquet_path=parquet_path)

    # Cria a chave do bucket com partições baseadas na data atual
    now = datetime.now()
    formatted_month = f"{now.month:02}"
    formatted_day = f"{now.day:02}"
    raw_key = f"partitioned_data/{now.year}/{formatted_month}/{formatted_day}/{parquet_file_name}"

    # Faz o upload do arquivo Parquet para o S3
    try:
        s3.upload_file(parquet_path, bucket_name, raw_key)
        print(f"Arquivo '{parquet_path}' enviado com sucesso para o bucket '{bucket_name}'")
    except FileNotFoundError:
        print(f"Arquivo '{parquet_path}' não encontrado")
    except Exception as e:
        print(f"Ocorreu um erro ao fazer upload para o S3: {e}")


def convert_to_parquet(csv_path: str, parquet_path: str) -> None:
    """
    Converte um arquivo CSV para o formato Parquet.

    Parâmetros:
        csv_path (str): Caminho do arquivo CSV com os dados da B3.
        parquet_path (str): Caminho de destino do arquivo Parquet.
    """
    print("Convertendo arquivo para Parquet...")

    # Lê o arquivo CSV e converte para Parquet
    df = pd.read_csv(csv_path)
    df.to_parquet(parquet_path, index=False)

    print(f"Arquivo convertido com sucesso para Parquet: {parquet_path}")