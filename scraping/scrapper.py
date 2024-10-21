import csv
import os
import time
from io import StringIO
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# Função para inicializar o WebDriver do Chrome
def initialize_driver():
    chrome_service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=chrome_service)

# Função principal para obter valores da B3 e gerar um arquivo CSV
def get_values_b3(url: str, csv_path: str) -> None:
    """
    Obtém valores da página da B3 e gera um arquivo CSV a partir de um DataFrame.

    Parâmetros:
    url (str): URL da página da B3.
    csv_path (str): Caminho do arquivo CSV onde os dados serão armazenados.
    """
    print("Iniciando a coleta de dados da B3...")

    # Remove o arquivo CSV existente, se houver
    if os.path.exists(csv_path):
        os.remove(csv_path)

    driver = initialize_driver()

    try:
        # Acessa a página da B3
        driver.get(url)

        # Aguarda até que o elemento de consulta por 'Setor de Atuação' esteja presente
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="segment"]'))
        )

        # Seleciona a opção de consulta por 'Setor de Atuação'
        consult_by = driver.find_element(By.XPATH, '//*[@id="segment"]')
        consult_by.click()
        consult_by_option_2 = driver.find_element(By.XPATH, '//*[@id="segment"]/option[2]')
        consult_by_option_2.click()

        # Aguarda até que a tabela esteja presente
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.table.table-responsive-sm.table-responsive-md"))
        )

        all_data = []
        page_count = 0

        # Loop para percorrer todas as páginas da tabela
        while True:
            time.sleep(0.5)
            page_count += 1

            # Coleta os dados da tabela
            table = driver.find_element(By.CSS_SELECTOR, "table.table.table-responsive-sm.table-responsive-md")
            table_html = table.get_attribute("outerHTML")
            df = pd.read_html(StringIO(table_html))[0]
            df = df.iloc[:-2]  # Remove as últimas duas linhas (provavelmente rodapés)
            all_data.append(df)

            # Verifica se há mais páginas
            if not navigate_to_next_page(driver):
                print(f"Coleta de dados concluída. Dados coletados de {page_count} página(s).")
                break

        # Se houver dados, concatena e salva no CSV
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df.to_csv(csv_path, index=False)
            time.sleep(2)
            delete_first_row(csv_path)

    except Exception as e:
        print(f"Ocorreu um erro durante a execução: {e}")
    finally:
        driver.quit()

# Função para verificar se o botão de próxima página existe e clicar nele
def navigate_to_next_page(driver) -> bool:
    """
    Verifica se o botão de próxima página existe e navega para a próxima página.

    Retorna:
    bool: True se houver próxima página, False caso contrário.
    """
    try:
        time.sleep(1)
        if check_exists_by_xpath(driver, '//*[@id="listing_pagination"]/pagination-template/ul/li[8]/a'):
            next_button = driver.find_element(By.XPATH, '//*[@id="listing_pagination"]/pagination-template/ul/li[8]/a')
            next_button.click()
            return True
        else:
            return False
    except Exception as e:
        print(f"Erro ao navegar para a próxima página: {e}")
        return False

# Função para verificar se um elemento existe pelo XPath
def check_exists_by_xpath(driver, xpath: str) -> bool:
    """
    Verifica se um elemento existe na página pelo XPath.

    Argumentos:
    xpath (str): XPath do elemento HTML a ser identificado.

    Retorna:
    bool: True se o elemento existir, False caso contrário.
    """
    try:
        driver.find_element(By.XPATH, xpath)
        return True
    except NoSuchElementException:
        return False

# Função para deletar a primeira linha de um arquivo CSV
def delete_first_row(csv_file: str) -> None:
    """
    Remove a primeira linha de um arquivo CSV.

    Argumentos:
    csv_file (str): Caminho do arquivo CSV.
    """
    with open(csv_file, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    if len(lines) > 1:
        with open(csv_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(csv.reader(lines[1:]))
        print(f"A primeira linha do arquivo '{csv_file}' foi removida.")
    else:
        print(f"O arquivo '{csv_file}' não possui dados para remover.")
