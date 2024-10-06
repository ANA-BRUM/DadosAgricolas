import httpx
from bs4 import BeautifulSoup
import os
import zipfile
import logging

# Configurando o logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)

# Diminuir o nível de log para o httpx e outros loggers de terceiros
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

def baixar_e_extrair_arquivos(pasta_destino):
    """Baixa e extrai os arquivos.zip dos Dados Históricos contidos na página do INMET."""

    logging.info("Iniciando o processo de scraping...")
    base_url = "https://portal.inmet.gov.br/dadoshistoricos"

    try:
        response = httpx.get(base_url)
        response.raise_for_status()
        logging.debug("Página acessada com sucesso!")

        soup = BeautifulSoup(response.text, 'html.parser')
        artigos = soup.find_all("article", class_="post-preview")
        links_zip = []
        
        for artigo in artigos:
            link = artigo.find("a", href=True)

            # Verifica se é um arquivo .zip
            if link and link['href'].endswith('.zip'):
                nome_arquivo = link.text.strip()
                href = link['href']
                filename_sem_zip = href.split('/')[-1].split('.zip')[0]

                # Completa o link se for relativo
                if not href.startswith("http"):
                    href = base_url + href
                links_zip.append((nome_arquivo, href, filename_sem_zip))
        
        logging.info(f"Encontrados {len(links_zip)} arquivos para download.")

        # Criando diretório para salvar os arquivos
        if not os.path.exists(pasta_destino):
            os.makedirs(pasta_destino)
            logging.info(f"Criando diretório: {pasta_destino}")
        
        for nome_arquivo, url_zip, filename_sem_zip in links_zip:
            caminho_zip = os.path.join(pasta_destino, filename_sem_zip + ".zip")

            # Baixando o arquivo .zip
            logging.info(f"Baixando o arquivo: {nome_arquivo} de {url_zip}")
            with httpx.stream("GET", url_zip) as r:
                r.raise_for_status()
                with open(caminho_zip, 'wb') as f:
                    for chunk in r.iter_bytes():
                        f.write(chunk)
            
            logging.info(f"Arquivo {nome_arquivo} baixado com sucesso.")

            # Extraindo o arquivo .zip
            logging.info(f"Extraindo o arquivo: {nome_arquivo}")
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:

                # Verifica se tem alguma pasta
                lista = zip_ref.namelist()
                if len([f for f in lista if f.endswith('/')]) >= 1:
                    zip_ref.extractall(pasta_destino)
                else:
                    path = os.path.join(pasta_destino, filename_sem_zip)
                    if not os.path.exists(path):
                        os.makedirs(path)
                    zip_ref.extractall(path)

            logging.info(f"Arquivo {nome_arquivo} extraído com sucesso.")

            # Removendo o arquivo .zip após a extração
            os.remove(caminho_zip)
            logging.debug(f"Arquivo .zip removido após extração: {nome_arquivo}")

    except Exception as e:
        logging.error(f"Ocorreu um erro: {e}")

if __name__ == "__main__":

    pasta_destino = "Dados Historicos"
    baixar_e_extrair_arquivos(pasta_destino)
