import httpx
import pandas as pd
import logging
import calendar
import unidecode
import locale
from datetime import datetime, timedelta

# Definir a localização para português
locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)

logger = logging.getLogger(__name__)

# Diminuir o nível de log para o httpx e outros loggers de terceiros
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

def extrair_dados_municipios():
    """Realiza a requisição à API do IBGE para buscar dados sobre os municípios."""

    url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    
    with httpx.Client() as client:
        resposta = client.get(url)
        if resposta.status_code == 200:
            return resposta.json()
        else:
            return None

def buscar_id_por_nome(cidades, nome_cidade):
    """Busca o ID do município pelo nome."""
    
    nome_cidade_normalizado = unidecode.unidecode(nome_cidade).lower()

    for cidade in cidades:
        nome_cidade_api_normalizado = unidecode.unidecode(cidade["nome"]).lower()
        if nome_cidade_api_normalizado == nome_cidade_normalizado:
            return cidade["id"]

    return None

def calcular_intensidade(temperatura):
    """Calcula a intensidade da temperatura em termos de fraca, moderada ou forte."""
    if temperatura is None:
        return "Indefinida"
    try:
        temperatura = float(temperatura)
        if temperatura >= 3:
            return "Fraca"
        elif 1 <= temperatura < 3:
            return "Moderada"
        else:
            return "Forte"
    except ValueError:
        return "Indefinida"

def formatar_temperatura(temperatura):
    """Formata a temperatura para o padrão brasileiro em Celsius."""
    if temperatura is None:
        return "N/A"
    return f"{str(temperatura).replace('.', ',')}°C"

def formatar_data_brasileira(data_iso):
    """Formata a data para o padrão brasileiro (dd/mm/yyyy)."""
    data = datetime.strptime(data_iso, "%Y-%m-%d")
    return data.strftime("%d/%m/%Y")

def fazer_requisicao(url):
    """Faz uma requisição síncrona à API GEADA."""
    try:
        logging.debug(f"Fazendo requisição para a URL: {url}")
        response = httpx.get(url)
        response.raise_for_status()
        return response.json()
    except httpx.RequestError as exc:
        logging.error(f"Erro ao conectar-se à API: {exc}")
    except httpx.HTTPStatusError as exc:
        logging.error(f"Erro HTTP ao acessar a API: {exc}")
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
    return []

def extrair_dados_geada(excel_file="dados_geada.xlsx", data_inicio="2017-01-01"):
    """
    Extrai dados da API GEADA.

    Parâmetros:
        data_inicio (str): Data de início para a extração dos dados (formato: YYYY-MM-DD). Defaults a "2017-01-01".

    Retorna:
        Lista com os dados tratados, cada item é um dicionário contendo os dados de uma unidade federativa.
    """

    cidades = extrair_dados_municipios()

    data_atual = datetime.now()
    data_inicio = datetime.strptime(data_inicio, "%Y-%m-%d")

    dados_tratados = []

    # Loop sobre cada mês desde a data de início até o mês atual
    while data_inicio <= data_atual:
        ano = data_inicio.year
        mes = data_inicio.month
        nome_mes = data_inicio.strftime("%B").capitalize()
        logging.info(f"Extraindo dados para: {nome_mes} de {ano}")

        primeiro_dia = datetime(ano, mes, 1).strftime("%Y-%m-%d")
        ultimo_dia = datetime(ano, mes, calendar.monthrange(ano, mes)[1]).strftime("%Y-%m-%d")

        url = f"https://apitempo.inmet.gov.br/geada/{primeiro_dia}/{ultimo_dia}/CONVENCIONAL"
        dados = fazer_requisicao(url)

        # Tratamento dos dados
        if dados:
            for item in dados:
                uf = item.get("UF", "N/A")
                nome_cidade = item.get("NOME", "N/A").title()
                data_ocorrencia = formatar_data_brasileira(item.get("DT_MEDICAO"))
                temp_min = item.get("TEMP_MIN")
                temperatura_formatada = formatar_temperatura(temp_min)
                intensidade = calcular_intensidade(temp_min)
                id_cidade = buscar_id_por_nome(cidades, nome_cidade)

                # Adicionando os dados tratados à lista
                dados_tratados.append([id_cidade, uf, nome_cidade, data_ocorrencia, temperatura_formatada, intensidade])

        # Avançar para o próximo mês
        data_inicio = data_inicio + timedelta(days=calendar.monthrange(ano, mes)[1])

    # Criar DataFrame com os dados tratados
    colunas = ["Cod. IBGE", "Uf", "Município", "Dia de ocorrência", "Temperatura Mínima", "Intensidade"]
    df = pd.DataFrame(dados_tratados, columns=colunas)
    df.to_excel(excel_file, index=False)
    logging.info(f"Dados extraídos e salvos com sucesso no arquivo '{excel_file}'.")

if __name__ == "__main__":
    extrair_dados_geada()
