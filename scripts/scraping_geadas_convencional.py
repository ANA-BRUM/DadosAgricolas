import httpx
import pandas as pd
import logging
import calendar
import unidecode
import locale
from datetime import datetime, timedelta
import os

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

def fazer_requisicao(url: str, timeout: int = 10) -> list:
    """
    Faz a requisição para a API e retorna os dados.
    
    Parâmetros:
        url (str): URL da requisição.
        timeout (int): Tempo máximo de espera para a resposta.

    Retorna:
        list: Dados da resposta em formato JSON ou uma lista vazia.
    """
    try:
        response = httpx.get(url, timeout=timeout)
        response.raise_for_status()  # Levanta exceção para códigos HTTP >= 400
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Erro HTTP {e.response.status_code} ao fazer requisição para {url}")
        return []
    except httpx.RequestError as e:
        logger.error(f"Erro na requisição: {e}")
        return []

def formatar_data_brasileira(data_iso: str) -> str:
    """
    Converte a data do formato ISO para o formato brasileiro.

    Parâmetros:
        data_iso (str): Data em formato ISO (YYYY-MM-DD).

    Retorna:
        str: Data no formato DD/MM/YYYY.
    """
    try:
        data_formatada = datetime.strptime(data_iso, '%Y-%m-%d').strftime('%d/%m/%Y')
        return data_formatada
    except ValueError:
        logger.error(f"Erro ao formatar a data: {data_iso}")
        return "Data inválida"

def formatar_temperatura(temperatura: str) -> str:
    """
    Formata a temperatura, garantindo que seja exibida com uma casa decimal.

    Parâmetros:
        temperatura (str): Valor da temperatura.

    Retorna:
        str: Temperatura formatada com uma casa decimal.
    """
    try:
        return f"{float(temperatura):.1f}°C"
    except (TypeError, ValueError):
        return "N/A"

def calcular_intensidade(temp_min: str) -> str:
    """
    Calcula a intensidade da geada com base na temperatura mínima.

    Parâmetros:
        temp_min (str): Temperatura mínima.

    Retorna:
        str: Intensidade da geada.
    """
    try:
        temp_min_float = float(temp_min)
        if temp_min_float < 1:
            return "Forte"
        elif temp_min_float <= 3:
            return "Moderada"
        else:
            return "Fraca"
    except (TypeError, ValueError):
        return "N/A"

def buscar_id_por_nome(cidades: list, nome_cidade: str) -> int:
    """
    Busca o ID do município pelo nome.

    Parâmetros:
        cidades (list): Lista de dicionários com os dados dos municípios.
        nome_cidade (str): Nome do município a ser buscado.

    Retorna:
        int: ID do município ou -1 se não for encontrado.
    """
    nome_cidade_normalizado = unidecode.unidecode(nome_cidade).lower()
    for cidade in cidades:
        nome_cidade_api_normalizado = unidecode.unidecode(cidade["nome"]).lower()
        if nome_cidade_api_normalizado == nome_cidade_normalizado:
            return cidade["id"]
    return -1

def extrair_dados_geada(folder_path: str = r"C:\Users\ana.brum\Área de Trabalho\DadosMeteorologicos"):
    """
    Extrai os dados de geadas da API e salva em um arquivo Excel.
    
    Parâmetros:
        folder_path (str): Caminho da pasta onde o arquivo Excel será salvo.
    """
    cidades = fazer_requisicao("https://servicodados.ibge.gov.br/api/v1/localidades/municipios")
    if not cidades:
        logger.error("Não foi possível obter a lista de municípios.")
        return

    data_inicio = datetime(2017, 1, 1)
    data_fim = datetime(2024, 9, 30)
    dados_tratados = []

    while data_inicio <= data_fim:
        ano = data_inicio.year
        mes = data_inicio.month
        nome_mes = data_inicio.strftime('%B').capitalize()

        logger.info(f"Extraindo dados para: {nome_mes} de {ano}")

        primeiro_dia = data_inicio.strftime("%Y-%m-%d")
        ultimo_dia = (data_inicio + timedelta(days=calendar.monthrange(ano, mes)[1] - 1)).strftime("%Y-%m-%d")

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
        data_inicio += timedelta(days=calendar.monthrange(ano, mes)[1])

    # Criar DataFrame com os dados tratados
    colunas = ["Cod. IBGE", "Uf", "Município", "Dia de ocorrência", "Temperatura Mínima", "Intensidade"]
    df = pd.DataFrame(dados_tratados, columns=colunas)

    # Certificando-se de que a pasta existe
    os.makedirs(folder_path, exist_ok=True)

    # Caminho completo do arquivo Excel
    excel_file = os.path.join(folder_path, "dados_geada.xlsx")
    df.to_excel(excel_file, index=False)
    logger.info(f"Dados extraídos e salvos com sucesso no arquivo '{excel_file}'.")

if __name__ == "__main__":
    extrair_dados_geada()
