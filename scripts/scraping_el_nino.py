import httpx
from bs4 import BeautifulSoup
import pandas as pd
import logging

# Configuração do logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)

logger = logging.getLogger(__name__)

# Diminuir o nível de log para o httpx e outros loggers de terceiros
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# URL do site
url = "https://origin.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/ONI_v5.php"

def get_html(url: str):
    """
    Retorna o conteúdo HTML da página.
    
    Parâmetros:
        url (str): URL do site.
    
    Retorna:
        str: Conteúdo HTML da página.
    """

    logger.debug(f"Fazendo requisição para {url}")
    response = httpx.get(url)
    response.raise_for_status()
    logger.debug("Requisição bem sucedida")
    return response.text

def extract_table_data(html: str):
    """
    Extrai os dados da tabela HTML e retorna uma lista de listas.
    
    Parâmetros:
        html (str): Conteúdo HTML da página.

    Retorna:
        Lista de listas: Cada sublista contém os dados de uma linha da tabela.
    """

    logger.info("Extraindo os dados da tabela HTML")
    soup = BeautifulSoup(html, 'html.parser')
    table_rows = soup.find_all('tr')

    data = []
    for row in table_rows:
        cols = row.find_all('td')
        cols = [col.text.strip() for col in cols if col.text.strip()]
        if cols and cols[0].isdigit():
            data.append(cols)
        elif len(cols) == 1 and cols[0] == "Year":
            logger.debug("Ignorando linha repetida de década")
    logger.info("Dados da tabela extraídos com sucesso")
    return data

def create_dataframe(data):
    """
    Cria um DataFrame a partir dos dados extraídos.
    
    Parâmetros:
        data (List[List[str]]): Lista de listas com os dados da tabela.

    Retorna:
        pd.DataFrame: DataFrame com os dados.
    """

    logger.debug("Criando DataFrame a partir dos dados extraídos")
    columns = ["Year", "DJF", "JFM", "FMA", "MAM", "AMJ", "MJJ", "JJA", "JAS", "ASO", "SON", "OND", "NDJ"]
    df = pd.DataFrame(data, columns=columns)
    logger.debug("DataFrame criado com sucesso")
    return df

def calcular_media_mes(df, mes_colunas):
    """Calcula a média dos valores dos meses passados por ano."""

    logger.debug(f"Calculando a média para os meses: {mes_colunas}")
    return df[mes_colunas].astype(float).mean(axis=1)

def classificar_fenomeno(media):
    """Classifica o fenômeno El Niño ou La Niña."""

    if media > 0.5:
        if media >= 1.5:
            return 'El Niño', 'Forte'
        elif media >= 1.0:
            return 'El Niño', 'Moderado'
        else:
            return 'El Niño', 'Fraco'
    elif media < -0.5:
        if media <= -1.5:
            return 'La Niña', 'Forte'
        elif media <= -1.0:
            return 'La Niña', 'Moderado'
        else:
            return 'La Niña', 'Fraco'
    else:
        return 'Neutro', 'Neutro'

def obter_trimestres_para_mes(mes):
    """
    Mapeia o mês para os três trimestres correspondentes.
    """
    mapeamento = {
        'Janeiro': ['NDJ', 'DJF', 'JFM'],
        'Fevereiro': ['DJF', 'JFM', 'FMA'],
        'Março': ['JFM', 'FMA', 'MAM'],
        'Abril': ['FMA', 'MAM', 'AMJ'],
        'Maio': ['MAM', 'AMJ', 'MJJ'],
        'Junho': ['AMJ', 'MJJ', 'JJA'],
        'Julho': ['MJJ', 'JJA', 'JAS'],
        'Agosto': ['JJA', 'JAS', 'ASO'],
        'Setembro': ['JAS', 'ASO', 'SON'],
        'Outubro': ['ASO', 'SON', 'OND'],
        'Novembro': ['SON', 'OND', 'NDJ'],
        'Dezembro': ['OND', 'NDJ', 'DJF'],
    }
    return mapeamento[mes]

def analisar_ano(df):
    """
    Analisa os fenômenos El Niño e La Niña para todos os meses de cada ano.
    """
    logger.info("Analisando os fenômenos El Niño e La Niña por mês e ano")
    resultados = []
    
    # Itera por cada ano
    for index, row in df.iterrows():
        ano = row['Year']
        
        for mes in ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 
                    'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']:
            
            trimestres = obter_trimestres_para_mes(mes)
            
            # Calcula a média das anomalias para o mês com base nos trimestres
            media_mes = calcular_media_mes(df.iloc[index:index+1], trimestres).iloc[0]
            fenomeno, intensidade = classificar_fenomeno(media_mes)

            logger.info(f"Ano {ano}, Mês {mes}: Fenômeno {fenomeno}, Intensidade {intensidade}")
            
            # Salvando o resultado por ano e mês
            resultados.append([ano, mes, fenomeno, intensidade])
    
    # Criando o DataFrame final com Ano, Mês, Fenômeno e Intensidade
    df_resultados = pd.DataFrame(resultados, columns=['Ano', 'Mês', 'Fenômeno', 'Intensidade'])
    logger.info("Análise concluída")
    return df_resultados

def save_to_excel(df, file_name):
    """Salva o DataFrame em um arquivo Excel."""

    logger.info(f"Salvando resultados no arquivo {file_name}")
    df.to_excel(file_name, index=False)
    logger.info("Arquivo Excel salvo com sucesso")

# Função principal que orquestra todo o processo
def main(excel_file="resultados_oni.xlsx"):
    """Script principal para extração e análise dos dados do ONI."""

    try:
        logger.info("Iniciando o processo de extração e análise")
        
        # Etapa 1: Requisição e extração dos dados
        html = get_html(url)
        table_data = extract_table_data(html)

        # Etapa 2: Criar DataFrame
        df = create_dataframe(table_data)

        # Etapa 3: Analisar e classificar os fenômenos
        df_resultados = analisar_ano(df)

        # Etapa 4: Salvar em Excel
        save_to_excel(df_resultados, excel_file)
        logger.info("Processo concluído com sucesso")

    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")


if __name__ == "__main__":
    excel_file = "resultados_oni.xlsx"
    main(excel_file)
