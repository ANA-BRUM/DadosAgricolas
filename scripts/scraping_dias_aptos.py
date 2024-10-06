import httpx
import pandas as pd
import asyncio
import logging
import unidecode
from datetime import datetime, timedelta

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

locais_associacoes = {
    "ABROLHOS (A) - BA": "Caravelas",
    "AGUAS EMENDADAS (A) - GO": "Planaltina",
    "ALTO DA BOA VISTA (C) - RJ": "Rio de Janeiro",
    "ARCO VERDE (A) - PE": "Recife",
    "AREMBEPE (A) - BA": "Camaçari",
    "AVELAR (P.DO ALFERES) (C) - RJ": "Paty do Alferes",
    "BOM JESUS DO PIAUI (C) - PI": "Bom Jesus",
    "BOM JESUS DO PIAUI (A) - PI": "Bom Jesus",
    "CALCANHAR (A) - RN": "Touros",
    "CALDEIRAO (C) - PI": "Picos",
    "CAMARATUBA (A) - PB": "Mamanguape",
    "CAMPO NOVO DOS PARECIS (A) - MT": "Campo Novo do Parecis",
    "CAMPOS (C) - RJ": "Campos dos Goytacazes",
    "CAMPOS (A) - RJ": "Campos dos Goytacazes",
    "C. DO MATO DENTRO (C) - MG": "Conceição do Mato Dentro",
    "CEARA MIRIM (C) - RN": "Ceará-Mirim",
    "DELFINO (A) - BA": "Vitória da Conquista",
    "ECOLOGIA AGRÍCOLA (A) - RJ": "Seropédica",
    "FACULDADE DA TERRA DE BRASÍLIA (A) - DF": "Brasília",
    "FLORIANÓPOLIS-SÃO JOSE (A) - SC": "São José",
    "FORTE DE COPACABANA (A) - RJ": "Rio de Janeiro",
    "GLEBA CELESTE (C) - MT": "Sorriso",
    "IAUARETÊ (C) - AM": "São Gabriel da Cachoeira",
    "ILHA DE SANTANA (A) - MA": "São Luís",
    "ILHA DO MEL (A) - PR": "Paranaguá",
    "JACAREPAGUA (A) - RJ": "Rio de Janeiro",
    "LUIZ EDUARDO MAGALHAES (A) - BA": "Luís Eduardo Magalhães",
    "Mal. CANDIDO RONDON (A) - PR": "Marechal Cândido Rondon",
    "MARIA DE FÉ (A) - MG": "Maria da Fé",
    "MOCAMBINHO (C) - MG": "Pirapora",
    "MOCAMBINHO (A) - MG": "Pirapora",
    "MOELA (A) - SP": "Itapira",
    "MONTE VERDE (A) - MG": "Camanducaia",
    "MORRO DOS CAVALOS (C) - PI": "São João da Serra",
    "NHUMIRIM (A) - MS": "Corumbá",
    "NHUMIRIM (NHECOLANDIA) (C) - MS": "Corumbá",
    "NOVA XAV.(XAVANTINA) (C) - MT": "Nova Xavantina",
    "PADRE RICARDO REMETTER (C) - MT": "Campo Verde",
    "PALMEIRA DA MISSÕES (A) - RS": "Palmeira das Missões",
    "PARATÍ (A) - RJ": "Paraty",
    "PARQUE ESTADUAL CHANDLESS (A) - AC": "Manoel Urbano",
    "PICO DO COUTO (A) - RJ": "Petrópolis",
    "POXOREO (C) - MT": "Poxoréu",
    "PREGUIÇAS (A) - MA": "Barreirinhas",
    "PRES. KENNEDY (A) - ES": "Presidente Kennedy",
    "REALENGO (C) - RJ": "Rio de Janeiro",
    "RIO DE JANEIRO-MARAMBAIA (A) - RJ": "Rio de Janeiro",
    "RIO URUBU (A) - AM": "Itacoatiara",
    "SANTA MARTA (A) - SC": "Laguna",
    "SANTANA DO LIVRAMENTO (C) - RS": "Santana do Livramento",
    "SANTANA DO LIVRAMENTO (A) - RS": "Santana do Livramento",
    "SÃO FELIX DO ARAGUAIA (A) - MT": "São Félix do Araguaia",
    "SÃO LUIS DO PARAITINGA (A) - SP": "São Luís do Paraitinga",
    "SAO PAULO - MIRANTE (A) - SP": "São Paulo",
    "SAO S.DO PARAISO (C) - MG": "São Sebastião do Paraíso",
    "SERIDO (CAICO) (C) - RN": "Caicó",
    "SERRA DOS CARAJÁS (A) - PA": "Parauapebas",
    "S.G.DA CACHOEIRA(UAUPES) (C) - AM": "São Gabriel da Cachoeira",
    "S.J. DO RIO CLARO (A) - MT": "São José do Rio Claro",
    "STa. R. DE CASSIA (IBIPETUBA) (C) - BA": "Santa Rita de Cássia",
    "TARTARUGUALZINHO (A) - AP": "Tartarugalzinho",
    "TOMÉ AÇU (A) - PA": "Tomé-Açu",
    "TRÊS MARIA (A) - MG": "Três Marias",
    "USINA JUNQUEIRA (C) - SP": "Cravinhos",
    "VALE DO GURGUEIA (CRISTIANO CASTRO) (C) - PI": "Cristino Castro",
    "VILA MILITAR (A) - RJ": "Rio de Janeiro",
    "XEREM (A) - RJ": "Duque de Caxias"
}

ids_nao_mapeados = {
    "Santana do Livramento": 430016,
    "São Félix do Araguaia": 5107859,
    "São Luiz do Paraitinga": 3550001
}

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

async def fetch_estacoes():
    """Faz a requisição à API para buscar as estações"""

    url_estacoes = "https://sisdagro.inmet.gov.br/sisdagro/app/estacoes/list.json?_dc=1727402541975"
    
    try:
        async with httpx.AsyncClient() as client:
            logger.info(f"Fazendo requisição para buscar estações na URL: {url_estacoes}")
            response = await client.get(url_estacoes)
            response.raise_for_status()
            estacoes = response.json()["estacoes"]
            logger.info(f"{len(estacoes)} estações encontradas.")
            return estacoes
    except Exception as e:
        logger.error(f"Erro ao buscar estações: {e}")
        raise

async def fetch_dias_aptos(client: httpx.AsyncClient, estacao_id, pratica_agricola, data_plantio):
    """Faz a requisição à API para buscar os dias aptos para manejo solo"""

    url_manejo = "https://sisdagro.inmet.gov.br/sisdagro/app/climatologia/diasaptosmanejosolo/dams.json"
    
    payload = {
        'dataPlantio': data_plantio,
        'probabilidade': '1',
        'praticaAgricola': pratica_agricola,
        'dataInicial': 'Selecione',
        'culturaId': '',
        'estacaoId': estacao_id,
        'soloId': '',
        'cad': ''
    }

    try:
        logger.debug(f"Fazendo requisição para estacaoId: {estacao_id}, praticaAgricola: {pratica_agricola}")
        response = await client.post(url_manejo, data=payload)
        response.raise_for_status()
        logger.debug(f"Requisição bem-sucedida para estação {estacao_id} e prática agrícola {pratica_agricola}.")
        return response.json()["bhc"]
    except Exception as e:
        logger.error(f"Erro ao buscar dias aptos para estacaoId: {estacao_id}, praticaAgricola: {pratica_agricola}: {e}")
        raise

async def main():
    """Função principal que executa as requisições"""

    cidades = extrair_dados_municipios()

    # Obtendo 30 dias antes do dia atual
    data_plantio = (datetime.now() - timedelta(days=30)).strftime('%d/%m/%Y')    
    df = pd.DataFrame(columns=[
        'Cod. IBGE', 'Probabilidade', 'Pratica Agricola', 'Estação', 'Decêndio', 'Mês', 'Dias Aptos', 'Porcentagem Dias Aptos'
    ])
    
    logger.info(f"Iniciando busca com data de plantio: {data_plantio}")
    
    try:
        # Buscando as estações
        estacoes = await fetch_estacoes()
    except Exception as e:
        logger.critical("Falha ao buscar estações. Abortando execução.")
        return
    
    async with httpx.AsyncClient() as client:
        tasks = []
        
        # Iterando sobre cada estação
        for estacao in estacoes:
            estacao_id = estacao["codigoStr"]
            nome_estacao = estacao["nome"]
            id_cidade = buscar_id_por_nome(cidades, nome_estacao.split("(")[0].strip())
            if not id_cidade:
                municipio = locais_associacoes.get(nome_estacao, "local não encontrado")
                id_cidade = buscar_id_por_nome(cidades, municipio)
                if not id_cidade:
                    id_cidade = ids_nao_mapeados.get(municipio, None)

            logger.info(f"Processando estação: {nome_estacao} (ID: {estacao_id})")
            
            # Itera sobre as 3 práticas agrícolas
            for pratica_agricola in ['1', '2', '3']:
                try:
                    bhc_data_list = await fetch_dias_aptos(client, estacao_id, pratica_agricola, data_plantio)
                    rows_to_add = []

                    for bhc_data in bhc_data_list:
                        row = {
                            'Cod. IBGE': id_cidade,
                            'Probabilidade': 'Anual',
                            'Pratica Agricola': (
                                'Preparo do Solo' if pratica_agricola == '1' else
                                'Semeadura' if pratica_agricola == '2' else
                                'Colheita'
                            ),
                            'Estação': nome_estacao,
                            'Decêndio': bhc_data['decendio'],
                            'Mês': bhc_data['mes'],
                            'Dias Aptos': bhc_data['posicaoDia'],
                            'Porcentagem Dias Aptos': f"{bhc_data['valorDia']:.2f}%"
                        }
                        rows_to_add.append(row)

                    # Insira os valores da estação no Dataframe
                    df = pd.concat([df, pd.DataFrame(rows_to_add)], ignore_index=True)
                    
                except Exception as e:
                    logger.warning(f"Erro ao processar dados para a estação {nome_estacao}, prática agrícola {pratica_agricola}. Erro: {e}")

        logger.info(f"Dados processados com sucesso para estação {nome_estacao}.")
    
    try:
        # Salvando os dados em um arquivo Excel
        df.to_excel('dias_aptos_manejo_solo.xlsx', index=False)
        logger.info("Arquivo Excel gerado com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao salvar o arquivo Excel: {e}")

# Executando o código
if __name__ == "__main__":
    try:
        logger.info("Iniciando o programa de scraping.")
        asyncio.run(main())
    except Exception as e:
        logger.critical(f"Erro fatal no programa: {e}")
