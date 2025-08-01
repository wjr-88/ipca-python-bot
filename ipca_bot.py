import requests
import pandas as pd
from datetime import datetime
import os

# 🧠 Função 1 - Obter dados da API SIDRA
def obter_dados_ipca():
    """
    Faz requisição à API do IBGE para obter dados do IPCA.
    Retorna os dados em formato JSON.
    """
    url = "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"
    resposta = requests.get(url)
    
    if resposta.status_code == 200:
        return resposta.json()
    else:
        raise Exception(f"Erro ao acessar a API: {resposta.status_code}")

# 🔍 Função 2 - Processar os dados recebidos e transformá-los em DataFrame
def transformar_para_tabela(json_dados):
    """
    Extrai os períodos liberados do IPCA e converte em DataFrame.
    """
    periodos = json_dados.get("Periodos", {}).get("Periodos", [])
    dados_formatados = []

    for item in periodos:
        nome = item.get("Nome")
        codigo = item.get("Codigo")
        data_lib = item.get("DataLiberacao")

        if nome and data_lib:
            data_obj = datetime.fromisoformat(data_lib)
            if data_obj < datetime.now():
                dados_formatados.append({
                    "Período": nome,
                    "Código": codigo,
                    "Data de Liberação": data_obj
                })

    df = pd.DataFrame(dados_formatados)
    return df

# 📦 Função 3 - Exportar para arquivo parquet
def exportar_para_parquet(df, caminho="ipca_periodos.parquet"):
    """
    Exporta o DataFrame gerado para o formato Parquet.
    """
    df.to_parquet(caminho, index=False)
    print(f"✅ Arquivo exportado como: {caminho}")

# 🚀 Execução principal do bot
def rodar_bot_ipca():
    """
    Executa todas as etapas do robô de coleta IPCA:
    1. Obtem os dados
    2. Converte para DataFrame
    3. Exporta para arquivo parquet
    """
    print("🔄 Obtendo dados do IPCA...")
    dados = obter_dados_ipca()

    print("📊 Processando dados...")
    tabela_ipca = transformar_para_tabela(dados)

    print("📁 Salvando arquivo Parquet...")
    exportar_para_parquet(tabela_ipca)

# 🏁 Rodando o bot
if __name__ == "__main__":
    rodar_bot_ipca()