import random
import time
import csv

# Simula a coleta de dados de sensores
def coletar_dados():
    dados = []
    for _ in range(1000):  # Simula 1000 leituras de sensores
        leitura = {
            'timestamp': time.time(),
            'temperatura_motor': round(random.uniform(60.0, 100.0), 2),
            'vibracao_motor': round(random.uniform(0.0, 10.0), 2),
            'velocidade_moto': round(random.uniform(0.0, 180.0), 2),
            'nivel_combustivel': round(random.uniform(0.0, 100.0), 2)
        }
        dados.append(leitura)
        time.sleep(0.1)  # Pausa para simular tempo real
    return dados

# Salva os dados em um arquivo CSV
def salvar_dados_csv(dados, caminho_arquivo):
    chaves = dados[0].keys()
    with open(caminho_arquivo, 'w', newline='') as arquivo_csv:
        escritor = csv.DictWriter(arquivo_csv, fieldnames=chaves)
        escritor.writeheader()
        escritor.writerows(dados)

dados_sensores = coletar_dados()
salvar_dados_csv(dados_sensores, 'dados_sensores.csv')
