Configuração do Ambiente

Antes de iniciar, certifique-se de ter o Hadoop e o PySpark instalados e configurados no seu ambiente. Para o exemplo a seguir, assume-se que um cluster Hadoop já esteja em funcionamento.
1. Coleta de Dados

Coleta de dados de sensores instalados na oficina de moto peças.
```sh
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

```

2. Carregar Dados no HDFS

Use o comando hdfs dfs -put para carregar o arquivo CSV no HDFS.

````sh
hdfs dfs -put dados_sensores.csv /user/Erebos/dados_sensores.csv

````
3. Análise de Dados com PySpark

Agora que os dados estão no HDFS, podemos usar PySpark para processá-los e analisar as tendências.

```sh
from pyspark.sql import SparkSession

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("AnaliseDadosOficina") \
    .getOrCreate()

# Carrega os dados do HDFS
dados_sensores_df = spark.read.csv("hdfs:///user/Erebos/dados_sensores.csv", header=True, inferSchema=True)

# Exibe o esquema dos dados
dados_sensores_df.printSchema()

# Exibe as primeiras linhas dos dados
dados_sensores_df.show(5)

# Análise de Dados

# Média de temperatura do motor
media_temperatura_motor = dados_sensores_df.groupBy().avg("temperatura_motor").collect()[0][0]
print(f"Média de temperatura do motor: {media_temperatura_motor:.2f}°C")

# Média de vibração do motor
media_vibracao_motor = dados_sensores_df.groupBy().avg("vibracao_motor").collect()[0][0]
print(f"Média de vibração do motor: {media_vibracao_motor:.2f}")

# Correlação entre velocidade e nível de combustível
correlacao_velocidade_combustivel = dados_sensores_df.stat.corr("velocidade_moto", "nivel_combustivel")
print(f"Correlação entre velocidade da moto e nível de combustível: {correlacao_velocidade_combustivel:.2f}")

# Salvar resultados da análise no HDFS
resultados = [
    {"metric": "media_temperatura_motor", "value": media_temperatura_motor},
    {"metric": "media_vibracao_motor", "value": media_vibracao_motor},
    {"metric": "correlacao_velocidade_combustivel", "value": correlacao_velocidade_combustivel}
]

resultados_df = spark.createDataFrame(resultados)
resultados_df.write.csv("hdfs:///user/Erebos/resultados_analise.csv", header=True)

# Encerra a sessão Spark
spark.stop()

````
Para criar um projeto que integre uma oficina de moto peças com tecnologias de Big Data utilizando Hadoop e Python, é necessário configurar um ambiente distribuído para armazenamento e processamento de dados. O seguinte código Python mostra como podemos configurar a coleta de dados, armazenar esses dados no Hadoop Distributed File System (HDFS) e realizar análises básicas usando PySpark.

### Configuração do Ambiente

Antes de iniciar, certifique-se de ter o Hadoop e o PySpark instalados e configurados no seu ambiente. Para o exemplo a seguir, assume-se que um cluster Hadoop já esteja em funcionamento.

### 1. Coleta de Dados

Coleta de dados de sensores instalados na oficina de moto peças.

```python
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
```

### 2. Carregar Dados no HDFS

Use o comando `hdfs dfs -put` para carregar o arquivo CSV no HDFS.

```sh
hdfs dfs -put dados_sensores.csv /user/Erebos/dados_sensores.csv
```

### 3. Análise de Dados com PySpark

Agora que os dados estão no HDFS, podemos usar PySpark para processá-los e analisar as tendências.

```python
from pyspark.sql import SparkSession

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("AnaliseDadosOficina") \
    .getOrCreate()

# Carrega os dados do HDFS
dados_sensores_df = spark.read.csv("hdfs:///user/Erebos/dados_sensores.csv", header=True, inferSchema=True)

# Exibe o esquema dos dados
dados_sensores_df.printSchema()

# Exibe as primeiras linhas dos dados
dados_sensores_df.show(5)

# Análise de Dados

# Média de temperatura do motor
media_temperatura_motor = dados_sensores_df.groupBy().avg("temperatura_motor").collect()[0][0]
print(f"Média de temperatura do motor: {media_temperatura_motor:.2f}°C")

# Média de vibração do motor
media_vibracao_motor = dados_sensores_df.groupBy().avg("vibracao_motor").collect()[0][0]
print(f"Média de vibração do motor: {media_vibracao_motor:.2f}")

# Correlação entre velocidade e nível de combustível
correlacao_velocidade_combustivel = dados_sensores_df.stat.corr("velocidade_moto", "nivel_combustivel")
print(f"Correlação entre velocidade da moto e nível de combustível: {correlacao_velocidade_combustivel:.2f}")

# Salvar resultados da análise no HDFS
resultados = [
    {"metric": "media_temperatura_motor", "value": media_temperatura_motor},
    {"metric": "media_vibracao_motor", "value": media_vibracao_motor},
    {"metric": "correlacao_velocidade_combustivel", "value": correlacao_velocidade_combustivel}
]

resultados_df = spark.createDataFrame(resultados)
resultados_df.write.csv("hdfs:///user/Erebos/resultados_analise.csv", header=True)

# Encerra a sessão Spark
spark.stop()
```

### Explicação do Código

1. **Coleta de Dados:**
    - Simulamos a coleta de dados de sensores instalados em motos.
    - Os dados coletados incluem a temperatura do motor, vibração do motor, velocidade da moto e nível de combustível.
    - Os dados são armazenados em um arquivo CSV.

2. **Carregar Dados no HDFS:**
    - Usamos o comando `hdfs dfs -put` para carregar o arquivo CSV no HDFS.

3. **Análise de Dados com PySpark:**
    - Criamos uma sessão Spark e carregamos os dados do HDFS.
    - Exibimos o esquema e as primeiras linhas dos dados para verificar a importação.
    - Realizamos análises básicas, como calcular a média da temperatura do motor, média de vibração do motor e a correlação entre a velocidade da moto e o nível de combustível.
    - Os resultados da análise são salvos de volta no HDFS.

### Observações Finais

Este é um exemplo básico para demonstrar como integrar a coleta de dados, armazenamento no HDFS e análise de dados usando PySpark em uma oficina de moto peças. Um projeto mais avançado, envolveria um planejamento mais detalhado, configuração do ambiente distribuído, segurança dos dados e análises mais complexas.
