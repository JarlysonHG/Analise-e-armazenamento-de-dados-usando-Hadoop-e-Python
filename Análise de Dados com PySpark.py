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
