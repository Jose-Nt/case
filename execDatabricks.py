from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

# Definindo configurações da string de conexão
nomeChaveDeAcesso = ''
chaveDeAcesso = ''
nomeEventHub = ''
nameSpace = ''

stringDeConexao = f"Endpoint=sb://{nameSpace}.servicebus.windows.net/;SharedAccessKeyName={chaveDeAcesso};SharedAccessKey={chaveDeAcesso};EntityPath={nomeEventHub}"

# Definindo como o Spark se conectará com o Azure Event Hubs
# OBS: Caso futuramente hajam outras interações com esses mesmos dados, o event group deverá ser alterado para atender as novas necessidades
ehConf = {
    'eventhubs.connectionString': stringDeConexao,
    'eventhubs.consumerGroup': '$Default',
    'eventhubs.startingPosition': '@latest'
}

# Iniciando sessão spark já adicionando consifgurações para integração com o Delta Lake
spark = SparkSession.builder \
    .appName("Integracao-EventHub-Databricks") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Leitura >em tempo real< dos dados disponíveis no Event Hubs
eventHubStream = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

# Dfinindo o schema dos dados em questão
schema = StructType([
    StructField("dadoEmString", StringType(), True),
    StructField("dadoEmTimeStamp", TimestampType(), True)
    # E assim por diante...
])

# Decodificando a mensagem do Event Hub (por exemplo, JSON)
# Aqui os dados são convertidos em string e estruturados em um dataframe
eventStream = eventHubStream \
    .withColumn("body", col("body").cast("string")) \
    .select(from_json(col("body"), schema).alias("data")) \
    .select("data.*")

# Definindo diretório de armazenamento dos dados no Delta Lake
nomeContainer = ''
nomeContaArmazenamento = ''
pathDadosDeltaLake = ''
diretorio_delta_table = f"abfss://{nomeContainer}@{nomeContaArmazenamento}.dfs.core.windows.net/{pathDadosDeltaLake}"

# O método append foi escolhido devido ao fato de que, nesse caso, é interessante que só os novos dados sejam adicionados, e não o dataframe inteiro seja reescrito
query = eventStream.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "/mnt/delta/_checkpoints/eventhub-to-delta") \
    .start(diretorio_delta_table)

query.awaitTermination()