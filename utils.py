#########################################
dbutils
#########################################

# Listar mountpoints
dbutils.fs.mounts()
 
# Listar pastas e ficheiros num certo caminho no DL
dbutils.fs.ls('dbfs:/mnt/edpd/src/exemplo')
 
# Remover pasta e/ou ficheiros dentro de um caminho no DL
dbutils.fs.rm('dbfs:/mnt/edpd/src/exemplo', recurse = True)

#########################################
Criar BD's em databricks com ligação ao DL
#########################################

# Criar uma pasta (bd) no DL com o nome 'raw'
 
import os
db_location = 'dbfs:/mnt/data/databases'
db_raw = 'raw'
spark.sql(f'create database if not exists {db_raw} location "{os.path.join(db_location, db_raw)}.db"')

#########################################
Avro to DataFrame
#########################################

def ReadAvro(path):
  return spark.read.format("avro").load(path)
  
#########################################
Guardar DataFrame na base de dados
#########################################

 def SaveDF(df, db, name, write_mode = 'overwrite'):
  df.write.mode(write_mode).format('delta').option('overwriteSchema','true').saveAsTable(f'{db}.{name}')
 
#########################################
Consolidar dados (de acordo com uma chave/s e coluna/s de ordenação)
#########################################

def ConsolidateDF(df, key, order):
  df_consolidated = df.withColumn("rn",f.row_number().over(Window.partitionBy([f.col(x) for x in key]).orderBy([f.col(x).desc() for x in order]))).filter(f.col("rn")==1).drop("rn")
  return df_consolidated
  
#########################################
Limpar todos os nomes de colunas de uma tabela (lower, remover pontuação, símbolos, e múltiplos espaços separados por '_')
#########################################

def CleanFeatureNames(df):
  import unidecode
  import string
   
  for name in df.columns:
    new_name = name.lower()
    new_name = unidecode.unidecode(new_name) # normalizar texto (ex: á -> a, ô -> o)
    new_name = new_name.translate(str.maketrans(string.punctuation, ' '*len(string.punctuation))) # remover pontuação
    new_name = '_'.join((' '.join(new_name.split())).split()) # Substituir 1 ou vários espaços por '_'
    df = df.withColumnRenamed(name, new_name)
  return df

#########################################
Adicionar um prefixo a todas as variáveis num DataFrame
#########################################

def PrefixAdd(df, prefix):
  for feature in df.columns: df = df.withColumnRenamed(feature, f'{prefix}_{feature}')
  return df

#########################################
Listar todos os valores para uma chave (Groupby list)
#########################################

# Listar todos os valores unicos (set)
df.groupBy('key').agg(f.collect_set('feature').alias('name'))
 
# Listar todos os valores com repetições (list)
df.groupBy('key').agg(f.collect_list('feature').alias('name'))
 
