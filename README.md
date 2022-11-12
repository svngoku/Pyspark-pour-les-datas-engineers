# Introduction à Pyspark pour les Data Engineers

## Prérequis

Pour suivre ce tutoriel, vous devez avoir installé les outils suivants:

- Python 3.10
- Spark 3.3.1
- Envrionnement jupyter Notebook
- Connaissance basique de Python, de Spark et de SQL

## Installation

- Installation de pyspark

```bash
pip3 install pyspark
```

## Aide mémoire de Spark

![Spark Cheat Sheet](https://res.cloudinary.com/dyd911kmh/image/upload/f_auto,q_auto:best/v1625837623/PySpark_Cheat_Sheet-_Spark_in_Python_owornh.png)

## Initialisation de Spark

```python
from pyspark.sql import SparkSession
sc = SparkSession.builder.getOrCreate()
```

## Inspecté la Session Spark

```python
sc.version #Retrieve SparkContext version
sc.pythonVer #Retrieve Python version
sc.master #Master URL to connect to
str(sc.sparkHome) #Path where Spark is installed on worker nodes
str(sc.sparkUser()) #Retrieve name of the Spark User running SparkContext
sc.appName #Return application name
sc.applicationld #Retrieve application ID
sc.defaultParallelism #Return default level of parallelism
sc.defaultMinPartitions #Default minimum number of partitions for RDDs
```

## Configuration

```python
from pyspark import SparkConf, SparkContext
conf = (
    SparkConf()
    .setMaster("local")
    .setAppName("My app")
    .set("spark. executor.memory","lg")
)
sc = SparkContext(conf = conf)
```