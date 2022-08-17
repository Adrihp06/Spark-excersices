#!/usr/bin/python3 


import sys
from types import new_class
import pyspark
from pyspark.sql import SparkSession
import numpy as np

#vamos a preparar la salida de los datos con el map
def PrepareData(linea):
    clavevalor=[]
    #debido a la naturaleza del fichero, simplemente haciendo el .split de las columnas deseadas somos capaces de extraer la información para el ejercicio
    #Es necesario hacer un try-except porque la presencia de datos vacíos arruina el reduceByKey siguiente
    #Si los datos existen los guardamos en un array, si existen los ignoramos
    try: 
        clave, valor = (linea.split("\t")[3]), int(linea.split("\t")[5])
        clavevalor.append((clave, valor))
    except Exception:
        pass
    return clavevalor

#iniciamos la sesión
spark = SparkSession.builder.appName('NameViews').getOrCreate()  

#Asignamos a variable las rutas de la entrada y la salida
entrada = sys.argv[1] 
salida = sys.argv[2] 


datosEntrada = spark.sparkContext.textFile(entrada)
#Vamos a resolver el problema con el map de la función y el reduceByKey usando lambda, después calculamos el min de igual manera
suma = datosEntrada.flatMap(PrepareData).reduceByKey(lambda x ,y: x + y).min(lambda x:x[1])
#Al hacer el .min() la variable suma pierde su estructura RDD, tras ponerlo de la forma deseada
resultado = [str(suma[0]+ ';' + str(suma[1]))]
#Transformamos la variable resultado a RDD usando parallelize y después exportamos
spark.sparkContext.parallelize(list(resultado)).repartition(1).saveAsTextFile(salida)

#spark-submit CategoriaDeVideosMenosVista.py 'file:/Users/adrihp/Master/MBID03/scriptsSpark/Problema2/0222/*.txt' file:/Users/adrihp/Master/MBID03/scriptsSpark/Problema2/salida2