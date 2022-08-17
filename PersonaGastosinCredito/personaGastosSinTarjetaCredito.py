#!/usr/bin/python3 

import sys
from types import new_class 
from pyspark.sql import SparkSession

#Vamos a crear la función con el map
def sumaTotal(linea):
    clavevalor = [] #creamos un array vacío donde guardaremos los valores
    new_linea = linea.split("\n") #Dividimos los datos de entrada en <nombre,metodo,saldo>

    for element in new_linea: #Recorremos cada uno de estos valores
        nombre, metodo, saldo = element.split(";") #Separamos
        claveValor = (nombre, int(saldo)) #Generamos la <clave-valor> que queremos emitir como <Nombre-Saldo>
        if metodo != 'Tarjeta de crédito': #Esta clave-valor se almacenará en el array si el método de pago es distinto de tarjeta de crédito
            clavevalor.append(claveValor)
        else:
            if nombre not in clavevalor: #Para cubrir el caso de Bob, una persona que solo ha pagado con tarjeta de crédito, añadimos la clave-valor <Nombre, 0> si no existe en el array.
                claveValor = (nombre, 0)
                clavevalor.append(claveValor)
    return clavevalor #Devolvemos el array

#Usando el reduceByKey sumamos todos los valores pertenecientes a la clave
def sumaSaldo(suma1, suma2):
    return suma1 + suma2


#iniciamos el programa
spark = SparkSession.builder.appName('SaldoSum').getOrCreate()  

#Asignamos a variable las rutas de la entrada y la salida
entrada = sys.argv[1] 
salida = sys.argv[2] 


#Leemos el
#  fichero de entrada
datosEntrada = spark.sparkContext.textFile(entrada)
suma = datosEntrada.flatMap(sumaTotal).reduceByKey(sumaSaldo)
#Para obtener la respuesta deseada vamora a iterar sobre "suma" usando un map y guardando la informacion con la estructura deseada
resultado = suma.map(lambda linea: linea[0]+";"+str(linea[1]))
resultado.repartition(1).saveAsTextFile(salida)


#spark-submit personaGastosSinTarjetaCredito.py file:/Users/adrihp/Master/MBID03/scriptsSpark/Problema1/entrada1 file:/Users/adrihp/Master/MBID03/scriptsSpark/Problema1/salida1