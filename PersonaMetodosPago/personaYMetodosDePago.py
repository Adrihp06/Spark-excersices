#!/usr/bin/python3 

import sys
from types import new_class 
from pyspark.sql import SparkSession

#Tenemos que crear dos funciones para poder dividir la informacion
def mapMayor(linea):
    
    #Separamos por linea
    new_linea = linea.split("\n")
    for element in new_linea:
        mayor_counter = []
        #Y separamos la informacion de cada linea en nombre, metodo y slado
        nombre, metodo , saldo = element.split(";")
        #si el metodo es tarjeta de credito y mayor que 1500 lo agregamos al array vacio con su contador
        if metodo == 'Tarjeta de crédito':
            if int(saldo) > 1500:
                mayor_counter.append((nombre, 1))
                #clave-valor: nombre-1
        else:
            mayor_counter.append((nombre, 0))
            #en caso de que no, guardamos la clave-valor: nombre-0 para devolver la información como dice el ejercicio
    return mayor_counter
#de igual manera pero con el saldo menor o igual a 1500
def mapMenor(linea):
    new_linea = linea.split("\n")
    for element in new_linea:
        menor_counter = []
        nombre, metodo , saldo = element.split(";")
        if metodo == 'Tarjeta de crédito':
            if int(saldo) <= 1500:
                menor_counter.append((nombre, 1))
        else:
            menor_counter.append((nombre, 0))
    return menor_counter



#iniciamso la sesion
spark = SparkSession.builder.appName('SaldoSum').getOrCreate()  

#leemos la entrada y las dos salidas correspondientes a las dos carpetas en donde vamoa a guardar la salida
entrada = sys.argv[1] 
salida1 = sys.argv[2] 
salida2 = sys.argv[3] 



datosEntrada = spark.sparkContext.textFile(entrada)
#Calculamos con un reduceByKey y la función anterior cuantas veces han gastado mas de 1500 con tarjeta de crédito
mayor = datosEntrada.flatMap(mapMayor).reduceByKey(lambda x, y: x +y)
resultadomayor = mayor.map(lambda linea: linea[0]+";"+str(linea[1])).repartition(1)
#Calculamos con un reduceByKey y la función anterior cuantas veces han gastado menos de 1500 con tarjeta de crédito
menor = datosEntrada.flatMap(mapMenor).reduceByKey(lambda x, y: x +y)
resultadomenor = menor.map(lambda linea: linea[0]+";"+str(linea[1])).repartition(1)

#Guardamos en las dos carpetas
resultadomayor.saveAsTextFile(salida1) 
resultadomenor.saveAsTextFile(salida2)

#spark-submit personaYMetodosDePago.py file:/Users/adrihp/Master/MBID03/scriptsSpark/Problema3/entrada3.txt file:/Users/adrihp/Master/MBID03/scriptsSpark/Problema3/comprasCreditoMayorDe1500 file:/Users/adrihp/Master/MBID03/scriptsSpark/Problema3/comprasCreditoMenorDe1500