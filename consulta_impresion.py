#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys;
import argparse;
import random
import re
import mysql.connector
import csv
import time
import os

def obtenerConexionMySQL(database_name):
	'''Regresa la conexion de la base de datos'''
	conexion_en_disco = mysql.connector.connect(user='root', database=database_name, password='9821poa');
	return conexion_en_disco;

def obtenerArgs(argv):
    """Devuelve un objeto que tiene la información de los argumentos de línea de comandos"""	
    parser = argparse.ArgumentParser();
    parser.add_argument("-db", "--database", 
			help="Nombre de la base de datos", 
			type=str, default="encode");
    parser.add_argument("-niv", "--nivel",
                        help="nivel molecular de las muestras: cromosoma, lineaCelular, regionReguladora ",
                        type=str, default="cromosoma") 
    parser.add_argument("-c", "--coding",
                        help="Tipo de codificacion que se quiere analizar: voss, neighborJoining",
                        type=str, default="voss") 
    parser.add_argument("-car", "--cardinalidad",
                        help="Cardinalidad para crear los archivos: OneToOne, OneToMany",
                        type=str, nargs="+",
                        default="OneToOne");
    parser.add_argument("-N", "--tamanioMuestra", 
			help="Tamanio total de la muestra de donde se tomaran las bases de aprendizaje y prueba",
			type=str, default="400");
    parser.add_argument("-p", "--probabilidad",
			help="Probabilidad de ser seleccionado como instancia en la muestra",
			type=float, default="0.002");
    return parser.parse_args(argv);


def crearListaDatos(conexion, args):
	consultaNiveles = "SELECT DISTINCT " + args.nivel + " FROM dato"; # tomara mucho tiempo la consulta, deberemos sustituirlo por una lista
	cursor = conexion.cursor(buffered=True);
	cursor.execute(consultaNiveles);
	niveles = cursor.fetchall();
	listas = dict();
	header = ','.join(["f{:d}".format(x) for x in range(400)]);

	if args.cardinalidad == 'OneToOne': # Aqui se construyen las comparaciones de uno contra uno de los elementos recuperado
		print ("Calculando archivos OneToOne");
		header = header + "," + args.nivel;
		consultaElementos = "SELECT " + header + " FROM dato WHERE " + args.nivel + "=%s AND coding = %s AND RAND()<=%s LIMIT %s";
		for i in range(len(niveles)):
			cursor.execute(consultaElementos, (str(niveles[i][0]), args.coding, args.probabilidad, int(args.tamanioMuestra)));
			registros1 = cursor.fetchall();
			registros1.insert(0,tuple(header.split(',')));
			if niveles[i][0] not in listas:
				listas[niveles[i][0]] = dict();
			for j in range(i,len(niveles)):
				if i == j:
					continue;
				cursor.execute(consultaElementos, (str(niveles[j][0]), args.coding, args.probabilidad, int(args.tamanioMuestra)));
				registros2 = cursor.fetchall();
				if niveles[j][0] not in listas[niveles[i][0]]:
					listas[niveles[i][0]][niveles[j][0]] = None;
			
				listas[niveles[i][0]][niveles[j][0]] = registros1 + registros2;
				print(niveles[i][0] + "-" + niveles[j][0]);
		
	elif args.cardinalidad == 'OneToMany': # Aqui se construyen las comparacinoes de uno contra todos de los elementos recuperados
		print(args.cardinalidad);
	
	return listas;

def crearCSV(registros, nivel, coding):
	path = "./" + coding + "/" + nivel + "/";
	if not os.path.exists(path):
		os.makedirs(path);
	for e1 in registros.keys():
		for e2 in registros[e1]:
			ofile = open(path + e1 + "-" + e2 + ".csv" , "w");
			writer = csv.writer(ofile);
			print("Escribiendo archivo en disco: " + e1 + "-" + e2);
			for row in registros[e1][e2]:
				writer.writerow(row);
			ofile.close();

def main(argv):
	'''Entrada al programa'''
	args = obtenerArgs(argv);
	conexion = obtenerConexionMySQL(args.database);
	start = time.time();
	registros=crearListaDatos(conexion, args);
	crearCSV(registros, args.nivel, args.coding);
	end = time.time();
	conexion.close();
	print("Tiempo transcurrido: " + str(((end - start) / 60)) + " min");

if __name__ == "__main__":
	main(sys.argv[1:]);
