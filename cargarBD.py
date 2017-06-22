#!/usr/bin/env python3
# En este código insertaremos todo los datos necesarios para hacer las consultas, los que serán llamados y mostrados por db.py 

import sys
import glob
#import numpy
import sqlite3
import math
import re
import argparse
import csv

def main(argv):
	args = getArgs(argv);
	conexion = crearConexion(args.database);
	createDatabase(conexion);
	loadDataFromTxtFiles(args.datasetsDirPath, conexion);
	#conexion.close();

def getArgs(argv):
	parser = argparse.ArgumentParser();
	parser.add_argument('-path','--datasetsDirPath', help='Directorio de la ruta a la carpeta con el dataset', type=str, default="../datasets/DataENCODE/");
	parser.add_argument('-db','--database', help='Nombre de la base de datos', type=str, default="encode.db");
	return parser.parse_args(argv);

def loadDataFromTxtFiles(dirPath, conexion):

	PATRON_DE_BUSQUEDA = "/([A-Za-z0-9]+)_Chromosome(\d+)_RR(\d+)(NeighborJoining|Voss)Per.txt";
	
	for filename in glob.glob(dirPath +  "*_Chromosome*_RR*.txt"):
		print(filename);
		matchObj = re.search(PATRON_DE_BUSQUEDA, filename);
		cromosoma = matchObj.group(2);
		regionReguladora = matchObj.group(3);
		lineaCelular = matchObj.group(1);
		print (cromosoma + regionReguladora + lineaCelular);
		f = open(filename);
		reader = csv.reader(f);
		tuplas = list();
		for fila in reader:
			fila += "," + cromosoma + "," + regionReguladora + "," + lineaCelular;
			t = tuple(fila);
			tuplas.append(t);

		conexion.cursor().executemany('INSERT INTO dato VALUES(?' + (',?' * 399) + ',?,?,?)', tuplas);
		f.close();	
		
	conexion.commit();
	
def createDatabase(conexion):
	cursor = conexion.cursor()
	header =  ','.join(["'{:d}' real".format(x) for x in range(400)]);
	sql = "CREATE TABLE IF NOT EXISTS dato(" + header + ", cromosoma text, regionReguladora text, lineaCelular text);";
	cursor.execute(sql);


	# Se crean índices para datos_locomocion para hacer más eficiente la consulta. Los nombres corresponden a las columnas de la BD. 
	headerIndex =  ','.join(["{:d}".format(x) for x in range(400)]);
	sqlIndex = "CREATE INDEX IF NOT EXISTS indices ON dato(" + headerIndex  + ", cromosoma, regionReguladora, lineaCelular);";
	cursor.execute(sqlIndex);
	conexion.commit();
	

def crearConexion(nombre_basedatos):
	return sqlite3.connect(nombre_basedatos);

if __name__ == "__main__":
	main(sys.argv[1:]);
