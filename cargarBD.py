#!/usr/bin/env python3
# En este código insertaremos todo los datos necesarios para hacer las consultas, los que serán llamados y mostrados por db.py 

import sys
import glob
import h5py
import numpy
import sqlite3
import math
import re
import argparse
import csv

def main(argv):
	args = getArgs(argv);
	#conexion = crearConexion(args.database);
	#createDatabase(conexion);
	#loadSujetosFromCSVFile(args.sujetoCSVfile, conexion);
	loadDataFromTxtFiles(args.datasetsDirPath, conexion = None);
	#conexion.close();

def getArgs(argv):
	parser = argparse.ArgumentParser();
	parser.add_argument('--datasetsDirPath', help='Directorio de la ruta a la carpeta MATHResult', type=str, default="../datasets/DataEncode/");
	parser.add_argument('--database', help='Nombre de la base de datos', type=str, default="encode.db");
	return parser.parse_args(argv);
'''	
def loadDataFromTxtFiles(dirPath, conexion):

	DIRECTORIO_BASE = dirPath;

	#DIRECTORIO_ESTANDAR = "/MATHResults/Results";
	TIPOS_SUJETO = {'A':'alto', 'M':'medio', 'B':'bajo'};
	EXPERIMENTOS = {'D':'durante', 'Ds':'despues'}; 
	BANDAS = {'all':'all', 'a':'alpha', 'b':'beta', 'd':'delta', 'g':'gamma', 't':'theta'};
	METRICAS = ['C', 'E', 'Gio', 'EIGL'];
	ELECTRODOS = range(0, 20); 
	SUJETOS =  range(0, 60);
	tuplasDato = list();
	tuplasSujeto = list();
	
	for tipo_sujeto in TIPOS_SUJETO.keys():
		for experimento in EXPERIMENTOS.keys():
			patron_de_nombre_de_archivos = DIRECTORIO_BASE + DIRECTORIO_ESTANDAR + tipo_sujeto + experimento + "/*";
			archivos_de_evaluacion = glob.glob(patron_de_nombre_de_archivos); # glob.glob lista los archivos de acuerdo con un patrón dado.
			for archivo in archivos_de_evaluacion:
				datos = h5py.File(archivo, 'r');
				reObj = re.search(r'.*/([A-Z0-9]+)([a-z]?)_coherence_metricas.mat$',archivo);
				sujeto = reObj.group(1);
				banda = 'all' if not reObj.group(2) else BANDAS[reObj.group(2)]; # No he probado que funcione el else. Deberia de asignar all o el nombre de la banda
				for metrica in METRICAS:
					datos_de_metrica = numpy.array(datos[metrica]).transpose();
					for electrodo in ELECTRODOS:
						valor = datos_de_metrica[electrodo][0];
						tuplasDato.append((sujeto, experimento, banda, metrica, electrodo + 1, valor));

	
	cursor = conexion.cursor();
	cursor.executemany('INSERT INTO dato VALUES(?, ?, ?, ?, ?, ?)', tuplasDato);
	conexion.commit();
'''

def loadDataFromTxtFiles(dirPath, conexion):

	PATRON_DE_BUSQUEDA = "(.+)_Chromosome(\d+)_RR(\d+)(NeighborJoining|Voss)Per.txt";
	print (dirPath);
	for filename in glob.glob(dirPath + "*"):
		print(filename);
	
	#f = open(filename);
	#reader = csv.reader(f);
	#tuplas = list();
	#cont=0;
	#for fila in reader:
	#	if cont != 0:
	#		tipoSujeto = re.search('([AMB]).*', fila[1]).group(1);
	#		fila.append(tipoSujeto);
	#		t = tuple(fila);
	#		tuplas.append(t[1:]);
	#	cont+=1;

	#conexion.cursor().executemany('INSERT INTO sujeto VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', tuplas);
	#conexion.commit();
	
	#f.close();	


def createDatabase(conexion):
	cursor = conexion.cursor()
	cursor.execute(
		'''
		CREATE TABLE IF NOT EXISTS sujeto(
			codigo text,
			edad real,
			genero text,
			respuesta_correcta int,
			promedio_tiempo_reaccion float,
			nombre_registro text primary key,
			condicion_2sbder text,
			condicion_2sbizq text,
			observaciones text,
			tipoSujeto text
		)
		'''
	);

	cursor.execute(
	    '''
	    CREATE TABLE IF NOT EXISTS dato(
	    		sujeto string,
			experimento string,
			banda text,
			metrica text,
			electrodo int,
			valor real
	    )
	    '''
	);# Falta poner la llave foranea : FOREIGN KEY(sujeto) REFERENCES sujeto(nombre_registro)	

	# Se crean índices para datos_locomocion para hacer más eficiente la consulta. Los nombres corresponden a las columnas de la BD. 
	cursor.execute("CREATE INDEX IF NOT EXISTS indices ON dato(sujeto, experimento, banda, metrica, electrodo, valor)")
	cursor.execute("CREATE INDEX IF NOT EXISTS indices ON sujeto(codigo, edad, genero, respuesta_correcta, promedio_tiempo_reaccion, nombre_registro, condicion_2sbizq, condicion_2sbder)")
	conexion.commit();
	

def crearConexion(nombre_basedatos):
	return sqlite3.connect(nombre_basedatos);

if __name__ == "__main__":
	main(sys.argv[1:]);
