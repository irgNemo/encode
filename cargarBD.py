#!/usr/bin/env python3
# En este código insertaremos todo los datos necesarios para hacer las consultas, los que serán llamados y mostrados por db.py 

import sys
import glob
import sqlite3
import math
import re
import argparse
import csv
import time
import mysql.connector
from mysql.connector import errorcode


def main(argv):
	args = getArgs(argv);
	conexion = crearConexionMySQL(args.database);
	#createDatabaseMySQL(conexion);
	start = time.time();
	#loadDataFromTxtFilesMySQL(args.datasetsDirPath, conexion);
	end = time.time();
	conexion.close();
	print("Time elapsed: " + str(( (end - start) / 60 ) / 60 ) + " hrs");

def getArgs(argv):
	parser = argparse.ArgumentParser();
	parser.add_argument('-path','--datasetsDirPath', help='Directorio de la ruta a la carpeta con el dataset', type=str, default="../datasets/DataENCODE/");
	parser.add_argument('-db','--database', help='Nombre de la base de datos', type=str, default="encode");
	return parser.parse_args(argv);

def loadDataFromTxtFiles(dirPath, conexion):

	PATRON_DE_BUSQUEDA = "/([A-Za-z0-9]+)_Chromosome([0-9X]+)_RR(\d+)(NeighborJoining|Voss)Per.txt";
	
	for filename in glob.glob(dirPath +  "*_Chromosome*_RR*.txt"):
		print(filename);
		matchObj = re.search(PATRON_DE_BUSQUEDA, filename);
		cromosoma = matchObj.group(2);
		regionReguladora = matchObj.group(3);
		lineaCelular = matchObj.group(1);
		coding = matchObj.group(4);
		f = open(filename);
		reader = csv.reader(f);
		tuplas = list();
		for fila in reader:
			fila.append(cromosoma);
			fila.append(regionReguladora);
			fila.append(lineaCelular);
			fila.append(coding);
			t = tuple(fila);
			tuplas.append(t);
		conexion.cursor().executemany('INSERT INTO dato VALUES(?' + (',?' * 399) + ',?,?,?,?)', tuplas);
		f.close();	
		
	conexion.commit();

def loadDataFromTxtFilesMySQL(dirPath, conexion):

	PATRON_DE_BUSQUEDA = "/([A-Za-z0-9]+)_Chromosome([0-9X]+)_RR(\d+)(NeighborJoining|Voss)Per.txt";
	
	for filename in glob.glob(dirPath +  "*_Chromosome*_RR*.txt"):
		print(filename);
		matchObj = re.search(PATRON_DE_BUSQUEDA, filename);
		cromosoma = matchObj.group(2);
		regionReguladora = matchObj.group(3);
		lineaCelular = matchObj.group(1);
		coding = matchObj.group(4);
		header =  ','.join(["f{:d}".format(x) for x in range(400)]);
		f = open(filename);
		reader = csv.reader(f);
		tuplas = list();
		for fila in reader:
			fila.append(cromosoma);
			fila.append(regionReguladora);
			fila.append(lineaCelular);
			fila.append(coding);
			t = tuple(fila);
			tuplas.append(t);
		conexion.cursor().executemany('INSERT INTO dato (' + header + ', cromosoma, regionReguladora, lineaCelular, coding) VALUES(%s' + (',%s' * 399) + ',%s,%s,%s,%s)', tuplas);
		conexion.commit();
		f.close();	
		
	
def createDatabase(conexion):
	cursor = conexion.cursor()
	header =  ','.join(["'{:d}' real".format(x) for x in range(400)]);
	sql = "CREATE TABLE IF NOT EXISTS dato(" + header + ", cromosoma text, regionReguladora text, lineaCelular text, coding text);";
	cursor.execute(sql);

	# Se crean índices para datos_locomocion para hacer más eficiente la consulta. Los nombres corresponden a las columnas de la BD. 
	headerIndex =  ','.join(["'{:d}'".format(x) for x in range(400)]);
	sqlIndex = "CREATE INDEX IF NOT EXISTS indices ON dato(" + headerIndex  + ", cromosoma, regionReguladora, lineaCelular, coding);";
	cursor.execute(sqlIndex);
	
	conexion.commit();
	
def createDatabaseMySQL(conexion):
	cursor = conexion.cursor(buffered=True);
	header =  ','.join(["f{:d} float".format(x) for x in range(400)]);
	sql = "CREATE TABLE IF NOT EXISTS dato(" + header + ", cromosoma varchar(20), regionReguladora varchar(20), lineaCelular varchar(20), coding varchar(20));";
	cursor.execute(sql);
	conexion.commit();

def crearConexion(nombre_basedatos):
	return sqlite3.connect(nombre_basedatos);

def crearConexionMySQL(nombre_basedatos):
	try:
		con = mysql.connector.connect(user='root', password='9821poa', database=nombre_basedatos);
	except mysql.connector.Error as err:
		print("Error en la conexion: {}".format(err));
		con = mysql.connector.connect(user='root', password='9821poa');
		con.cursor().execute('CREATE DATABASE ' + nombre_basedatos);
		con.cursor().execute('USE ' + nombre_basedatos);
	return con; 


if __name__ == "__main__":
	main(sys.argv[1:]);
