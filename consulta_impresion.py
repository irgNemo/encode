#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys;
import argparse;
import sqlite3
import sqlitebck
import random
import re
import mysql.connector

def obtenerConexion(database_name):
	'''Regresa la conexion de la base de datos'''
	conexion_en_disco = sqlite3.connect(database_name);
	#conexion_en_memoria = sqlite3.connect(":memory:");
	#sqlitebck.copy(conexion_en_disco, conexion_en_memoria);
	#conexion_en_disco.close();
	#return conexion_en_memoria;
	return conexion_en_disco;

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
                        help="Tipo de codificacion que se quiere analizar: v->voss, n->NeighborJoining",
                        type=str, default="v") 
    parser.add_argument("-car", "--cardinalidad",
                        help="Cardinalidad para crear los archivos: OneToOne, OneToMany",
                        type=str, nargs="+",
                        default="OneToOne");
    parser.add_argument("-N", "--tamanioMuestra", 
			help="Tamanio total de la muestra de donde se tomaran las bases de aprendizaje y prueba",
			type=str, default="400");
    return parser.parse_args(argv);


def crearListaDatos(conexion, args):

	consultaNiveles = "SELECT DISTINCT " + args.nivel + " FROM dato;";
	resultNiveles = conexion.cursor().execute(consultaNiveles);
	niveles = resultNiveles.fetchall();


	if args.cardinalidad == 'OneToOne': # Aqui se construyen las comparaciones de uno contra uno de los elementos recuperados
		listas = dict();
		consultaElementos = "SELECT * FROM dato where ";
		for i in range(len(niveles)):
			#consultaRegistrosPorNivel = conexion.cursor().execute(consultaElementos + args.nivel + "=" + niveles[i][0]);
			#registrosPorNivel = consultaRegistrosPorNivel.fetchall();
			for j in range(i,len(niveles)):
				if i == j:
					break;
				#consultaPorNivel2 = conexion.cursor().execute(consulta + args.nivel + "=" + niveles[j][0]);
				#registrosPorNivel2 = consultaPorNivel2.fetchall();
				print(niveles[i] + ":" + niveles[j]);
		
		#if elementos1 and elementos2:
		#	if not listas_key(e1[0]):
		#		listas[e1[0]] = dict();
		#	listas[e1[0]][e2[0]] = elementos1 + elementos2;
	
	elif args.cardinalidad == 'OneToMany': # Aqui se construyen las comparacinoes de uno contra todos de los elementos recuperados
		print(args.cardinalidad);


def crearCSV(registros):
	for tupla in registros: 
		tuplaStr = re.sub('[ ()\']', '', str(tupla));
		tuplaStr = re.sub('None','?',tuplaStr);	
		print (tuplaStr);
def main(argv):
	'''Entrada al programa'''
	args = obtenerArgs(argv);
	conexion = obtenerConexionMySQL(args.database);
	crearListaDatos(conexion, args);
	#if args.randomize:
	#	random.shuffle(registros);
	#print(",".join(cabecera));
	#crearCSV(registros);
	conexion.close();

'''	
	subConsulta = "(SELECT nombre_registro FROM sujeto WHERE tipoSujeto = ";
	count = 0;
	for tipoSujeto in args.tipoSujeto:
		if count == 0:
			subConsulta += "'" + tipoSujeto + "'";
		else:	
			subConsulta += " OR tipoSujeto = '" + tipoSujeto + "'";
		count += 1;
	subConsulta += ")";
	
	registros = list();
	cabeceraLista = list();
	
	if args.clase == "tipoSujeto":	
		for banda in args.bandas:
			for metrica in args.metricas:
				for experimento in args.experimentos:
					for electrodo in args.electrodos:
						cabeceraLista.append(banda + "_" + metrica + "_" +  experimento + "_"+ str(electrodo));
						consulta = "SELECT valor FROM dato WHERE experimento= '" + experimento  + "' and banda= '" + banda + "' and metrica='" + metrica + "' and electrodo=" + str(electrodo) + " and sujeto in " + subConsulta + " order by sujeto";
						query = conexion.cursor().execute(consulta);
						electrodoDatos = query.fetchall();
						if not registros: # Construye el arreglo de listas para ser llenado
							for i in range(len(electrodoDatos)):
								registros.append(list());
						for i in range(len(electrodoDatos)): # Se llena el arreglo
							if not registros[i]:
								registros[i] = list();
							registros[i].append(electrodoDatos[i][0]);
				
	elif args.clase == 'experimento':	
		count = 0;
		condicionExperimento = "(experimento = ";
		for experimento in args.experimentos:
			if count == 0:
				condicionExperimento += "'" + experimento + "'";
			else:
				condicionExperimento += " OR experimento = '" + experimento + "'";	
			count +=1;
		condicionExperimento += ")";	
		
		for banda in args.bandas:
			for metrica in args.metricas:
				for electrodo in args.electrodos:
					cabeceraLista.append(banda + "_" + metrica + "_" + str(electrodo));
					consulta = "SELECT valor FROM dato WHERE " + condicionExperimento  + " and banda= '" + banda + "' and metrica='" + metrica + "' and electrodo=" + str(electrodo) + " and sujeto in " + subConsulta + " order by sujeto";
					query = conexion.cursor().execute(consulta);
					electrodoDatos = query.fetchall();
					if not registros: # Construye el arreglo de listas para ser llenado
						for i in range(len(electrodoDatos)):
							registros.append(list());
					for i in range(len(electrodoDatos)): # Se llena el arreglo
						if not registros[i]:
							registros[i] = list();
						registros[i].append(electrodoDatos[i][0]);

	#aqui insertamos el atributo nombresujeto
	cabeceraLista.append('Sujeto');
	consulta="";
	if args.clase=='tipoSujeto':
		consulta = "SELECT sujeto FROM dato,sujeto WHERE experimento= '" + experimento  + "' and banda= '" + banda + "' and metrica='" + metrica + "' and electrodo=" + str(electrodo) + " and dato.sujeto = sujeto.nombre_registro and sujeto in " + subConsulta + " order by sujeto";
	elif args.clase == 'experimento':
		consulta="SELECT sujeto FROM dato,sujeto WHERE banda= '" + banda + "' and metrica='" + metrica + "' and electrodo=" + str(electrodo) + " and dato.sujeto=sujeto.nombre_registro and sujeto in "  + subConsulta + " order by sujeto";
	#print (consulta);
	query = conexion.cursor().execute(consulta);
	claseDatos = query.fetchall();
	for i in range(len(claseDatos)): # Se llena el arreglo
		registros[i].append(claseDatos[i][0]);

	#aqui insertamos la clase
	cabeceraLista.append('clase');
	consulta = "";
	if args.clase == 'tipoSujeto':
		consulta = "SELECT tipoSujeto FROM dato,sujeto WHERE experimento= '" + experimento  + "' and banda= '" + banda + "' and metrica='" + metrica + "' and electrodo=" + str(electrodo) + " and dato.sujeto = sujeto.nombre_registro and sujeto in " + subConsulta + " order by sujeto";
	elif args.clase == 'experimento':
		consulta="SELECT experimento FROM dato,sujeto WHERE banda= '" + banda + "' and metrica='" + metrica + "' and electrodo=" + str(electrodo) + " and dato.sujeto=sujeto.nombre_registro and sujeto in "  + subConsulta + " order by sujeto";
		#consulta="SELECT experimento FROM dato,sujeto WHERE banda= '" + banda + "' and metrica='" + metrica + "' and electrodo=" + str(electrodo) + " and dato.sujeto=sujeto.nombre_registro order by sujeto";
	#print (consulta);
	query = conexion.cursor().execute(consulta);
	claseDatos = query.fetchall();
	for i in range(len(claseDatos)): # Se llena el arreglo
		registros[i].append(claseDatos[i][0]);

	# Convierte la lista en tuplas para poder revolverlos con facilidad	
	for i in range(len(registros)):
		registros[i] = tuple(registros[i]);

	return registros, cabeceraLista;
'''

if __name__ == "__main__":
	main(sys.argv[1:]);
