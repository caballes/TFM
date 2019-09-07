import pymongo
from pymongo import MongoClient

client= MongoClient('mongodb://localhost:27017/')
db = client.lv
outbreaks = db.outbreaks
migrations = db.migrations

especies = [1340, 1610]

for especie in especies:
	nodo = ()
	nodos = []
	query = "CREATE "
	used = []
	data_migrations = migrations.find({"Especie": especie})
	for migration in data_migrations:
		if not migration['geohash'][:4] in used:
			nodo = (migration['geohash'][:4], migration['Localidad'], migration['Municipio'], migration['Provincia'])
			nodos.append(nodo)
			used.append(migration['geohash'][:4])
		if not migration['geohashR'][:4] in used:
			nodo = (migration['geohashR'][:4], migration['LocalidadR'], migration['MunicipioR'], migration['ProvinciaR'])
			nodos.append(nodo)
			used.append(migration['geohashR'][:4])



	# Borramos los nodos repetidos.
	nodos2 = list(set(nodos))
	for nodo in nodos2:
		query += "({}:".format(nodo[0])
		query += "Region{location:"
		query += "'{}'".format(nodo[0])
		query += ", localidad:'{}'".format(nodo[1])
		query += ", municipio:'{}'".format(nodo[2])
		query += ", provincia:'{}'".format(nodo[3])
		query += "}), \n"

	valores = {}
	lista = []

	data_migrations = migrations.find({"Especie": especie})
	for migration in data_migrations:
		stringAux = "{}-{}".format(migration['geohash'][:4], migration['geohashR'][:4])
		if stringAux not in lista:
			lista.append(stringAux)
			valores[stringAux]=1
		else:
			valores[stringAux]+=1

	for element in lista :
		regiones = element.split('-')
		query += "({}) -[:MIGRA{}".format(regiones[0], especie)
		query += "{valor:"
		query += "{}".format(valores[element])
		query += "}]-> "
		query += "({}), \n".format(regiones[1])
		

	print(query)