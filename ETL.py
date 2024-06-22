# -*- coding: utf-8 -*-
import sys
import csv
import psycopg2
import io
import time
import random as rand
import os

#-----------------Variables Globales------------------#
error_con                = False
contador                 = 0
contador_reparaciones    = 0
contador_actualizaciones = 0
contador_acentuaciones   = 0
contador_creaciones      = 0
reparacionesETL          = 0     #Reparaciones hechas a medida que se extraían los datos
reparacionesELT          = 0     #Reparaciones hechas desde las tablas de la base de datos
detalle_reparaciones     = []    #Lista con el detalle de las reparaciones por campos vacíos

#Parámetros de conexion a la base de datos
v_host =     "localhost"
v_port =     "5432"
v_database = "Graduados_BD"
v_user =     "postgres"
v_password = "12345"
directorio_actual = os.getcwd()

#---------------------------------------------------------------------------------------------------------------
#Clase: Carga Tabla Temporal
#---------------------------------------------------------------------------------------------------------------
def cargaTablaTemporal(conn, cur, contador, id_institucion, ies_padre, nombre_institucion, prin_o_sec, id_sector, sector, id_caracter, caracter,
                       id_depto_domicilio, depto_domicilio, id_municipio, municipio, cod_snies_programa, programa, id_nivel_academico, nivel_academico,
                       id_nivel_formacion, nivel_formacion,id_metodologia, metodologia, id_area_conocimiento, area_conocimiento, id_nucleo, nucleo, 
                       id_depto_programa, depto_programa, id_municipio_programa, municipio_programa, id_sexo, sexo, semestre, graduados, año):
    
    print("Cargando temporal... ->",id_institucion, graduados, año, "registro #", contador)
    
    try:
        command = '''INSERT INTO temporal(id_institucion, ies_padre, nombre_institucion, prin_o_sec, id_sector, sector, id_caracter, caracter,
                       id_depto_domicilio, depto_domicilio, id_municipio_domicilio, municipio, cod_snies_programa, programa, id_nivel_academico, nivel_academico,
                       id_nivel_formacion, nivel_formacion,id_metodologia, metodologia, id_area_conocimiento, area_conocimiento, id_nucleo, nucleo, 
                       id_depto_programa, depto_programa, id_municipio_programa, municipio_programa, id_sexo, sexo, semestre, graduados, año_registro)
                       VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    #33 values
        cur.execute(command, (id_institucion, ies_padre, nombre_institucion, prin_o_sec, id_sector, sector, id_caracter, caracter,
                              id_depto_domicilio, depto_domicilio, id_municipio, municipio, cod_snies_programa, programa, id_nivel_academico, nivel_academico,
                              id_nivel_formacion, nivel_formacion,id_metodologia, metodologia, id_area_conocimiento, area_conocimiento, id_nucleo, nucleo, 
                              id_depto_programa, depto_programa, id_municipio_programa, municipio_programa, id_sexo, sexo, semestre, graduados, año))
        conn.commit()
    except(Exception) as error:
        print("Error: ", error)
        sys.exit("Error: Carga en la tabla temporal.")


#---------------------------------------------------------------------------------------------------------------
#Clase: Carga tbl_instituciones
#---------------------------------------------------------------------------------------------------------------
def cargarInstituciones(conn, cur, id_institucion, nombre_institucion):
    #Verifica que no exista el id de la institucion
    command = f"SELECT * FROM tbl_instituciones WHERE id_institucion = {id_institucion}"
    cur.execute(command)
    registros = cursor.fetchall()
    
    #Si la consulta devuelve un largo de 0 registros encontrados, se crea la institucion en cuestión
    if len(registros) == 0:
        command = f"INSERT INTO tbl_instituciones (id_institucion, nombre_institucion) VALUES ({id_institucion},'{nombre_institucion}')"
        cur.execute(command, (id_institucion, nombre_institucion))
        print(f"Institucion Cargada: {nombre_institucion}")
        conn.commit()
        

#---------------------------------------------------------------------------------------------------------------
#Clase: Carga tbl_departamento
#---------------------------------------------------------------------------------------------------------------
def cargarDepartamentos(conn,cur,id_departamento, nombre_departamento):
    #Verifica que el codigo del departamento no exista
    command = f"SELECT * FROM tbl_departamento WHERE id_departamento = {id_departamento}"
    cur.execute(command)
    registros = cursor.fetchall()
    
    #Si la consulta devuelve un largo de 0 registros encontrados, se crea el departamento en cuestión
    if len(registros) == 0:
        command = f"INSERT INTO tbl_departamento (id_departamento, nombre_departamento) VALUES({id_departamento}, '{nombre_departamento}')"
        cur.execute(command, (id_departamento, nombre_departamento))
        print(f"Departamento Cargado: {nombre_departamento} ID: {id_departamento}")
        conn.commit()
        

#---------------------------------------------------------------------------------------------------------------
#Clase: Carga tbl_municipio
#---------------------------------------------------------------------------------------------------------------
def cargarMunicipios(conn, cur, id_municipio, nombre_municipio):
    #Verifica que el codigo del municipio no exista
    command = f"SELECT * FROM tbl_municipio WHERE id_municipio = {id_municipio}"
    cur.execute(command)
    registros = cursor.fetchall()

    #Si la consulta devuelve un largo de 0 registros encontrados, se crea el municipio en cuestión
    if len(registros) == 0:
        command = f"INSERT INTO tbl_municipio (id_municipio, nombre_municipio) VALUES({id_municipio}, '{nombre_municipio}')"
        cur.execute(command, (id_municipio, nombre_municipio))
        print(f"Municipio Cargado: {nombre_municipio} ID: {id_municipio}")
        conn.commit()


#---------------------------------------------------------------------------------------------------------------
#Clase: Carga tbl_programa
#---------------------------------------------------------------------------------------------------------------
def cargarProgramas(conn, cur, cod_snies, nombre_programa):
    #Verifica que el cod snies del programa no exista
    command = f"SELECT * FROM tbl_programa where cod_snies = {cod_snies}"
    cur.execute(command)
    registros = cursor.fetchall()
    
    #Si la consulta devuelve un largo de 0 registros encontrados, se crea el programa en cuestión
    if len(registros) == 0:
        command = f"INSERT INTO tbl_programa (cod_snies, nombre_programa) VALUES({cod_snies}, '{nombre_programa}')"
        cur.execute(command, (cod_snies, nombre_programa))
        print(f"Programa creado: {nombre_programa} ID: {cod_snies}")
        conn.commit()    


#---------------------------------------------------------------------------------------------------------------
#Clase: Carga tbl_nivel_formacion
#---------------------------------------------------------------------------------------------------------------
def cargarNivelFormacion(conn, cur, id_nivel_formacion, desc_nivel_formacion):
    #verifica que el id del nivel_formacion no exista
    command = f"SELECT * FROM tbl_nivel_formacion WHERE id_nivel_formacion = {id_nivel_formacion}"
    cur.execute(command)
    registros = cursor.fetchall()

    #Si la consulta devuelve un largo de 0 registros encontrados, se crea el nivel de formacion en cuestión
    if len(registros) == 0:
        command = f"INSERT INTO tbl_nivel_formacion (id_nivel_formacion, desc_nivel_formacion) VALUES({id_nivel_formacion}, '{desc_nivel_formacion}')"
        cur.execute(command,(id_nivel_formacion,desc_nivel_formacion))
        print(f"Nivel de formacion creado: {desc_nivel_formacion} ID: {id_nivel_formacion}")
        conn.commit()

#---------------------------------------------------------------------------------------------------------------
#Clase: Carga tbl_area_conocimiento
#---------------------------------------------------------------------------------------------------------------
def cargaAreaConocimiento(conn, cur, id_area_conocimiento, desc_area_conocimiento):
    #verifica que el id del area de conocimiento no exista
    command = f"SELECT * FROM tbl_area_conocimiento where id_area_conocimiento = {id_area_conocimiento}"
    cur.execute(command)
    registros = cur.fetchall()
    
    #Si la consulta devuelve un largo de 0 registros encontrados, se crea el area de conocimiento en cuestión
    if len(registros) == 0:
        command = f"INSERT INTO tbl_area_conocimiento (id_area_conocimiento, desc_area_conocimiento) VALUES({id_area_conocimiento}, '{desc_area_conocimiento}');"
        cur.execute(command,(id_area_conocimiento,desc_area_conocimiento))
        print(f"Area de conocimiento creada: {desc_area_conocimiento} ID: {id_area_conocimiento}") 
        conn.commit()


#---------------------------------------------------------------------------------------------------------------
#Clase: Carga tbl_nucleo
#---------------------------------------------------------------------------------------------------------------
def cargarNucleos(conn, cur, id_nucleo, desc_nucleo):
    #verifica que el id del nucleo no exista
    command = f"SELECT * FROM tbl_nucleo WHERE id_nucleo = {id_nucleo}"
    cur.execute(command)
    registros = cur.fetchall()
    
    #Si la consulta devuelve un largo de 0 registros encontrados, se crea el nucleo en cuestión
    if len(registros) == 0:
        command = f"INSERT INTO tbl_nucleo (id_nucleo, desc_nucleo) VALUES({id_nucleo}, '{desc_nucleo}')"
        cur.execute(command,(id_nucleo,desc_nucleo))
        print(f"Nucleo creado: {desc_nucleo}, ID: {id_nucleo}")
        conn.commit()


#---------------------------------------------------------------------------------------------------------------
#Clase: Carga tbl_institucion
#---------------------------------------------------------------------------------------------------------------
def cargarInstitucion(conn, cur, id_institucion, ies_padre, prin_o_sec, id_sector, id_caracter, id_depto_domicilio, depto_domicilio, id_municipio_domicilio,municipio_domicilio, cod_snies_programa,
                        id_nivel_academico, id_nivel_formacion, id_metodologia, id_area_conocimiento, id_nucleo, id_depto_programa,depto_programa, id_municipio_programa,municipio_programa, id_sexo,
                        semestre, graduados, año_registro,nombre_institucion, cont):
    
    if prin_o_sec == "principal":
        prin_o_sec = 1
    elif prin_o_sec == "seccional":
        prin_o_sec = 2
    else:
        detalle_reparaciones.append(f"CUIDADO: VALOR DESCONOCIDO: {prin_o_sec}. deberia ser 1 o 2")
        
    #ID DEPTO REPARADO DESDE EL SERVIDOR
    command = f"select id_departamento from tbl_departamento where id_departamento = {id_depto_domicilio}"
    cur.execute(command)
    rows = cur.fetchone()
    if not rows:
        command = f"select id_departamento from tbl_departamento where nombre_departamento = {depto_domicilio}"
        cur.execute(command)
        dato_reparado = cur.fetchone()
        if len(dato_reparado) > 0:     
            detalle_reparaciones.append(f"CREADO EN EL SERVIDOR: id_depto {id_municipio_domicilio} NO EXISTENTE -> {dato_reparado[0]} registro #{contador}")
            id_depto_domicilio = dato_reparado[0]
            print(f"REPARADO EN EL SERVIDOR: id_depto {id_municipio_domicilio} NO EXISTENTE -> {dato_reparado[0]} registro #{contador}")
            sumar_contador_reparaciones(4,2)
        else:
            depto_domicilio = 11
            detalle_reparaciones.append(f"CREADO EN EL SERVIDOR: id_depto {id_municipio_domicilio} NO EXISTENTE -> {dato_reparado[0]} registro #{contador}")
            sumar_contador_reparaciones(4,2)
            print(id_depto_domicilio)
   
        
    #ID MUNICIPIO REPARADO DESDE EL SERVIDOR

    command = f"select id_municipio from tbl_municipio where id_municipio = {id_municipio_domicilio}"
    cur.execute(command)
    rows = cur.fetchone()
    if not rows:
        command = f"select id_municipio from tbl_municipio where nombre_municipio = {municipio_domicilio}"
        cur.execute(command)
        dato_reparado = cur.fetchone()
        if len(dato_reparado) > 0:  
            detalle_reparaciones.append(f"CREADO EN EL SERVIDOR: id_municipio {id_municipio_domicilio} NO EXISTENTE -> {dato_reparado[0]}")
            print(f"REPARADO EN EL SERVIDOR: id_municipio {id_municipio_domicilio} NO EXISTENTE -> {dato_reparado[0]}")
            sumar_contador_reparaciones(4,2)
            id_municipio_domicilio = dato_reparado[0]
        else:
            id_municipio_domicilio = 11001
            detalle_reparaciones.append(f"CREADO EN EL SERVIDOR: id_municipio {id_municipio_domicilio} NO EXISTENTE -> {dato_reparado[0]}")
            sumar_contador_reparaciones(4,2)

    #ID MUNICIPIO PROGRAMA NO EXITENTE REPARADO DESDE EL SERVIDOR
    command = f"select id_municipio from tbl_municipio where id_municipio = {id_municipio_programa}"
    cur.execute(command)
    rows = cur.fetchone()
    if not rows:
        command = f"INSERT INTO tbl_municipio (id_municipio, nombre_municipio) VALUES({id_municipio_programa},'{municipio_programa}')"
        cur.execute(command)
        print(f"CREADO EN EL SERVIDOR: municipio {municipio_programa}, {id_municipio_programa}")
        detalle_reparaciones.append(f"CREADO EN EL SERVIDOR: municipio {municipio_programa}, {id_municipio_programa}")
        sumar_contador_reparaciones(4,2)
        
    #ID DEPTO PROGRMA NO EXISTENTE REPARADO DESDE EL SERVIDOR
    command = f"select id_departamento from tbl_departamento where id_departamento = {id_depto_programa}"
    cur.execute(command)
    rows = cur.fetchone()
    if not rows:
        command = f"INSERT INTO tbL_departamento (id_departamento, nombre_departamento) VALUES({id_depto_programa}, '{depto_programa}')"
        cur.execute(command)
        print(f"CREADO EN EL SERVIDOR: departamento {depto_programa}, {id_depto_programa}")
        detalle_reparaciones.append(f"CREADO EN EL SERVIDOR: departamento {depto_programa}, {id_depto_programa}")
        sumar_contador_reparaciones(4,2)


    print(f"institucion creada-> n: {nombre_institucion}, p/s{prin_o_sec} registro #{contador}")
    command = f"INSERT INTO tbl_institucion (id_institucion, ies_padre, prin_o_sec, id_sector, id_caracter, id_depto_domicilio, id_municipio_domicilio, cod_snies_programa, id_nivel_academico, id_nivel_formacion, id_metodologia, id_area_conocimiento, id_nucleo, id_depto_programa, id_municipio_programa, id_sexo, semestre, graduados, año_registro) VALUES({id_institucion}, {ies_padre}, {prin_o_sec}, {id_sector}, {id_caracter}, {id_depto_domicilio}, {id_municipio_domicilio}, {cod_snies_programa}, {id_nivel_academico}, {id_nivel_formacion}, {id_metodologia}, {id_area_conocimiento}, {id_nucleo}, {id_depto_programa}, {id_municipio_programa}, {id_sexo}, {semestre}, {graduados}, {año_registro})"
    cur.execute(command)
    conn.commit()

#---------------------------------------------------------------------------------------------------------------
#Excentas de carga: tbl_principal_seccional, tbl_sector, tbl_caracter, tbl_nivel_academico, tbl_metodologia, tbl_año, tbl_sexo
#Razon: Tienen 4 o menos registros, se insertó manualmente al momento de la creación de la base de datos
#---------------------------------------------------------------------------------------------------------------


#############----------------CODIGO DEL PROGRAMA------------------------##########

#Medir el tiempo que se demora en leer y transferir a la bd
tiempo_inicial = time.time()

#---------------------------------------------------------------------------------------------------------------
#CONEXIÓN A BD
#---------------------------------------------------------------------------------------------------------------
try:
    connection = psycopg2.connect(user = v_user, password = v_password, host= v_host, port = v_port, database = v_database)
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("Estás conectado a - ", record, "\n")
except(Exception) as error:
    print("Error: ", error)
    error_con = True
finally:
    if(error_con):
        sys.exit("Error de conexión al servidor")


#---------------------------------------------------------------------------------------------------------------
#Funciones de limpieza
#---------------------------------------------------------------------------------------------------------------

#Funcion creada por nosotros que verifica si un dato puede ser convertido a otro.
#Devuelve el dato convertido si se puede y true si puede ser transformado.
#Devuelve el dato no-convertido si no se puede y false si no puede ser transformado.
def tryParse(dato, tipo_dato):
    #dato = dato.replace('"','') <----Eliminar '"' no es necesario ahora que construimos un programa para exportar
    try:
        return tipo_dato(dato), True
    except (ValueError, TypeError):
        return dato, False
    
    
#Si el dato que deberia ser entero no es entero o si el dato que deberia ser string no es string, imprime la advertencia
def analyze(name, result, dataType, contador):
    if not result:
        print(f"No se pudo transformar {name} a {dataType}. Registro # {contador}")
    

#Muestra que reparaciones está realizando el programa y cuales se intentaron pero no se pudieron reparar.
def display_reparations(state, name, value, cont):
    if state:
        sumar_contador_reparaciones(1,1)
        detalle_reparaciones.append(f"REPARACION EXITOSA: {name} se reparo a valor {value} en registro: {cont}")
        print(f"REPARACION EXITOSA: {name} se reparo a valor {value} en registro: {cont}")
    else:
        print(f"REPARACION FALLIDA: No se pudo reparar {name} en registro: {cont}")

#Elimina las letra que contengan acentos
def eliminar_acentos(palabra, contador):
    acentos = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "Á": "A", "É": "E", "Í": "I", "Ó":"O", "Ú":"U"}
    palabra_nueva = ""
    ban = False
    removidos = []
    for letra in palabra:
        if letra in acentos:
            palabra_nueva += acentos[letra]
            removidos.append(letra)
            ban = True
        else:
            palabra_nueva+=letra
    
    if ban:
        detalle_reparaciones.append(f"ACENTO ELIMINADO de {palabra} : {removidos}. Registro: {contador}")
        sumar_contador_reparaciones(3,1)
    return palabra_nueva

#Se encarga de sumar la variable global ya que una variable global no puede ser sumada así como así en python
def sumar_contador_reparaciones(tp, reptp):
    if tp == 1:
        global contador_reparaciones
        contador_reparaciones += 1
    elif tp == 2:
        global contador_actualizaciones
        contador_actualizaciones += 1
    elif tp == 3:
        global contador_acentuaciones
        contador_acentuaciones += 1
    elif tp == 4:
        global contador_creaciones
        contador_creaciones += 1
        
    if reptp == 1:
        global reparacionesETL
        reparacionesETL+=1
    elif reptp == 2:
        global reparacionesELT
        reparacionesELT += 1
        
    


#---------------------------------------------------------------------------------------------------------------
#Limpieza de tablas
#---------------------------------------------------------------------------------------------------------------
print('-----------------Limpieza de tablas---------------')
command = '''DELETE FROM temporal'''
cursor.execute(command)
print('-----------------Se limpió: Temporal---------------')
command = '''DELETE FROM tbl_instituciones'''
cursor.execute(command)
command = '''DELETE FROM tbl_departamento'''
print('-----------------Se limpió: Departamento---------------')
cursor.execute(command)
command = '''DELETE FROM tbl_municipio'''
print('-----------------Se limpió: Municipio---------------')
cursor.execute(command)
command = '''DELETE FROM tbl_programa'''
print('-----------------Se limpió: Programa---------------')
cursor.execute(command)
command = '''DELETE FROM tbl_nivel_formacion'''
print('-----------------Se limpió: Formación---------------')
cursor.execute(command)
command = '''DELETE FROM tbl_area_conocimiento'''
print('-----------------Se limpió: Conocimiento---------------')
cursor.execute(command)
command = '''DELETE FROM tbl_nucleo'''
print('-----------------Se limpió: Nucleo---------------')
cursor.execute(command)
command = '''DELETE FROM tbl_institucion'''
cursor.execute(command)

#---------------------------------------------------------------------------------------------------------------
#Codigo del programa - Extraction - ABRIR EL ARCHIVO CSV - ENCODING UTF-8
#---------------------------------------------------------------------------------------------------------------
try:
    archivo = 'unificado_finalr.csv'
    with io.open(archivo, encoding=('utf-8')) as File:
        reader = csv.reader(File, delimiter='|', quotechar=(','), quoting=csv.QUOTE_MINIMAL)
        #contar los registros
        for row in reader:
            id_institucion        = row[0].strip() #int strip = trim
            ies_padre             = row[1].strip() #int
            nombre_institucion    = row[2].strip() # FK
            prin_o_sec            = row[3].strip().lower()
            id_sector             = row[4].strip() #int
            sector                = row[5].strip()
            id_caracter           = row[6].strip() #int
            caracter              = row[7].strip()
            id_depto_domicilio    = row[8].strip() #int
            depto_domicilio       = row[9].strip()
            id_municipio          = row[10].strip() #int
            municipio             = row[11].strip()
            cod_snies_programa    = row[12].strip() #int
            programa              = row[13].strip()
            id_nivel_academico    = row[14].strip() #int
            nivel_academico       = row[15].strip()
            id_nivel_formacion    = row[16].strip() #int
            nivel_formacion       = row[17].strip() #amarillo especializacion
            id_metodologia        = row[18].strip() #int
            metodologia           = row[19].strip()
            id_area_conocimiento  = row[20].strip() #int
            area_conocimiento     = row[21].strip()
            id_nucleo             = row[22].strip() #int 
            nucleo                = row[23].strip()
            id_depto_programa     = row[24].strip() #int
            depto_programa        = row[25].strip()
            id_municipio_programa = row[26].strip() #int
            municipio_programa    = row[27].strip()
            id_sexo               = row[28].strip() #int
            sexo                  = row[29].strip()
            semestre              = row[30].strip() #int
            graduados             = row[31].strip() #int
            año                   = row[32].strip() #int
            
            #limpieza de datos ---TRANSFORM---
            #Cáscaras
            #Algunas estaban sin clasificar, se reemplazaron por -1 porque la base de datos esperaba un numero, no un texto
            if id_area_conocimiento == "Sin clasificar": 
                id_area_conocimiento = -1
                detalle_reparaciones.append(f"ACTUALIZACION EXITOSA: id_area_conocimiento 'Sin clasificar' -> -1 registro:{contador}")
                sumar_contador_reparaciones(2,1)
            
            if id_nucleo == "Sin clasificar":
                id_nucleo = -1
                detalle_reparaciones.append(f"ACTUALIZACION EXITOSA: id_nucleo 'Sin clasificar' -> -1 registro:{contador}")
                sumar_contador_reparaciones(2,1)
                
            if id_metodologia != 1 and id_metodologia != 2:
                detalle_reparaciones.append(f"ACTUALIZACION EXITOSA: id_metodologia: {id_metodologia} -> 1 Registro: {contador}")
                id_metodologia = 1
                sumar_contador_reparaciones(2,1)
                
            ##Recortamos el nombre de Archipielago De San Andres, Providencia Y Santa Catalina porque interfiere de forma negativa en la visualizacion de los gráficos de Metabase - 19/5/23 14:27
            if depto_domicilio == "Archipielago De San Andres, Providencia Y Santa Catalina":
                depto_domicilio = "San Andres Y Providencia"
                sumar_contador_reparaciones(2,1)
            
            if depto_programa == "Archipielago De San Andres, Providencia Y Santa Catalina":
                depto_programa = "San Andres Y Providencia"
                sumar_contador_reparaciones(2,1)
                
            
            #Verificar si el dato en cuestion es un numero o un string Y LO TRANSFORMA 
            #Limpia los datos de las '"' generadas por el archivo CSV
            #Elimina los acentos que haya en la palabra
            id_institucion, result        = tryParse(id_institucion, int)
            analyze("id_institucion", result, "entero", contador)           
            ies_padre, result             = tryParse(ies_padre, int)
            analyze("ies_padre", result, "entero", contador)           
            nombre_institucion, result    = tryParse(nombre_institucion, str)
            nombre_institucion            = eliminar_acentos(nombre_institucion, contador)
            analyze("nombre_institucion", result, "string", contador)              
            prin_o_sec, result            = tryParse(prin_o_sec, str)
            prin_o_sec                    = eliminar_acentos(prin_o_sec, contador)
            analyze("prin_o_sec", result, "string", contador)           
            id_sector, result             = tryParse(id_sector, int)
            analyze("id_sector", result, "entero", contador) 
            sector, result                = tryParse(sector, str)
            sector                        =eliminar_acentos(sector, contador)
            analyze("sector", result, "string", contador)                      
            id_caracter, result           = tryParse(id_caracter, int)
            analyze("id_caracter", result, "entero", contador)           
            caracter, result              = tryParse(caracter, str)
            caracter                      = eliminar_acentos(caracter, contador)
            analyze("caracter", result, "string", contador)           
            id_depto_domicilio, result    = tryParse(id_depto_domicilio, int)
            analyze("id_depto_domicilio", result, "entero", contador)           
            depto_domicilio, result       = tryParse(depto_domicilio, str)
            depto_domicilio               = eliminar_acentos(depto_domicilio, contador)
            analyze("depto_domicilio", result, "string", contador)
            id_municipio, result          = tryParse(id_municipio, int)
            analyze("id_municipio", result, "entero", contador)           
            municipio, result             = tryParse(municipio, str)
            municipio                     = eliminar_acentos(municipio,contador)
            analyze("municipio", result, "string", contador)
            cod_snies_programa, result    = tryParse(cod_snies_programa, int)
            analyze("cod_snies_programa", result, "entero", contador) 
            programa, result              = tryParse(programa, str)
            programa                      = eliminar_acentos(programa, contador)
            analyze("programa", result, "string", contador)
            id_nivel_academico, result    = tryParse(id_nivel_academico, int)
            analyze("id_nivel_academico", result, "entero", contador) 
            nivel_academico, result       = tryParse(nivel_academico, str)
            nivel_academico               = eliminar_acentos(nivel_academico, contador)
            analyze("nivel_academico", result, "string", contador)
            id_nivel_formacion, result    = tryParse(id_nivel_formacion, int)
            analyze("id_nivel_formacion", result, "entero", contador) 
            nivel_formacion, result       = tryParse(nivel_formacion, str)
            nivel_formacion               = eliminar_acentos(nivel_formacion, contador)
            analyze("nivel_formacion", result, "string", contador) 
            id_metodologia, result        = tryParse(id_metodologia, int)
            analyze("id_metodologia", result, "entero", contador)                
            metodologia, result           = tryParse(metodologia, str)
            metodologia                   = eliminar_acentos(metodologia, contador)
            analyze("metodologia", result, "string", contador)
            id_area_conocimiento, result  = tryParse(id_area_conocimiento, int)
            analyze("id_area_conocimiento", result, "entero", contador) 
            area_conocimiento, result     = tryParse(area_conocimiento, str)
            area_conocimiento             = eliminar_acentos(area_conocimiento, contador)
            analyze("area_conocimiento", result, "string", contador) 
            id_nucleo, result             = tryParse(id_nucleo, int)
            analyze("id_nucleo", result, "entero", contador)           
            nucleo, result                = tryParse(nucleo, str)
            nucleo                        = eliminar_acentos(nucleo, contador)
            analyze("nucleo", result, "string", contador)
            id_depto_programa, result     = tryParse(id_depto_programa, int)
            analyze("id_depto_programa", result, "entero", contador) 
            depto_programa, result        = tryParse(depto_programa, str)
            depto_programa                = eliminar_acentos(depto_programa, contador)
            analyze("depto_programa", result, "string", contador)
            id_municipio_programa, result = tryParse(id_municipio_programa, int)
            analyze("id_municipio_programa", result, "entero", contador) 
            municipio_programa, result    = tryParse(municipio_programa, str)
            municipio_programa            = eliminar_acentos(municipio_programa, contador)
            analyze("municipio_programa", result, "string", contador)       
            id_sexo, result               = tryParse(id_sexo, int)
            analyze("id_sexo", result, "entero", contador)
            sexo , result                 = tryParse(sexo, str)
            sexo                          = eliminar_acentos(sexo, contador)
            analyze("sexo", result, "string", contador)
            semestre, result              = tryParse(semestre, int)
            analyze("semestre", result, "entero", contador) 
            graduados , result            = tryParse(graduados, int)
            analyze("graduados", result, "entero", contador)
            año, result                   = tryParse(año, int)
            analyze("año",result,"entero",contador)
            
            #---------------------------------------------------------------------------------------------------------------
            #Reparacion de datos en la extracción - Aqui podemos reparar datos infiriendo en otros datos del mismo registro.
            #---------------------------------------------------------------------------------------------------------------
            #id_institucion
            if len(str(id_institucion)) == 0:
                if len(str(ies_padre)) > 0:             #Si el ies_padre existem se le asiga el id de la ies_padre
                    id_institucion = ies_padre
                    display_reparations(True,"id_institucion",id_institucion,contador)
                else:
                    display_reparations(False,"id_institucion",id_institucion,contador)            
            #ies_padre
            if len(str(ies_padre)) == 0:
                if len(str(id_institucion)) > 0:
                    ies_padre = id_institucion
                    display_reparations(True,"ies_padre",ies_padre,contador)
                else:
                    display_reparations(False,"ies_padre",ies_padre,contador)
        
            #nombre_ies
            if len(nombre_institucion) == 0:
                print(f"No se puede reparar nombre_institucion en la fase de extracción registro: {contador}")
                    
            #Prin_o_secc
            if len(prin_o_sec) == 0: #Se le asignará principal si está vacio. explicacion: notes, 241
                prin_o_sec = "Principal"   
                display_reparations(True,"prin_o_sec",prin_o_sec,contador)

            #id_sector_ies
            if len(str(id_sector)) == 0:
                if len(str(sector)) > 0:
                    if sector == 'OFICIAL':
                        id_sector = 1
                        display_reparations(True,"id_sector",id_sector,contador)
                    elif sector == "PRIVADA":
                        id_sector = 2       
                        display_reparations(True,"id_sector",id_sector,contador)
                else:
                    display_reparations(False,"id_sector",id_sector,contador)

            #sector_ies
            if len(sector) == 0:
                if len(id_sector) > 0:
                    if id_sector == 1:
                        sector = "OFICIAL"
                        display_reparations(True, "sector", sector, contador)
                    elif id_sector == 2:
                        display_reparations(True, "sector", sector, contador)
                    else:
                        display_reparations(False, "sector", sector, contador)

            #id_caracter
            if len(str(id_caracter)) == 0: 
                if caracter == "INSTITUCIÓN TÉCNICA PROFESIONAL":
                    id_caracter = 1
                    display_reparations(True, "id_caracter", id_caracter, contador)
                elif caracter == "INSTITUCIÓN TECNOLÓGICA":
                    id_caracter = 2
                    display_reparations(True, "id_caracter", id_caracter, contador)
                elif caracter == "INSTITUCIÓN UNIVERSITARIA/ESCUELA TECNOLÓGICA":
                    id_caracter = 3
                    display_reparations(True, "id_caracter", id_caracter, contador)

                elif caracter == "Universidad":
                    id_caracter = 4
                    display_reparations(True, "id_caracter", id_caracter, contador)
                else:
                    display_reparations(False, "id_caracter", id_caracter, contador)
                    
            
            #Caracter IES
            if len(caracter) == 0:
                if(id_caracter) == "INSTITUCIÓN TÉCNICA PROFESIONAL":
                    caracter = 1
                    display_reparations(True,"caracter", caracter, contador)
                elif(id_caracter) == "INSTITUCIÓN TECNOLÓGICA":
                    caracter = 2
                    display_reparations(True,"caracter", caracter, contador)                  
                elif(id_caracter) == "INSTITUCIÓN UNIVERSITARIA/ESCUELA TECNOLÓGICA":
                    caracter = 3
                    display_reparations(True,"caracter", caracter, contador)
                elif(id_caracter) == "UNIVERSIDAD":
                    caracter = 4
                    display_reparations(True,"caracter", caracter, contador)
                else:
                    display_reparations(False,"caracter", caracter, contador)
            
            #id_departamento la reparacion del id departamento depende de la tabla departamento, que un no ha sido cargada,
            #por lo tanto aún no se puede reparar
            
            #departamento: la reparacion del departamento depende de la tbl_departamento, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #id_municipio: la reparacion del id_municipio depende de tbl_municipio, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #municipio: la reparacion del municipio depende de tbl_municipio, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #cod_snies_programa: la reparacion del cod_snies depende de tbl_programa, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #programa académico: la reparación del programa académico depende de tbl_programa, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #id_nivel_académico
            if len(str(id_nivel_academico)) == 0:
                if len(nivel_academico) > 0:
                    if nivel_academico == "PREGRADO":
                        id_nivel_academico = 1
                        display_reparations(True, "id_nivel_academico", id_nivel_academico, contador)
                    elif nivel_academico == "POSGRADO":
                        id_nivel_academico = 2
                        display_reparations(True, "id_nivel_academico", id_nivel_academico, contador)
                    else:
                        display_reparations(False, "id_nivel_academico", id_nivel_academico, contador)
                else:
                    display_reparations(False, "id_nivel_academico", id_nivel_academico, contador)
                    
            #nivel_academico
            if len(nivel_academico) == 0:
                if len(id_nivel_academico) > 0:
                    if id_nivel_academico == 1:
                        nivel_academico = "PREGRADO"
                        display_reparations(True, "nivel_academico", nivel_academico, contador)
                    elif id_nivel_academico == 2:
                        nivel_academico = "POSGRADO"
                        display_reparations(True, "nivel_academico", nivel_academico, contador)
                    else:
                        display_reparations(False, "nivel_academico", nivel_academico, contador)
                else:
                    display_reparations(False, "nivel_academico", nivel_academico, contador)
            
            #id_nivel_formacion la reparación de id_nivel_frormacion dependen de tbl_nivel_formacion, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #nivel:formacion. la reparacion de nivel_formacion dependen de tbl_nivel_formacion que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #id_metodologia
            if len(str(id_metodologia)) == 0:
                if len(metodologia) > 0:
                    if metodologia == "Presencial":
                        id_metodologia = 1
                        display_reparations(True, "id_metodologia", id_metodologia, contador)
                    elif metodologia == "Virtual":
                        id_metodologia = 2
                        display_reparations(True, "id_metodologia", id_metodologia, contador)
                    else:
                        id_metodologia = 1
                        display_reparations(True, "id_metodologia", id_metodologia, contador)
            
            #metodologia
            if len(metodologia) == 0:
                if len(str(id_metodologia)) > 0:
                    if id_metodologia == 1:
                        metodologia = "Presencial"
                        display_reparations(True,"Metodologia", metodologia, contador)
                    elif id_metodologia == 2:
                        metodologia = "Virtual"
                        display_reparations(True,"Metodologia", metodologia, contador)
                    else: 
                        metodologia = "Presencial"
                        display_reparations(True,"Metodologia", metodologia, contador)
                else: 
                    display_reparations(False,"Metodologia", metodologia, contador)
            
            #id_area_conocimiento la reparacion del id area del conocimiento depende de tbl_area_conocimiento, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #area_conocimiento la reparacion de la area del conocimiento depende de tbl_area_conocimiento, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #id_nucleo. La reparacion de id_nucleo depende de tbl_nucleo, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #nucleo. La reparacion de nucleo depende de tbl_nucleo, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #id_departamento_programa, La reparacion de departamento del deparamento_programa depende de tbl_deparamento, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #departamento_programa, La reparacion de departamento del deparamento_programa depende de tbl_deparamento, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #id_municipio_programa, la reparacion de id_municipio_programa depende de la tabla tbl_municipio, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #municipio_programa, la reparacion de municipio_programa depende de la tabla tbl_municipio, que aun no ha sido cargada
            #por lo tanto aún no se pude reparar
            
            #id_sexo
            if len(str(id_sexo)) == 0:
                if len(sexo) > 0:
                    if sexo == "Hombre":
                        id_sexo = 1
                        display_reparations(True, "id_sexo", id_sexo, contador)
                    elif sexo == "Mujer":
                        id_sexo = 2
                        display_reparations(True, "id_sexo", id_sexo, contador)
                    else:
                        display_reparations(False, "id_sexo", id_sexo, contador)
                else:
                    display_reparations(False, "id_sexo", id_sexo, contador)
                
            #sexo
            if len(sexo) == 0:
                if len(id_sexo) > 0:
                    if id_sexo == 1:
                        sexo = "Hombre"
                        display_reparations(True, "sexo", sexo, contador)
                    elif id_sexo == 2:
                        sexo = "Mujer"
                        display_reparations(True, "sexo", sexo, contador)
                    else:
                        display_reparations(False, "sexo", sexo, contador)
                else:
                    display_reparations(False, "sexo", sexo, contador)
                    
            #Semestre:
            if len(str(semestre)) == 0:
                semestre = rand.randint(1,2)
                display_reparations(True,"Semestre",semestre, contador)
                
            #Graduados: No puede ser reparado. Razon: Notes, 338
            #Año: todos están bien
            
            #Cargar tabla temporal
            cargaTablaTemporal(connection, cursor, contador, id_institucion, ies_padre, nombre_institucion, prin_o_sec, id_sector, sector, 
                               id_caracter, caracter, id_depto_domicilio, depto_domicilio, id_municipio, municipio, cod_snies_programa, programa,
                               id_nivel_academico, nivel_academico, id_nivel_formacion, nivel_formacion, id_metodologia, metodologia, id_area_conocimiento, area_conocimiento,
                               id_nucleo, nucleo, id_depto_programa, depto_programa, id_municipio_programa, municipio_programa, id_sexo,sexo, semestre,
                               graduados, año)
            #contador
            contador+=1    
except (Exception) as error:
    print(error)





#---------------------------------------------------------------------------------------------------------------
#Procesamiento de tablas - LOAD
#---------------------------------------------------------------------------------------------------------------
try:
    #---------------------------------------------------------------------------------------------------------------
    #tbl_instituciones
    #--------------------------------------------------------------------------------------------------------------- 
    print("---------INSTITUCIONES-------------") 
    sql_command = f"SELECT distinct id_institucion, nombre_institucion FROM temporal" 
    cursor.execute(sql_command)
    registros = cursor.fetchall()
    
    for row in registros: 
        id_institucion = row[0]
        nombre_institucion = row[1]
        cargarInstituciones(connection, cursor, id_institucion, nombre_institucion)
        
    #---------------------------------------------------------------------------------------------------------------
    #tbl_departamento
    #--------------------------------------------------------------------------------------------------------------- 
    print("----------DEPARTAMENTOS-----------")
    sql_command = f"SELECT distinct id_depto_domicilio, depto_domicilio FROM temporal"
    cursor.execute(sql_command)
    registros = cursor.fetchall()
    
    for row in registros:
        id_depto_domicilio = row[0]
        depto_domicilio = row[1]
        cargarDepartamentos(connection, cursor,id_depto_domicilio, depto_domicilio)

    #---------------------------------------------------------------------------------------------------------------
    #tbl_municipio
    #--------------------------------------------------------------------------------------------------------------- 
    print("-----------MUNICIPIOS-----------")
    sql_command = f"SELECT distinct id_municipio_domicilio, municipio from temporal"
    cursor.execute(sql_command)
    registros = cursor.fetchall()
    
    for row in registros:
        id_municipio = row[0]
        municipio = row[1]
        cargarMunicipios(connection,cursor, id_municipio, municipio)
    
    
    #---------------------------------------------------------------------------------------------------------------
    #tbl_programa
    #---------------------------------------------------------------------------------------------------------------    
    print("-----------PROGRAMAS------------")
    sql_command = f"SELECT distinct cod_snies_programa, programa FROM temporal"    
    cursor.execute(sql_command)
    registros = cursor.fetchall()
    
    for row in registros:
        cod_snies_programa = row[0]
        programa = row[1]
        cargarProgramas(connection, cursor, cod_snies_programa, programa)
        
    #---------------------------------------------------------------------------------------------------------------
    #tbl_nivel_formacion
    #---------------------------------------------------------------------------------------------------------------    
    print("---------------NIVELES DE FORMACION--------------")
    sql_command = f"SELECT DISTINCT id_nivel_formacion, nivel_formacion from temporal"
    cursor.execute(sql_command)
    registros = cursor.fetchall()
    
    for row in registros:
        id_nivel_formacion = row[0]
        nivel_formacion = row[1]
        cargarNivelFormacion(connection, cursor, id_nivel_formacion, nivel_formacion)
        
        
    #---------------------------------------------------------------------------------------------------------------
    #tbl_area_conocimiento
    #--------------------------------------------------------------------------------------------------------------- 
    print("--------------AREAS DE CONOCIMIENTO----------------")          
    sql_command = f"SELECT DISTINCT id_area_conocimiento, area_conocimiento from temporal"
    cursor.execute(sql_command)
    registros = cursor.fetchall()
    
    for row in registros:
        id_area_conocimiento = row[0]
        area_conocimiento = row[1]
        cargaAreaConocimiento(connection, cursor, id_area_conocimiento, nivel_formacion)
        
        
    #---------------------------------------------------------------------------------------------------------------
    #tbl_nucleo
    #---------------------------------------------------------------------------------------------------------------
    print("----------------NUCLEOS---------------")           
    sql_command = f"SELECT DISTINCT id_nucleo, nucleo from temporal"
    cursor.execute(sql_command)
    registros = cursor.fetchall()
    
    for row in registros:
        id_nucleo = row[0]
        nucleo = row[1]
        cargarNucleos(connection,cursor, id_nucleo, nucleo)
        
        
    #---------------------------------------------------------------------------------------------------------------
    #tbl_institucion
    #--------------------------------------------------------------------------------------------------------------- 
    contador = 0       
    print("Procesando instituciones...")   
    sql_command = f"SELECT id_institucion, ies_padre, prin_o_sec, id_sector, id_caracter, id_depto_domicilio, depto_domicilio, id_municipio_domicilio, municipio, cod_snies_programa, id_nivel_academico, id_nivel_formacion, id_metodologia, id_area_conocimiento, id_nucleo, id_depto_programa,depto_programa, id_municipio_programa,municipio_programa, id_sexo, semestre, graduados, año_registro, nombre_institucion from temporal"
    cursor.execute(sql_command)
    registros = cursor.fetchall()
    
    for row in registros:
        id_institucion        = row[0]
        ies_padre             = row[1]
        prin_o_sec            = row[2]
        id_sector             = row[3]
        id_caracter           = row[4]
        id_depto_domicilio    = row[5]
        depto_domicilio       = row[6]
        id_municipio          = row[7]
        municipio             = row[8]
        cod_snies_programa    = row[9]
        id_nivel_academico    = row[10]
        id_nivel_formacion    = row[11]
        id_metodologia        = row[12] 
        id_area_conocimiento  = row[13]
        id_nucleo             = row[14]
        id_depto_programa     = row[15]
        depto_programa        = row[16]
        id_municipio_programa = row[17]
        municipio_programa    = row[18]
        id_sexo               = row[19] 
        semestre              = row[20]  
        graduados             = row[21]
        año                   = row[22]
        nombre_institucion    = row[23]
        cargarInstitucion(connection, cursor, id_institucion, ies_padre, prin_o_sec, id_sector, id_caracter, id_depto_domicilio,depto_domicilio, id_municipio,municipio, cod_snies_programa, id_nivel_academico, id_nivel_formacion, id_metodologia,
                          id_area_conocimiento, id_nucleo, id_depto_programa,depto_programa, id_municipio_programa,municipio_programa, id_sexo, semestre, graduados, año,nombre_institucion, contador)
        contador+=1
    print("----------PROCESO ETL FINALIZADO CORRECTAMENTE---------")

except (Exception) as error:
    print("Error ---------> ",error)
finally:
    if(connection):
        #---------------------------------------------------------------------------------------------------------------
        #Finalmente borramos la tabla temporal para que no ocupe espacio adicional
        #---------------------------------------------------------------------------------------------------------------
        command = '''DELETE FROM temporal'''
        cursor.execute(command)
        
        connection.close()
        print(f"Conexion con la base de datos {v_database} cerrada.")
        


#---------------------------------------------------------------------------------------------------------------
#Creacion del archivo txt de reparados
#---------------------------------------------------------------------------------------------------------------
with open("reparados.txt", "w") as reparados:
    reparados.write("") #Borramos el contenido que haya en el archivo de texto
    reparados.write(f"|---------------BIENVENIDO AL ARCHIVO DE REPARADOS.---------------|\n|             Autor: Hamilton Dario             |\n|_________________________________________________________________|\n\n|--------------------------Reparaciones---------------------------|\n|		Datos Reparados	               :{contador_reparaciones}\n|		Datos Actualizados	       :{contador_actualizaciones}\n|		Datos acentuaciones removidas  :{contador_acentuaciones}\n|		Datos Creados desde el servidor:{contador_creaciones}\n|		Datos Reparados con ETL	       :{reparacionesETL}\n|		Datos Reparados con ELT        :{reparacionesELT}\n|-----------------------------------------------------------------|\n|		Total datos afectados	       :{contador_reparaciones+contador_creaciones+contador_acentuaciones+contador_actualizaciones}\n|----------------------------DETALLES-----------------------------|\n")
    for reparado in detalle_reparaciones:
        reparados.write(reparado + "\n")
    reparados.write(f"\nFin del proceso de extracción")
    
#calcular el tiempo
tiempo_final = time.time()
tiempo_total_transcurrido = tiempo_final - tiempo_inicial
minutos = int(tiempo_total_transcurrido // 60)
segundos = int(tiempo_total_transcurrido % 60)

print(f"Fin del proceso ETL.\nTiempo de ejecucion: {minutos}:{segundos} (min:seg).\nSe ha creado un archivo de texto con los archivos reparados en {directorio_actual} con el nombre de: reparados.txt")