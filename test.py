id_metodologia = 2

if id_metodologia != 1 and id_metodologia !=2:
    print("No deberia",id_metodologia)
    



# def eliminar_acentos(palabra):
#     acentos = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u"}
#     palabra_nueva = ""
#     for letra in palabra:
#         if letra in acentos:
#             palabra_nueva += acentos[letra]
#         else:
#             palabra_nueva+=letra
#     return palabra_nueva

# pal = "psicologia"
# pal = eliminar_acentos(pal)
# print(pal)


# def tryParse(dato, tipo_dato):
#     try:
#         return tipo_dato(dato), True
#     except (ValueError, TypeError) as error:
#         return dato, False

# num = "1101"
# num, result = tryParse(num,int)

# print(result)

# def analyze(name, result, dataType, contador):
#     if not result:
#         print(f"No se pudo transformar {name} a {dataType}. Registro # {contador}")
        
        
##################################################
#-----------------Reparación de datos----------------
            #id_institucion
            # if len(id_institucion) == 0:
            #     if len(ies_padre) > 0:             #Si el ies_padre existem se le asiga el id de la ies_padre
            #         id_institucion = ies_padre
            #         print(f"REPARACIÓN: id de la institucion reaparada a base de la ies_padre. Registro: {contador}")
            #     elif len(nombre_institucion) > 0:  #Si no existe, se busca en la base de datos el id del nombre de la institucion
            #         sql_command = f"select id_institucion from tbl_instituciones where nombre_institucion = '{nombre_institucion}'"
            #         cursor.execute(sql_command)
            #         registros = cursor.fetchall()
            #         id_institucion = registros[0]
            #         print(f"REPARACION: id de la institucion reparado a base del nombre de la institucion. Registro: {contador}")
            #     else:
            #         print(f"No se pudo reparar id_institucion del registro: {contador}")
            
            # #ies_padre
            # if len(ies_padre) == 0:
            #     if len(id_institucion) > 0:
            #         ies_padre = id_institucion
            #         print(f"REPARACIÓN: ies_padre reaparada a base de id_institucion. Registro: {contador}")
            #     elif len(nombre_institucion) > 0:
            #         sql_command = f"select id_institucion from tbl_instituciones where nombre_institucion = '{nombre_institucion}'"
            #         cursor.execute(sql_command)
            #         registros = cursor.fetchall()
            #         ies_padre = registros[0]
            #         print(f"REPARACION: ies_reparada a base del nombre de la institucion. Registro: {contador}")
            #     else:
            #         print(f"No se pudo reparar ies_padre del registro: {contador}")
                    
            # #nombre_ies
            # if len(nombre_institucion) == 0:
            #     if len(id_institucion) > 0:
            #         sql_command = f"SELECT nombre_institucion from tbl_instituciones where id_institucion = {id_institucion}"
            #         cursor.execute(sql_command)
            #         registros = cursor.fetchall()
            #         nombre_institucion = registros[0]
            #         print(f"REPARACION: nombre_institucion reparado a base del id_institucion. Registro: {contador}")
            #     elif len(ies_padre) > 0:
            #         sql_command = f"SELECT nombre_institucion from tbl_instituciones where id_institucion = {ies_padre}"
            #         cursor.execute(sql_command)
            #         registros = cursor.fetchall()
            #         nombre_institucion = registros[0]
            #         print(f"REPARACION: nombre_institucion reparado a partir de la ies_padre. Registro: {contador}")
                    
            # #Prin_o_secc
            # if len(prin_o_sec) == 0: #Se le asignará principal si está vacio. explicacion: notes, 241
            #     prin_o_sec = "Principal"       
            
            # #id_sector_ies
            # if len(id_sector) == 0:
            #     if len(sector) > 0:
            #         id_sector = 1 if sector == "Privado" else 2