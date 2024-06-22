
-------------------------------------------
--Creación de tablas
-------------------------------------------

--tbl_instituciones
CREATE TABLE tbl_instituciones(
    id_institucion INT NOT NULL PRIMARY KEY,
    nombre_institucion varchar (100) not null
)

--tbl_principal_seccional
CREATE TABLE tbl_principal_seccional(
    id_prin_sec INT NOT NULL PRIMARY KEY,
    desc_prin_sec VARCHAR(40) NOT NULL,
    CONSTRAINT chk_principalOseccional CHECK (desc_prin_sec IN ('Principal','Seccional'))
)
---UML---
INSERT INTO tbl_principal_seccional (id_prin_sec, desc_prin_sec)  VALUES(1,'Principal')
INSERT INTO tbl_principal_seccional (id_prin_sec, desc_prin_sec) VALUES(2,'Seccional')
select * from tbl_principal_seccional

--sector
CREATE TABLE tbl_sector(
    id_sector INT NOT NULL PRIMARY KEY,
    desc_sector VARCHAR(40) NOT NULL
    CONSTRAINT chk_desc_sector CHECK (desc_sector IN ('Oficial','Privada'))
)
--UML--
INSERT INTO tbl_sector (id_sector, desc_sector) VALUES(1,'Oficial')
INSERT INTO tbl_sector (id_sector, desc_sector) VALUES(2,'Privada')
select * from tbl_sector

--caracter
CREATE TABLE tbl_caracter(
    id_caracter INT NOT NULL PRIMARY KEY,
    desc_caracter VARCHAR(60) NOT NULL
    CONSTRAINT chk_desc_caracter CHECK (desc_caracter IN('Institución técnica profesional','Institución Tecnológica','Institución universitaria/Escuela tecnológica','Universidad'))
)
--UML
INSERT INTO tbl_caracter (id_caracter, desc_caracter) VALUES(1,'Institución técnica profesional')
INSERT INTO tbl_caracter (id_caracter, desc_caracter) VALUES(2,'Institución Tecnológica')
INSERT INTO tbl_caracter (id_caracter, desc_caracter) VALUES(3,'Institución universitaria/Escuela tecnológica')
INSERT INTO tbl_caracter (id_caracter, desc_caracter) VALUES(4,'Universidad')
select * from tbl_caracter

--drop table [IF EXISTS] tbl_caracter 
--departamento
CREATE TABLE tbl_departamento(
    id_departamento INT NOT NULL PRIMARY KEY,
    nombre_departamento VARCHAR(100) NOT NULL
)
drop table tbl_departamento
--Municipio
CREATE TABLE tbl_municipio(
    id_municipio INT NOT NULL PRIMARY KEY,
    nombre_municipio VARCHAR(40) NOT NULL
)
--programa
CREATE TABLE tbl_programa(
    cod_snies INT NOT NULL PRIMARY KEY,
    nombre_programa VARCHAR(180) NOT NULL
)
drop table tbl_programa
--Nivel academico
CREATE TABLE tbl_nivel_academico(
    id_nivel_academico INT NOT NULL PRIMARY KEY,
    desc_nivel_academico VARCHAR(10) NOT NULL
    CONSTRAINT chk_desc_nivel_academico CHECK (desc_nivel_academico in ('Posgrado','Pregrado'))
)
--UML
INSERT INTO tbl_nivel_academico (id_nivel_academico,desc_nivel_academico) VALUES (1,'Pregrado')
INSERT INTO tbl_nivel_academico (id_nivel_academico,desc_nivel_academico) VALUES (2,'Posgrado')
select * from tbl_nivel_academico

--Nivel formacion
CREATE TABLE tbl_nivel_formacion(
    id_nivel_formacion INT NOT NULL PRIMARY KEY,
    desc_nivel_formacion VARCHAR(40) NOT NULL
)
--Metodologia
CREATE TABLE tbl_metodologia(
    id_metodologia INT NOT NULL PRIMARY KEY,
    desc_metodologia VARCHAR(15) NOT NULL
    CONSTRAINT chk_desc_metodologia CHECK (desc_metodologia IN ('Presencial','Virtual'))
)
---UML
insert into tbl_metodologia VALUES(1,'Presencial')
insert into tbl_metodologia VALUES(2,'Virtual')
select * from tbl_metodologia

--Area conocimiento
CREATE TABLE tbl_area_conocimiento(
    id_area_conocimiento INT NOT NULL PRIMARY KEY,
    desc_area_conocimiento VARCHAR(40) NOT NULL
)

--AÑO
CREATE TABLE tbl_año(
    id_año INT NOT NULL PRIMARY KEY,
    desc_año INT NOT NULL    
)
---UML--
INSERT INTO tbl_año (id_año, desc_año) VALUES(1,2018)
INSERT INTO tbl_año (id_año, desc_año) VALUES(2,2019)
INSERT INTO tbl_año (id_año, desc_año) VALUES(3,2020)
INSERT INTO tbl_año (id_año, desc_año) VALUES(4,2021)
select * from tbl_año where desc_año = 2020

--sexo
CREATE TABLE tbl_sexo(
    id_sexo INT NOT NULL PRIMARY KEY,
    desc_sexo VARCHAR(10) NOT NULL
    CONSTRAINT chk_sexo CHECK(desc_sexo IN ('Hombre','Mujer'))
)
---UML---
INSERT INTO tbl_sexo (id_sexo, desc_sexo) VALUES(1,'Hombre')
INSERT INTO tbl_sexo (id_sexo, desc_sexo) VALUES(2,'Mujer')
select * from tbl_sexo

--nucleo
CREATE TABLE tbl_nucleo(
    id_nucleo INT NOT NULL PRIMARY KEY,
    desc_nucleo VARCHAR(150) NOT NULL
)
drop table tbl_nucleo

--institucion
CREATE TABLE tbl_institucion(
    id_institucion INT NOT NULL,
    ies_padre INT NOT NULL,
    prin_o_sec INT NOT NULL,
    id_sector INT NOT NULL,
    id_caracter INT NOT NULL,
    id_depto_domicilio INT NOT NULL,
    id_municipio_domicilio INT NOT NULL,
	cod_snies_programa INT NOT NULL,
    id_nivel_academico INT NOT NULL,
    id_nivel_formacion INT NOT NULL,
    id_metodologia INT NOT NULL,
    id_area_conocimiento INT NOT NULL,
    id_nucleo INT NOT NULL,
    id_depto_programa INT NOT NULL,
    id_municipio_programa INT NOT NULL,
	id_sexo INT NOT NULL,
    semestre INT NOT NULL,
    graduados INT NOT NULL,
    año_registro INT NOT NULL,

    -- FOREIGN KEYS
    CONSTRAINT FK_id_institucion FOREIGN KEY(id_institucion) REFERENCES tbl_instituciones(id_institucion) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_ies_padre FOREIGN KEY(ies_padre) REFERENCES tbl_instituciones(id_institucion)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_prin_o_sec FOREIGN KEY (prin_o_sec) REFERENCES tbl_principal_seccional(id_prin_sec)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_id_sector FOREIGN KEY (id_sector) REFERENCES tbl_sector(id_sector)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_id_caracter FOREIGN KEY (id_caracter) REFERENCES tbl_caracter(id_caracter)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_id_depto_domicilio FOREIGN KEY (id_depto_domicilio) REFERENCES tbl_departamento(id_departamento)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_id_municipio_domicilio FOREIGN KEY (id_municipio_domicilio) REFERENCES tbl_municipio(id_municipio)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_cod_snies_programa FOREIGN KEY (cod_snies_programa) REFERENCES tbl_programa(cod_snies)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_id_nivel_academico FOREIGN KEY (id_nivel_academico) REFERENCES tbl_nivel_academico(id_nivel_academico)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_id_nivel_formacion FOREIGN KEY (id_nivel_formacion) REFERENCES tbl_nivel_formacion(id_nivel_formacion)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_id_metodologia FOREIGN KEY (id_metodologia) REFERENCES tbl_metodologia(id_metodologia)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_id_area_conocimiento FOREIGN KEY (id_area_conocimiento) REFERENCES tbl_area_conocimiento(id_area_conocimiento)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_id_nucleo FOREIGN KEY (id_nucleo) REFERENCES tbl_nucleo(id_nucleo)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_id_depto_programa FOREIGN KEY (id_depto_programa) REFERENCES tbl_departamento(id_departamento)ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_id_municipio_programa FOREIGN KEY (id_municipio_programa) REFERENCES tbl_municipio(id_municipio)ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT FK_id_sexo FOREIGN KEY (id_sexo) REFERENCES tbl_sexo(id_sexo) ON DELETE CASCADE ON UPDATE CASCADE
);

DROP TABLE tbl_institucion
DROP TABLE temporal

CREATE TABLE temporal(
	id_institucion INT NOT NULL,
    ies_padre INT NOT NULL,
	nombre_institucion VARCHAR(100) NOT NULL,
    prin_o_sec VARCHAR(100) NOT NULL,
	id_sector INT NOT NULL,
	sector VARCHAR(100) NOT NULL,
    id_caracter INT NOT NULL,
	caracter VARCHAR(100) NOT NULL,
    id_depto_domicilio INT NOT NULL,
	depto_domicilio VARCHAR(100) NOT NULL,
    id_municipio_domicilio INT NOT NULL,
	municipio VARCHAR(100) NOT NULL,
	cod_snies_programa INT NOT NULL,
	programa VARCHAR(180) NOT NULL,
    id_nivel_academico INT NOT NULL,
	nivel_academico VARCHAR(100) NOT NULL,
    id_nivel_formacion INT NOT NULL,
	nivel_formacion VARCHAR(100) NOT NULL,
    id_metodologia INT NOT NULL,
	metodologia VARCHAR(100) NOT NULL,
    id_area_conocimiento INT NOT NULL,
	area_conocimiento VARCHAR(100) NOT NULL,
    id_nucleo INT NOT NULL,
	nucleo VARCHAR(100) NOT NULL,
    id_depto_programa INT NOT NULL,
	depto_programa VARCHAR(100) NOT NULL,
    id_municipio_programa INT NOT NULL,
	municipio_programa VARCHAR(100) NOT NULL,
	id_sexo INT NOT NULL,
	sexo VARCHAR(100) NOT NULL,
    semestre INT NOT NULL,
    graduados INT NOT NULL,
    año_registro INT NOT NULL
)
			
select * from temporal



