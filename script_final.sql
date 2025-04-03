----------------------------------------------------------------------------------------------
-----------Etapa 2: Diseño e implementación del modelo de datos – 20%		CREACION ESQUEMA INICIAL		-----------------
------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS inicial

CREATE TABLE IF NOT EXISTS inicial.datos (
  "fecha" VARCHAR(50),
  "serie_hidrologica" VARCHAR(50),
  "fuente_hidrologica" VARCHAR(50),
  "aporte_hidrico" VARCHAR(50)
);
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS corregido;

CREATE TABLE IF NOT EXISTS corregido.regiones (
  "id" SERIAL PRIMARY KEY,
  "nombre" VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS corregido.fuentes_hidricas (
  "id" SERIAL PRIMARY KEY,
  "nombre" VARCHAR(4) NOT NULL
);

CREATE TABLE IF NOT EXISTS corregido.embalses (
  "id" SERIAL PRIMARY KEY,
  "nombre" VARCHAR(4) NOT NULL,
  "id_fuente" INT,
  "id_region" INT,
  FOREIGN KEY (id_fuente) REFERENCES corregido.fuentes_hidricas ("id"),
  FOREIGN KEY (id_region) REFERENCES corregido.regiones ("id")
);

CREATE TABLE IF NOT EXISTS corregido.aportes_hidricos (
  "id" SERIAL PRIMARY KEY,
  "fecha" DATE,
  "id_embalse" INT,
  "aporte_hidrico" FLOAT,
  FOREIGN KEY (id_embalse) REFERENCES corregido.embalses ("id")
);
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------

INSERT INTO corregido.regiones(nombre)
SELECT DISTINCT cast(fuente_hidrologica as varchar(20))
FROM inicial.datos;

INSERT INTO corregido.fuentes_hidricas(nombre)
SELECT DISTINCT substring(serie_hidrologica,5,8)
FROM inicial.datos;

INSERT INTO corregido.embalses(nombre, id_fuente, id_region)
SELECT DISTINCT substring(serie_hidrologica,1,4), fh.id, r.id
FROM inicial.datos
JOIN corregido.fuentes_hidricas fh ON fh.nombre = substring(inicial.datos.serie_hidrologica,5,8)
JOIN corregido.regiones r ON r.nombre = inicial.datos.fuente_hidrologica;

INSERT INTO corregido.aportes_hidricos(fecha, aporte_hidrico, id_embalse)
SELECT cast(inicial.datos.fecha as date), cast(inicial.datos.aporte_hidrico as FLOAT), e.id
FROM inicial.datos
JOIN corregido.embalses e ON e.nombre = substring(inicial.datos.serie_hidrologica,1,4);
----------------------------------------------------------------------------------------------
---------------------------Etapa 3: Diagnóstico de completitud de datos – 30%-------------------------------------------------------------------
----------------------------------------------------------------------------------------------

WITH dias_por_anio AS (
    SELECT 2023 AS anio, 365 AS total_dias UNION ALL
    SELECT 2024 AS anio, 366 AS total_dias
),
conteo_aportes AS (
    SELECT 
        e.id AS id_embalse,
        e.id_region,
        EXTRACT(YEAR FROM ah.fecha) AS anio,
        COUNT(ah.id) AS dias_con_aporte
    FROM corregido.aportes_hidricos ah
    JOIN corregido.embalses e ON ah.id_embalse = e.id
    GROUP BY e.id, e.id_region, anio
)
SELECT 
    r.nombre AS region,
    e.nombre AS embalse,
    c.anio,
    c.dias_con_aporte,
    d.total_dias,
    ROUND((c.dias_con_aporte::NUMERIC / d.total_dias) * 100, 2) AS porcentaje_completitud
FROM conteo_aportes c
JOIN dias_por_anio d ON c.anio = d.anio
JOIN corregido.embalses e ON c.id_embalse = e.id
JOIN corregido.regiones r ON e.id_region = r.id
ORDER BY r.nombre, e.nombre, c.anio;

----------------------------------------------------------------------------------------------

EXPLAIN ANALYZE 
WITH dias_por_anio AS (
    SELECT 2023 AS anio, 365 AS total_dias UNION ALL
    SELECT 2024 AS anio, 366 AS total_dias
),
conteo_aportes AS (
    SELECT 
        e.id AS id_embalse,
        e.id_region,
        EXTRACT(YEAR FROM ah.fecha) AS anio,
        COUNT(ah.id) AS dias_con_aporte
    FROM corregido.aportes_hidricos ah
    JOIN corregido.embalses e ON ah.id_embalse = e.id
    GROUP BY e.id, e.id_region, anio
)
SELECT 
    r.nombre AS region,
    e.nombre AS embalse,
    c.anio,
    c.dias_con_aporte,
    d.total_dias,
    ROUND((c.dias_con_aporte::NUMERIC / d.total_dias) * 100, 2) AS porcentaje_completitud
FROM conteo_aportes c
JOIN dias_por_anio d ON c.anio = d.anio
JOIN corregido.embalses e ON c.id_embalse = e.id
JOIN corregido.regiones r ON e.id_region = r.id
ORDER BY r.nombre, e.nombre, c.anio;

----------------------------------------------------------------------------------------------
---------------------------Etapa 4: Diagnóstico de niveles mínimos de aporte hídrico – 30%-------------------------------------------------------------------
----------------------------------------------------------------------------------------------

WITH aportes_2024 AS (
    SELECT 
        ah.id_embalse,
        ah.aporte_hidrico
    FROM corregido.aportes_hidricos ah
    WHERE EXTRACT(YEAR FROM ah.fecha) = 2024
),
valores_extremos AS (
    SELECT 
        id_embalse,
        MIN(aporte_hidrico) AS valor_minimo,
        MAX(aporte_hidrico) AS valor_maximo
    FROM aportes_2024
    GROUP BY id_embalse
)
SELECT 
    e.nombre AS embalse,
    v.valor_maximo,
    v.valor_minimo,
    CASE 
        WHEN v.valor_maximo = 0 THEN NULL -- Para evitar división por cero
        ELSE ((v.valor_maximo - v.valor_minimo) / v.valor_maximo) * 100
    END AS porcentaje_reduccion
FROM valores_extremos v
JOIN corregido.embalses e ON v.id_embalse = e.id
ORDER BY porcentaje_reduccion DESC;
-----------------------------------------------------

EXPLAIN ANALYZE
WITH aportes_2024 AS (
    SELECT 
        ah.id_embalse,
        ah.aporte_hidrico
    FROM corregido.aportes_hidricos ah
    WHERE EXTRACT(YEAR FROM ah.fecha) = 2024
),
valores_extremos AS (
    SELECT 
        id_embalse,
        MIN(aporte_hidrico) AS valor_minimo,
        MAX(aporte_hidrico) AS valor_maximo
    FROM aportes_2024
    GROUP BY id_embalse
)
SELECT 
    e.nombre AS embalse,
    v.valor_maximo,
    v.valor_minimo,
    CASE 
        WHEN v.valor_maximo = 0 THEN NULL -- Para evitar división por cero
        ELSE ((v.valor_maximo - v.valor_minimo) / v.valor_maximo) * 100
    END AS porcentaje_reduccion
FROM valores_extremos v
JOIN corregido.embalses e ON v.id_embalse = e.id
ORDER BY porcentaje_reduccion DESC;

----------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_regiones_nombre ON corregido.regiones(nombre);
CREATE INDEX IF NOT EXISTS idx_regiones_id ON corregido.regiones(id);
CREATE INDEX IF NOT EXISTS idx_embalses_nombre ON corregido.embalses(nombre);
CREATE INDEX IF NOT EXISTS idx_embalses_id ON corregido.embalses(id);
CREATE INDEX IF NOT EXISTS idx_fuentes_nombre ON corregido.fuentes_hidricas(nombre);
CREATE INDEX IF NOT EXISTS idx_fuentes_id ON corregido.fuentes_hidricas(id);
CREATE INDEX IF NOT EXISTS idx_aportes_id ON corregido.aportes_hidricos(id);
CREATE INDEX IF NOT EXISTS idx_aportes_fecha ON corregido.aportes_hidricos(fecha);

DROP TABLE IF EXISTS corregido.aportes_hidricos;
DROP TABLE IF EXISTS corregido.embalses;
DROP TABLE IF EXISTS corregido.fuentes_hidricas;
DROP TABLE IF EXISTS corregido.regiones;
DROP TABLE IF EXISTS inicial.datos;