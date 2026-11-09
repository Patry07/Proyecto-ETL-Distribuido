#!/usr/bin/env python
# coding: utf-8

# # 02 — ETL paralelo de datos abiertos de COVID-19 e influenza (México)
# 
# **Responsable:** Integrante 2 — procesamiento paralelo y Docker.
# 
# ## Objetivo
# 
# Implementar un ETL paralelo utilizando Python y ProcessPoolExecutor.
# Se aplicarán las mismas reglas de limpieza que en la versión secuencial
# para comparar correctamente los tiempos de ejecución.

# ### 1. Importar librerías

# In[2]:

import os
import json
import numpy as np
import pandas as pd

from pathlib import Path
from time import perf_counter
from concurrent.futures import ProcessPoolExecutor


# Rutas del proyecto
RUTA_CSV = Path("datos/COVID19MEXICO.csv")
CARPETA_RESULTADOS = Path("resultados")

COLUMNAS_FECHA = [
    "FECHA_ACTUALIZACION",
    "FECHA_INGRESO",
    "FECHA_SINTOMAS",
    "FECHA_DEF",
]


# Transformar un bloque de datos
def transformar_bloque(tarea):
    bloque, columnas_fecha = tarea
    datos = bloque.copy()

    # Convertir fechas
    for columna in columnas_fecha:
        if columna in datos.columns:
            fechas = datos[columna].astype("string").str.strip()
            fechas = fechas.replace({
                "9999-99-99": pd.NA,
                "": pd.NA
            })

            datos[columna] = pd.to_datetime(
                fechas,
                errors="coerce"
            )

    # Validar edades
    edades_invalidas = 0

    if "EDAD" in datos.columns:
        datos["EDAD"] = pd.to_numeric(
            datos["EDAD"],
            errors="coerce"
        )

        mascara = (
            datos["EDAD"].notna()
            & ~datos["EDAD"].between(0, 120)
        )

        edades_invalidas = int(mascara.sum())
        datos.loc[mascara, "EDAD"] = np.nan

    # Limpiar espacios en textos
    columnas_texto = datos.select_dtypes(
        include=["object", "string"]
    ).columns

    for columna in columnas_texto:
        datos[columna] = (
            datos[columna].astype("string").str.strip()
        )

    return datos, edades_invalidas


# Ejecutar ETL paralelo
def ejecutar_etl_paralelo(df, numero_procesos=4):

    inicio = perf_counter()
    datos = df.copy()

    # Normalizar encabezados
    datos.columns = datos.columns.str.strip().str.upper()

    # Eliminar duplicados exactos
    duplicados_exactos = int(datos.duplicated().sum())
    datos = datos.drop_duplicates()

    # Eliminar ID_REGISTRO repetidos
    duplicados_id = 0

    if "ID_REGISTRO" in datos.columns:
        duplicados_id = int(
            datos["ID_REGISTRO"].duplicated().sum()
        )

        datos = datos.drop_duplicates(
            subset=["ID_REGISTRO"],
            keep="first"
        )

    # Dividir los datos en bloques
    numero_procesos = max(1, numero_procesos)

    indices = np.array_split(
        np.arange(len(datos)),
        numero_procesos
    )

    bloques = [
        datos.iloc[indice].copy()
        for indice in indices
        if len(indice) > 0
    ]

    tareas = [
        (bloque, COLUMNAS_FECHA)
        for bloque in bloques
    ]

    # Procesamiento paralelo
    with ProcessPoolExecutor(
        max_workers=numero_procesos
    ) as executor:

        resultados = list(
            executor.map(transformar_bloque, tareas)
        )

    # Unir resultados
    df_resultado = pd.concat(
        [resultado[0] for resultado in resultados],
        ignore_index=True
    )

    edades_invalidas = sum(
        resultado[1] for resultado in resultados
    )

    tiempo = perf_counter() - inicio

    auditoria = {
        "filas_entrada": len(df),
        "duplicados_exactos_eliminados": duplicados_exactos,
        "duplicados_id_eliminados": duplicados_id,
        "edades_invalidas_convertidas_a_nulo": edades_invalidas,
        "filas_salida": len(df_resultado),
        "procesos_utilizados": len(bloques),
        "tiempo_segundos": tiempo
    }

    return df_resultado, auditoria


# Programa principal
def main():

    numero_procesos = 4

    print("Iniciando ETL paralelo...")
    print("CPU disponibles:", os.cpu_count())

    # Comprobar que existe el archivo de entrada
    if not RUTA_CSV.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo: {RUTA_CSV}"
        )

    # Cargar datos
    print("Cargando archivo CSV...")

    df_original = pd.read_csv(
        RUTA_CSV,
        encoding="utf-8-sig",
        low_memory=False
    )

    print("Filas originales:", len(df_original))

    # Ejecutar procesamiento
    print(f"Procesando con {numero_procesos} procesos...")

    df_resultado, auditoria = ejecutar_etl_paralelo(
        df_original,
        numero_procesos=numero_procesos
    )

    # Crear carpeta de resultados
    CARPETA_RESULTADOS.mkdir(
        parents=True,
        exist_ok=True
    )

    # Guardar CSV procesado
    ruta_salida = (
        CARPETA_RESULTADOS / "COVID19MEXICO_limpio.csv"
    )

    df_resultado.to_csv(
        ruta_salida,
        index=False,
        encoding="utf-8-sig"
    )

    # Guardar auditoría
    ruta_auditoria = CARPETA_RESULTADOS / "auditoria.json"

    with open(ruta_auditoria, "w", encoding="utf-8") as archivo:
        json.dump(
            auditoria,
            archivo,
            indent=4,
            ensure_ascii=False
        )

    print("\nETL terminado correctamente.")
    print("Filas procesadas:", len(df_resultado))
    print("Archivo generado:", ruta_salida)
    print("Auditoría generada:", ruta_auditoria)
    print("Tiempo:", round(auditoria["tiempo_segundos"], 4), "segundos")


if __name__ == "__main__":
    main()
