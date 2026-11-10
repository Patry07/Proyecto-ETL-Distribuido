"""Genera, ejecuta con kernel Jupyter y exporta los cuatro avances integrados."""
from pathlib import Path
import argparse
import nbformat as nbf
from nbclient import NotebookClient
from nbconvert import HTMLExporter
from comun import ROOT

def md(text):
    return nbf.v4.new_markdown_cell(text)

def code(text):
    return nbf.v4.new_code_cell(text)

COMMON = '''from pathlib import Path
import sys, json
import pandas as pd
from PIL import Image

ROOT = Path.cwd()
if not (ROOT / "cpp/etl.cpp").exists():
    raise RuntimeError("Abrir y ejecutar el notebook desde la carpeta del proyecto")
sys.path.insert(0, str(ROOT / "scripts"))
OUT = ROOT / "resultados_experimentos"
entorno = json.loads((OUT / "entorno.json").read_text())
mediciones = pd.read_csv(OUT / "mediciones.csv")
resumen = pd.read_csv(OUT / "resumen_rendimiento.csv")
pd.set_option("display.max_columns", 20)
pd.set_option("display.float_format", lambda x: f"{x:.6f}")
print("Dataset SHA-256:", entorno["dataset_sha256"])
print("Compilación:", entorno["compilacion"])
print("Kubernetes:", entorno["kubernetes"])
'''

REFERENCES = '''## Fuentes técnicas

- [OpenMP: distribución de iteraciones](https://www.openmp.org/spec-html/5.2/openmpse66.html).
- [GCC: compilación con `-fopenmp`](https://gcc.gnu.org/projects/gomp/).
- [Kubernetes: Jobs paralelos e indexados](https://kubernetes.io/docs/concepts/workloads/controllers/job/).
- [Cornell: Ley de Amdahl](https://cvw.cac.cornell.edu/parallel/efficiency/amdahls-law).

Los resultados numéricos proceden exclusivamente de los CSV, auditorías y registros de esta entrega.
'''

def notebooks():
    first = [
        md('''# 01 · Datos y ETL secuencial en C++

**Proyecto:** ETL distribuido de COVID-19 e influenza, México.  
**Objetivo del curso:** programación paralela en C/C++.  
**Integración:** conserva los avances originales y añade un motor común C++17.

El CSV se extrae y explora; el ejecutable C++ lee, infiere columnas numéricas, limpia y exporta. Python se usa para exploración, validación y análisis. Los notebooks Python originales se conservan como referencia de las reglas, sin confundirlos con la implementación C++.
'''),
        md('''## 1. Entorno y archivo de entrada

La entrada real es `datos/COVID19MEXICO.csv`. El hash identifica exactamente el archivo evaluado. Todos los escenarios usan el mismo ejecutable C++ compilado en la imagen OpenMP existente, con optimización `-O3` y soporte OpenMP.
'''), code(COMMON),
        code('''original = pd.read_csv(ROOT / "datos/COVID19MEXICO.csv", encoding="utf-8-sig", low_memory=False)
print(f"Entrada: {len(original):,} filas × {len(original.columns)} columnas")
display(original.head(3))
display(pd.DataFrame({"tipo": original.dtypes.astype(str), "nulos": original.isna().sum()}))
print("Duplicados exactos:", int(original.duplicated().sum()))
print("ID repetidos:", int(original.ID_REGISTRO.duplicated().sum()))'''),
        code('''import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
fig, axes = plt.subplots(1, 2, figsize=(11, 3.5))
original.EDAD.hist(bins=30, ax=axes[0], color="#16859b")
axes[0].set(title="Distribución de edad", xlabel="Edad", ylabel="Registros")
original.CLASIFICACION_FINAL_COVID.value_counts().sort_index().plot.bar(ax=axes[1], color="#334155")
axes[1].set(title="Clasificación COVID: códigos originales", xlabel="Código", ylabel="Registros")
fig.tight_layout()
fig.savefig(OUT / "exploracion_datos.png", dpi=160)
plt.close(fig)
display(Image.open(OUT / "exploracion_datos.png"))'''),
        md('''## 2. Reglas conservadas de la versión original

1. Normalizar encabezados y conservar el esquema.
2. Eliminar duplicados exactos y después identificadores repetidos, conservando la primera aparición.
3. Convertir fechas ISO válidas; el marcador `9999-99-99` y las fechas inválidas quedan vacíos.
4. Convertir `EDAD` a número y marcar como faltantes los valores fuera de 0–120, sin eliminar filas.
5. Recortar espacios de los textos y exportar CSV UTF-8.

El lector C++ admite BOM, CRLF, comillas escapadas y campos multilínea. La inferencia numérica previa conserva el comportamiento de pandas para el esquema real (por ejemplo, `015` en una columna numérica se representa como `15`). El validador de fechas está definido para el formato ISO de esta fuente; no pretende admitir cualquier formato de fecha externo.

Los códigos clínicos se conservan sin asignar interpretaciones que no estén documentadas en su catálogo.
'''),
        code('''print((ROOT / "Makefile").read_text())
print("Comando de referencia:")
print("bin/etl --input datos/COVID19MEXICO.csv --output resultados/limpio.csv --audit resultados/auditoria.json --mode seq")
archivo = OUT / "ejecuciones/cpp_secuencial_05/auditoria.json"
display(pd.DataFrame([json.loads(archivo.read_text())]))'''),
        md('''## 3. Validación y productos

Se ejecutó la función original del notebook 01 para obtener una referencia independiente. Se compararon valores, columnas, orden y nulos; al recargar CSV se admiten tipos numéricos equivalentes. Además, los hashes de todas las salidas C++ deben coincidir exactamente entre sí.

Las pruebas sintéticas cubren duplicados entre particiones, fechas no válidas, edades fuera de rango, campos con comillas/saltos de línea y errores de entrada.
'''),
        code('''validacion = json.loads((OUT / "ejecuciones/cpp_secuencial_05/validacion.json").read_text())
display(pd.DataFrame([validacion]))
assert mediciones.validado.all()
assert mediciones.salida_sha256.nunique() == 1
print("Las 40 ejecuciones medidas coinciden con la referencia y entre sí.")
for ruta in sorted((OUT / "resumenes").glob("*.csv")):
    print(ruta.name)
    display(pd.read_csv(ruta))'''),
        md('''## Resultado del avance 1

El ETL secuencial C++ constituye la línea base de la evaluación. Los resúmenes descriptivos se generan desde su conjunto limpio y se guardan junto con las auditorías. Los tiempos de lectura, transformación y escritura quedan separados para la parte 4.
'''), md(REFERENCES)]

    second = [
        md('''# 02 · Paralelismo C++ / OpenMP y Docker Compose

**Objetivo:** ejecutar el mismo ETL en paralelo dentro de un contenedor y repartir particiones entre dos contenedores. El código de procesamiento y el reductor son C++.
'''), code(COMMON),
        md('''## 1. Memoria compartida con OpenMP

La preparación global resuelve duplicados. Después, `omp for schedule(static)` reparte las filas entre hilos. Cada hilo modifica filas distintas y la suma de edades inválidas usa `reduction`, por lo que no hay un contador compartido sin protección. El programa registra el tamaño real del equipo OpenMP.
'''),
        code('''fuente = (ROOT / "cpp/etl.cpp").read_text().splitlines()
inicio = next(i for i, linea in enumerate(fuente) if "#pragma omp parallel" in linea)
print("\\n".join(fuente[inicio-3:inicio+12]))
display(resumen[resumen.configuracion.str.startswith("cpp_")][["configuracion", "hilos", "repeticiones", "etl_segundos_media"]])'''),
        md('''## 2. Imagen y ejecución Docker

La etapa de compilación reutiliza `jupyterlab_docker_master-openmp:latest`. La imagen final contiene el ejecutable, sus bibliotecas dinámicas y el dataset, y se ejecuta como usuario 1000. No instala Python para realizar el ETL. El binario de esa misma imagen se copia a Workbench para evitar comparar compilaciones distintas.

Ejecutar desde el anfitrión con Docker disponible:

```bash
docker build --network=none -f Dockerfile.cpp -t etl-distribuido-cpp:local .
mkdir -p resultados_docker_cpp
docker run --rm --network=none --cpus=4 --memory=512m \
  --mount type=bind,src="$PWD/resultados_docker_cpp",dst=/resultados \
  etl-distribuido-cpp:local
```
'''),
        code('''print((ROOT / "Dockerfile.cpp").read_text())
print((OUT / "ejecuciones/docker_openmp_4_05/ejecucion.log").read_text())
display(pd.DataFrame([json.loads((OUT / "ejecuciones/docker_openmp_4_05/validacion.json").read_text())]))'''),
        md('''## 3. Dos trabajadores y unión con Compose

El hash estable FNV-1a del `ID_REGISTRO`, antes de recortar el texto, asigna cada registro a una de dos particiones. Los identificadores repetidos quedan en el mismo trabajador. Cada trabajador usa dos hilos y exporta índices originales; el reductor C++ espera las dos terminaciones correctas y reúne las filas en el orden de entrada.

Cada trabajador lee todo el CSV para inferir el mismo esquema; solamente transforma su partición. Esta lectura repetida y la unión añaden costo, que se incluye en la evaluación.

```bash
mkdir -p resultados_docker_cpp
docker compose -f compose.cpp.yaml up --no-build --force-recreate
docker compose -f compose.cpp.yaml down
```
'''),
        code('''print((ROOT / "compose.cpp.yaml").read_text())
carpeta = OUT / "ejecuciones/compose_2x2_05"
auditorias = [json.loads((carpeta / f"parte_{i}.json").read_text()) for i in range(2)]
display(pd.DataFrame(auditorias)[["particion", "filas_entrada", "filas_salida", "hilos_utilizados", "etl_segundos"]])
assert sum(a["filas_salida"] for a in auditorias) == 108733
assert sum(a["hilos_utilizados"] for a in auditorias) == 4
display(pd.DataFrame([json.loads((carpeta / "validacion.json").read_text())]))
print((carpeta / "ejecucion.log").read_text())'''),
        md('''## Resultado del avance 2

Se comprobaron dos niveles de paralelismo: hilos OpenMP dentro de un proceso y particiones distintas entre contenedores. La salida de ambos coincide con la referencia secuencial y conserva el orden. La evaluación repetida de sus tiempos está en el notebook 04.
'''), md(REFERENCES)]

    third = [
        md('''# 03 · Particiones C++ con Kubernetes

**Objetivo:** ejecutar el ETL mediante un Indexed Job con dos Pods, recuperar las salidas persistentes y reunirlas con un Job C++ adicional.

La prueba usa un nodo local K3s creado para esta entrega. Los dos Pods trabajan sobre particiones distintas en la misma máquina; no se presenta como una prueba entre máquinas físicas.
'''), code(COMMON),
        md('''## 1. Clúster y almacenamiento

`scripts/preparar_entorno.py` reutiliza la imagen OpenMP y prepara `etl-k3s`, sin reiniciar los contenedores existentes. El servidor K3s de laboratorio funciona dentro de Docker; no se expone una API pública. Sus recursos se limitan a 4 CPU y 1800 MiB de memoria.

Los manifiestos usan el namespace `etl-cpp` y un PVC de 1 GiB. `ReadWriteOnce` es adecuado para esta prueba de un nodo; una prueba multinodo necesitaría revisar el almacenamiento compartido.

```bash
docker exec etl-k3s kubectl get nodes
docker exec etl-k3s kubectl apply -f /tmp/etl-cpp-manifiestos/almacenamiento.yaml
```
'''),
        code('''print("Versión probada:", entorno["kubernetes"])
print((ROOT / "k8s/cpp/almacenamiento.yaml").read_text())
print((ROOT / "k8s/cpp/job-particiones.yaml").read_text())'''),
        md('''## 2. Ejecución paralela y unión

`parallelism: 2`, `completions: 2` y `completionMode: Indexed` crean dos tareas identificadas por índice. Cada Pod usa dos hilos OpenMP y escribe su propio CSV, auditoría e índices. La unión se lanza después de comprobar que el Job de particiones completó las dos tareas.

```bash
docker exec etl-k3s kubectl apply -f /tmp/etl-cpp-manifiestos/job-particiones.yaml
docker exec etl-k3s kubectl -n etl-cpp wait --for=condition=complete job/etl-cpp-particiones --timeout=180s
docker exec etl-k3s kubectl apply -f /tmp/etl-cpp-manifiestos/job-union.yaml
docker exec etl-k3s kubectl -n etl-cpp wait --for=condition=complete job/etl-cpp-union --timeout=120s
```
'''),
        code('''carpeta = OUT / "ejecuciones/kubernetes_2x2_05"
jobs = json.loads((carpeta / "jobs.json").read_text())
display(pd.DataFrame([{"job": j["metadata"]["name"], "completados": j["status"].get("succeeded", 0),
                       "esperados": j["spec"].get("completions", 1)} for j in jobs["items"]]))
pods = json.loads((carpeta / "pods.json").read_text())
display(pd.DataFrame([{"pod": p["metadata"]["name"], "fase": p["status"]["phase"],
                       "indice": p["metadata"].get("annotations", {}).get("batch.kubernetes.io/job-completion-index", "lector/unión")}
                      for p in pods["items"]]))
print((carpeta / "ejecucion.log").read_text())'''),
        md('''## 3. Recuperación y validación

El Pod lector monta el mismo PVC y permite leer los archivos con la utilidad mínima `etl --read-file`. La imagen no necesita shell ni `tar`. La recuperación y la comparación de contenido se hacen después del cronómetro del despliegue.
'''),
        code('''auditorias = [json.loads((carpeta / f"parte_{i}.json").read_text()) for i in range(2)]
display(pd.DataFrame(auditorias)[["particion", "filas_entrada", "filas_salida", "hilos_utilizados"]])
assert sum(a["filas_salida"] for a in auditorias) == 108733
assert sum(a["hilos_utilizados"] for a in auditorias) == 4
display(pd.DataFrame([json.loads((carpeta / "validacion.json").read_text())]))
display(pd.DataFrame([json.loads((carpeta / "union.json").read_text())]))'''),
        md('''## Resultado del avance 3

El Job indexado completó sus dos particiones, el Job de unión finalizó correctamente y la salida recuperada coincidió con la referencia. Se conservan Jobs, auditorías, registros de finalización y validaciones por repetición, además del estado completo de los Pods de la última corrida. El notebook 04 compara su tiempo de pared con las demás implementaciones.
'''), md(REFERENCES)]

    fourth = [
        md('''# 04 · Experimentos y rendimiento del ETL C++

**Integración de los avances 1, 2 y 3:** mismo dataset, reglas, ejecutable y contenido de salida.  
**Escenarios:** C++ secuencial, OpenMP 1/2/4/8 hilos, Docker, Compose 2×2 y Kubernetes 2×2.  
**Objetivo:** medir tiempos, speedup, eficiencia y el límite de Amdahl, e interpretar los resultados reales.
'''), code(COMMON),
        md('''## 1. Protocolo reproducible

- Cinco repeticiones medidas por escenario, precedidas por un calentamiento excluido.
- Orden de escenarios aleatorio y reproducible en cada ronda; semilla y algoritmo registrados.
- Misma entrada y mismo binario C++ para todos los escenarios. Construcción, descarga e importación de imágenes y arranque inicial del clúster fuera de las mediciones.
- Relectura y exportación en cada ejecución. Caché del sistema operativo sin vaciar; cierre de archivo sin `fsync`.
- Validación de contenido y recuperación fuera del cronómetro.
- Workbench y Docker individual: cuota de 4 CPU. Compose: dos contenedores con dos CPU cada uno. K3s: cuota de 4 CPU compartida con su control plane. Ocho hilos sobre cuota de cuatro estudia sobresuscripción.

Los resultados describen esta carga fija y este equipo. Los montajes y el almacenamiento del PVC son distintos; los tiempos no aíslan exclusivamente el costo del orquestador.
'''),
        code('''display(pd.DataFrame([{"dato": k, "valor": str(entorno[k])} for k in
    ["fecha_utc", "cpu_modelo", "compilacion", "binario_sha256", "docker", "kubernetes", "recursos", "temporizadores", "almacenamiento"]]))
display(mediciones.groupby("configuracion").agg(repeticiones=("repeticion", "count"), validas=("validado", "all")))
assert len(mediciones) == 40
assert mediciones.groupby("configuracion").size().eq(5).all()
assert mediciones.validado.all()
assert mediciones.salida_sha256.nunique() == 1'''),
        md('''## 2. Qué tiempo se compara

**Núcleo:** transformación de filas, distribuida por OpenMP.  
**ETL de transformación:** preparación global (normalización/deduplicación/partición) más núcleo.  
**Total interno:** lectura e inferencia + preparación + núcleo + escritura.  
**Pared del despliegue:** lanzamiento hasta finalización; para Compose/Kubernetes incluye los trabajadores y la unión. En Workbench incluye `docker exec` sobre el contenedor ya activo. En Kubernetes se suman los tiempos CLI de `apply`/`wait` de ambos Jobs.

Para el escalamiento C++ se comparan cronómetros internos equivalentes. Para los despliegues completos se compara pared externa; no se mezcla el tiempo de un bucle con el de un Job completo.
'''),
        code('''columnas = ["configuracion", "hilos", "repeticiones", "etl_segundos_media", "etl_segundos_desviacion",
            "lectura_escritura_total_segundos_media", "pared_segundos_media", "pared_segundos_desviacion"]
display(resumen[columnas])
display(Image.open(OUT / "graficas/01_tiempos_cpp.png"))'''),
        md('''## 3. Speedup y eficiencia

`S(p) = T_secuencial / T_paralelo(p)`  
`E(p) = S(p) / p`  
`E_porcentaje = 100 × E(p)`

`p` es el número real de hilos registrados: 1, 2, 4 u 8; Compose y Kubernetes tienen dos trabajadores de dos hilos, en total cuatro. La eficiencia expresa escalamiento relativo, no utilización de CPU. Un speedup menor que uno significa que esa ejecución fue más lenta que la referencia.
'''),
        code('''cpp = resumen[resumen.configuracion.str.startswith("cpp_")]
display(cpp[["configuracion", "hilos", "speedup_etl", "eficiencia_etl_porcentaje", "speedup_kernel"]])
display(Image.open(OUT / "graficas/03_eficiencia.png"))'''),
        md('''## 4. Ley de Amdahl con fracción serial instrumentada

`S_Amdahl(p) = 1 / [f + (1-f)/p]`  
`S_límite = 1/f`

Para la transformación, `f` es tiempo de preparación global dividido entre tiempo ETL secuencial. Para el proceso con entrada/salida, se considera serial todo el tiempo fuera del bucle de filas. Son estimaciones operativas obtenidas de fases cronometradas de la misma implementación.

El modelo supone un problema fijo y costos ideales; no incluye explícitamente barreras, creación de equipos, competencia por memoria ni variaciones de caché. El residuo entre tiempo medido y predicho no es una medición aislada de comunicación.
'''),
        code('''from informe import amdahl, conclusiones
modelo = amdahl(resumen)
display(modelo[["configuracion", "fraccion_serial_etl", "fraccion_serial_total", "speedup_etl", "speedup_amdahl_etl", "speedup_total_interno", "speedup_amdahl_total", "residuo_modelo_etl_segundos"]])
display(Image.open(OUT / "graficas/02_speedup_amdahl.png"))
display(Image.open(OUT / "graficas/05_fases.png"))'''),
        md('''## 5. Comparación completa de despliegues

El particionado de Compose y Kubernetes asigna IDs distintos a dos trabajadores y el reductor recupera el orden. Cada trabajador relee el archivo completo. Para una carga de unos 17 MiB, la lectura repetida, la unión y el lanzamiento pueden superar el ahorro de paralelizar las filas.

Kubernetes usa dos Pods sobre un nodo. La prueba comprueba distribución de trabajo entre contenedores y recuperación persistente; no demuestra escalabilidad entre máquinas.
'''),
        code('''display(resumen[["configuracion", "hilos", "pared_segundos_media", "pared_segundos_desviacion", "speedup_pared", "eficiencia_pared_porcentaje"]])
display(Image.open(OUT / "graficas/04_comparacion_despliegues.png"))'''),
        code('''display({"text/markdown": conclusiones(resumen, mediciones)}, raw=True)'''),
        md('''## 6. Archivos y reproducción

Las mediciones individuales están en `resultados_experimentos/mediciones.csv`; los calentamientos también se conservan en `mediciones_todas.csv`. Cada carpeta de ejecución contiene auditorías y validación; la última repetición conserva el CSV limpio. Las cinco figuras se exportan en PNG, SVG y PDF.

Para volver a desplegar y medir, ejecutar desde el anfitrión con Docker disponible y un entorno Python con pandas/numpy:

```bash
python scripts/preparar_entorno.py
python scripts/experimentos.py --repeticiones 5 --salida resultados_nueva_corrida
```

Este notebook puede volver a ejecutar sus cálculos y figuras de análisis desde los CSV ya medidos sin acceso al daemon Docker. No inventa mediciones nuevas al abrirse.
'''), md(REFERENCES)]
    return {"01_ETL_Cpp.ipynb": first, "02_Docker_Cpp.ipynb": second,
            "03_Kubernetes_Cpp.ipynb": third, "04_Experimentos_Rendimiento.ipynb": fourth}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--solo", nargs="*")
    args = parser.parse_args()
    for name, cells in notebooks().items():
        if args.solo and name not in args.solo:
            continue
        notebook = nbf.v4.new_notebook(cells=cells, metadata={
            "kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"},
            "language_info": {"name": "python"},
            "descripcion": "Python documenta y analiza; el ETL y el reductor se ejecutan en C++17/OpenMP."})
        NotebookClient(notebook, timeout=180, kernel_name="python3",
                       resources={"metadata": {"path": str(ROOT)}}, allow_errors=False).execute()
        nbf.validate(notebook)
        nbf.write(notebook, ROOT / name)
        html, _ = HTMLExporter().from_notebook_node(notebook)
        (ROOT / name.replace(".ipynb", ".html")).write_text(html, encoding="utf-8")
        print("Ejecutado y exportado:", name, flush=True)

if __name__ == "__main__":
    main()
