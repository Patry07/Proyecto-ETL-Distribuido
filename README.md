# ETL distribuido en C++17 / OpenMP

Entrega integrada de los avances 1, 2 y 3 y de la evaluación de rendimiento (parte 4).
El ETL y la unión de particiones se ejecutan en **C++**. Python organiza las pruebas,
valida contra la implementación original y genera tablas, gráficas y notebooks.

## Abrir la entrega

| Parte | Notebook ejecutado | Lectura sin Jupyter |
|---|---|---|
| 1. Datos, exploración y ETL secuencial C++ | [01_ETL_Cpp.ipynb](01_ETL_Cpp.ipynb) | [HTML](01_ETL_Cpp.html) |
| 2. OpenMP, Docker y Compose | [02_Docker_Cpp.ipynb](02_Docker_Cpp.ipynb) | [HTML](02_Docker_Cpp.html) |
| 3. Kubernetes con dos Pods y unión | [03_Kubernetes_Cpp.ipynb](03_Kubernetes_Cpp.ipynb) | [HTML](03_Kubernetes_Cpp.html) |
| 4. Experimentos, speedup, eficiencia y Amdahl | [04_Experimentos_Rendimiento.ipynb](04_Experimentos_Rendimiento.ipynb) | [HTML](04_Experimentos_Rendimiento.html) |

Los tres notebooks Python originales se conservan con sus nombres originales como
antecedentes. Los archivos con sufijo `Cpp` y el notebook 04 son la entrega integrada.

## Resultados reales

Dataset: **108,733 registros, 42 columnas, 17.43 MiB**.
Se ejecutaron ocho configuraciones, cinco repeticiones medidas por configuración
y un calentamiento excluido por configuración: **48 ejecuciones en total**.
Todas coincidieron con la función de referencia del notebook original 01 y todas
las salidas C++ produjeron el mismo SHA-256, conservando el orden.

| Configuración | Tiempo ETL interno medio | Pared media del lanzamiento |
|---|---:|---:|
| C++ secuencial en Workbench | 0.201135 s | 1.012379 s |
| C++ OpenMP, 4 hilos en Workbench | 0.158650 s | 0.971445 s |
| Docker, 4 hilos | 0.183475 s | 1.171510 s |
| Compose, 2 contenedores × 2 hilos | No se mezcla con el cronómetro de un solo proceso | 2.026892 s |
| Kubernetes, 2 Pods × 2 hilos | No se mezcla con el cronómetro de un solo proceso | 6.902838 s |

La mejor configuración OpenMP para la transformación fue **4 hilos**:
speedup **1.268×**, eficiencia **31.69 %**. La preparación global representa el
**74.18 %** del ETL secuencial; Amdahl estima un límite ideal de **1.348×** para
ese alcance. Incluyendo lectura y escritura, la fracción serial estimada es
**94.98 %** y el límite baja a **1.053×**.

Los promedios y desviaciones, incluido OpenMP con 1, 2 y 8 hilos, están en
[resumen_rendimiento.csv](resultados_experimentos/resumen_rendimiento.csv).
Las conclusiones completas están en [conclusiones.md](resultados_experimentos/conclusiones.md).

## Qué implementa cada avance

1. **Datos y ETL secuencial:** lector CSV C++ con BOM, comillas, campos multilínea
   y CRLF; inferencia numérica; normalización de encabezados; deduplicación;
   validación de fechas ISO y edades; limpieza de espacios; salida CSV UTF-8.
   Conserva las reglas de los notebooks originales para el esquema del dataset.
   Los resúmenes descriptivos se producen en la etapa de análisis.
2. **OpenMP y Docker:** reparto estático de filas y reducción del contador de
   edades inválidas; registro del número real de hilos; imagen compilada a partir
   del entorno `hpc-openmp` disponible. Compose ejecuta dos particiones y después
   un reductor C++.
3. **Kubernetes:** Indexed Job con `parallelism: 2`, `completions: 2`, PVC,
   Job de unión y Pod lector. Los trabajadores procesan particiones diferentes.
4. **Rendimiento:** cronómetros por fase, orden aleatorio reproducible,
   repeticiones y calentamiento separados, validación de cada salida, media,
   mediana, desviación estándar, speedup, eficiencia, fracción serial medida y
   comparación con Amdahl. Cinco figuras en PNG, SVG y PDF.

El hash FNV-1a del identificador original decide la partición. Así, los ID repetidos
permanecen en el mismo trabajador y la deduplicación conserva el comportamiento
secuencial. Los índices originales permiten reunir la salida en el orden inicial.

## Alcance de las mediciones

- **Núcleo:** limpieza de las filas con OpenMP.
- **ETL interno:** preparación global más núcleo, con los datos ya leídos.
- **Total interno:** lectura/inferencia, ETL y escritura.
- **Pared externa:** lanzamiento hasta finalización; Compose y Kubernetes incluyen
  ambos trabajadores y la unión. Workbench ya está activo. En las mediciones
  entregadas de Kubernetes se suman los tiempos de los comandos `apply` y `wait`.

El Workbench y Docker individual tienen cuota de 4 CPU. Compose asigna dos CPU a
cada trabajador. El K3s local tiene cuota de 4 CPU compartida con su control plane.
Ocho hilos en Workbench representan sobresuscripción. La eficiencia S/p no mide
utilización de CPU.

Se excluyeron construcción y descarga de imágenes, importación y arranque inicial
del clúster. La recuperación y validación ocurren después del cronómetro. La caché
del sistema operativo no se vació y el cierre de los archivos no fuerza `fsync`.
Los tipos de almacenamiento de Workbench, bind mount y PVC son diferentes.

Kubernetes utiliza **dos Pods en un nodo local**, no varias máquinas físicas.
Cada trabajador lee el CSV completo y después transforma su partición; esa lectura
repetida y la unión limitan el rendimiento para esta carga pequeña. El resultado
no demuestra una superioridad general de un orquestador.

## Estructura

- `cpp/etl.cpp`: motor y reductor C++17/OpenMP.
- `Makefile`: compilación local.
- `Dockerfile.cpp`: construcción de la imagen ETL usando el entorno OpenMP existente.
- `Dockerfile.toolchain`: alternativa para construir ese entorno en un equipo nuevo.
- `compose.cpp.yaml`: dos trabajadores y unión después de su finalización correcta.
- `k8s/cpp/`: namespace, PVC, Indexed Job, Job de unión y lector.
- `scripts/preparar_entorno.py`: construcción e importación de imagen y K3s de laboratorio.
- `scripts/experimentos.py`: orquestación reproducible desde el anfitrión.
- `scripts/analizar_resultados.py`: validación y consolidación de las evidencias.
- `scripts/informe.py`: métricas, Amdahl, conclusiones y figuras.
- `scripts/generar_notebooks.py`: ejecución con kernel Jupyter y exportación HTML.
- `tests/test_etl_cpp.py`: equivalencia secuencial/paralela y casos de borde del CSV.
- `resultados_experimentos/`: mediciones, entorno, auditorías, validaciones y figuras.

Los CSV limpios de la última repetición se conservan por configuración. En las
repeticiones anteriores se guardan auditoría, validación, hash y registro de
ejecución; los CSV grandes intermedios se eliminan. Se conserva el estado completo
de los Pods de la última prueba y los Jobs de cada repetición.

## Compilar y comprobar

```bash
make
bin/etl --help
bin/etl --input datos/COVID19MEXICO.csv --output resultados/limpio.csv \
  --audit resultados/auditoria.json --mode omp --threads 4
python -m unittest discover -s tests -v
```

La prueba de referencia requiere pandas y numpy. Las pruebas sintéticas cubren
comillas y saltos de línea, BOM/CRLF, fechas inválidas, edades fuera de rango,
duplicados entre particiones y rechazo de parámetros o CSV malformados.

## Repetir Docker y Kubernetes

Ejecutar desde el anfitrión con Docker disponible. El kernel de JupyterLab no
necesita acceso al daemon para leer los resultados y volver a calcular el análisis.

```bash
# Solo si falta la imagen de herramientas existente:
docker build -f Dockerfile.toolchain -t jupyterlab_docker_master-openmp:latest .

python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-experimentos.txt
.venv/bin/python scripts/preparar_entorno.py
.venv/bin/python scripts/experimentos.py --repeticiones 5 --salida resultados_nueva_corrida
.venv/bin/python scripts/informe.py --salida resultados_nueva_corrida
```

El script presupone que `dev-workbench` está activo. Crea únicamente el contenedor
de laboratorio `etl-k3s` y los recursos ETL del namespace `etl-cpp`. K3s dentro de
Docker usa el modo privilegiado que necesita su nodo de laboratorio. No publica
la API de Kubernetes en un puerto del anfitrión.

Se utilizó el `dev-workbench` existente para ejecutar C++ y los cuatro kernels
Jupyter. Hay una copia de trabajo dentro de ese contenedor en
`/tmp/proyecto-etl-cpp`; es temporal. La entrega permanente está en la carpeta de
Downloads y en su ZIP. Los HTML se abren directamente sin ejecutar código.

Para detener el clúster de esta entrega cuando ya no se necesite:

```bash
docker stop etl-k3s
```

El entorno exacto, binario, dataset y compilación se identifican en
[entorno.json](resultados_experimentos/entorno.json). Las versiones de análisis
disponibles en Workbench están en `requirements-experimentos.txt`; la validación
externa utilizó pandas 3.0.6 y numpy 2.5.4 y las figuras matplotlib 3.11.2, registrados
por separado en los metadatos.

## Referencias

- [OpenMP: reparto de iteraciones](https://www.openmp.org/spec-html/5.2/openmpse66.html).
- [GCC: soporte OpenMP](https://gcc.gnu.org/projects/gomp/).
- [Kubernetes: Jobs paralelos e indexados](https://kubernetes.io/docs/concepts/workloads/controllers/job/).
- [K3s: configuración y ejecución en Docker](https://docs.k3s.io/advanced).
- [Cornell: Ley de Amdahl](https://cvw.cac.cornell.edu/parallel/efficiency/amdahls-law).
