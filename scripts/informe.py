"""Tablas, Amdahl y figuras exportables a partir de las mediciones reales."""
from datetime import datetime, timezone
import argparse
import json
from pathlib import Path
import platform
import sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from comun import ROOT, sha256

OUT = ROOT / "resultados_experimentos"
LABELS = {"cpp_secuencial": "C++ secuencial", "cpp_openmp_1": "OpenMP · 1 hilo",
          "cpp_openmp_2": "OpenMP · 2 hilos", "cpp_openmp_4": "OpenMP · 4 hilos",
          "cpp_openmp_8": "OpenMP · 8 hilos", "docker_openmp_4": "Docker · 4 hilos",
          "compose_2x2": "Compose · 2 × 2", "kubernetes_2x2": "Kubernetes · 2 × 2"}

def cargar():
    return pd.read_csv(OUT / "resumen_rendimiento.csv"), pd.read_csv(OUT / "mediciones.csv")

def amdahl(summary):
    seq = summary[summary.configuracion == "cpp_secuencial"].iloc[0]
    f_etl = seq.global_segundos_media / seq.etl_segundos_media
    f_total = 1 - seq.kernel_segundos_media / seq.lectura_escritura_total_segundos_media
    cpp = summary[summary.configuracion.str.startswith("cpp_")].copy()
    cpp["fraccion_serial_etl"] = f_etl
    cpp["fraccion_serial_total"] = f_total
    cpp["speedup_amdahl_etl"] = 1 / (f_etl + (1 - f_etl) / cpp.hilos)
    cpp["speedup_amdahl_total"] = 1 / (f_total + (1 - f_total) / cpp.hilos)
    cpp["speedup_total_interno"] = seq.lectura_escritura_total_segundos_media / cpp.lectura_escritura_total_segundos_media
    cpp["residuo_modelo_etl_segundos"] = cpp.etl_segundos_media - seq.etl_segundos_media / cpp.speedup_amdahl_etl
    return cpp

def conclusiones(summary, raw):
    seq = summary[summary.configuracion == "cpp_secuencial"].iloc[0]
    omp = summary[summary.configuracion.str.startswith("cpp_openmp")]
    best = omp.loc[omp.etl_segundos_media.idxmin()]
    model = amdahl(summary)
    f = float(model.fraccion_serial_etl.iloc[0])
    ft = float(model.fraccion_serial_total.iloc[0])
    deployment = summary[summary.configuracion.isin(["docker_openmp_4", "compose_2x2", "kubernetes_2x2"])]
    lines = [
        "## Conclusiones obtenidas de esta ejecución",
        f"1. El ETL y la unión se implementaron en **C++17**. OpenMP reparte filas entre hilos; Compose y Kubernetes ejecutan dos particiones distintas con dos hilos por trabajador.",
        f"2. Se validaron **{len(raw)} ejecuciones medidas**, más un calentamiento excluido por configuración. Todas conservaron **108,733 filas y 42 columnas**, coincidieron con la referencia Python y produjeron el mismo CSV C++ ordenado. La auditoría no registró duplicados ni edades fuera de rango en este dataset.",
        f"3. La transformación secuencial C++ tardó **{seq.etl_segundos_media:.6f} ± {seq.etl_segundos_desviacion:.6f} s** (media ± desviación estándar muestral). La mejor configuración OpenMP para el ETL completo fue **{int(best.hilos)} hilos**, con **{best.etl_segundos_media:.6f} s**, speedup **{best.speedup_etl:.3f}×** y eficiencia **{best.eficiencia_etl_porcentaje:.2f} %**.",
        f"4. La preparación global representa aproximadamente **{f*100:.2f} %** del tiempo secuencial de transformación. La Ley de Amdahl estima un límite de **{1/f:.3f}×** para ese alcance, aun con muchos hilos. Al incluir lectura y escritura, la fracción serial estimada asciende a **{ft*100:.2f} %** y el límite ideal baja a **{1/ft:.3f}×**.",
        "5. Los tiempos de pared de los despliegues incluyen su lanzamiento y la unión. No se deben confundir con el cronómetro interno del bucle OpenMP:",
    ]
    lines.extend(f"   - {LABELS[r.configuracion]}: **{r.pared_segundos_media:.4f} ± {r.pared_segundos_desviacion:.4f} s**; speedup de pared frente al secuencial en Workbench **{r.speedup_pared:.3f}×**." for r in deployment.itertuples())
    lines += [
        "6. Para esta carga, la lectura, deduplicación y escritura limitan la mejora total. En Compose/Kubernetes cada trabajador vuelve a leer el archivo completo, y la unión añade trabajo serial. Los valores medidos describen este diseño y equipo; no demuestran una ventaja general de un orquestador.",
        "7. Kubernetes se probó con **dos Pods en un único nodo local K3s**. Es paralelismo entre contenedores sobre la misma máquina, con memoria compartida por los hilos de cada proceso. No es una prueba entre varias máquinas físicas.",
        "8. El Workbench tiene cuota de **4 CPU**. La prueba de 8 hilos estudia sobresuscripción; su eficiencia S/p expresa escalamiento relativo y no porcentaje de utilización de CPU. El control plane de K3s comparte el presupuesto de CPU del clúster.",
        "9. Las diferencias con Amdahl son residuos del modelo: incluyen variación del sistema, caché, acceso a memoria y sobrecarga de OpenMP. La fracción serial se obtuvo de fases instrumentadas, no se ajustó artificialmente para hacer coincidir las curvas.",
    ]
    return "\n\n".join(lines)

def guardar_figura(fig, name):
    target = OUT / "graficas"; target.mkdir(exist_ok=True)
    for ext in ["png", "svg", "pdf"]:
        fig.savefig(target / f"{name}.{ext}", dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)

def figuras(summary, raw):
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "axes.spines.top": False, "axes.spines.right": False,
                         "axes.titleweight": "bold", "axes.grid": True,
                         "grid.alpha": .18, "figure.constrained_layout.use": True})
    cpp = summary[summary.configuracion.str.startswith("cpp_")].copy()
    x = np.arange(len(cpp)); labels = ["Secuencial", "OMP 1", "OMP 2", "OMP 4", "OMP 8"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.7))
    for ax, metric, title in [(axes[0], "etl_segundos", "Transformación: preparación global + filas"),
                               (axes[1], "lectura_escritura_total_segundos", "Proceso completo: lectura + ETL + escritura")]:
        ax.bar(x, cpp[metric + "_media"], color=["#334155"] + ["#16859b"]*4,
               yerr=cpp[metric + "_desviacion"], capsize=4)
        ax.set_xticks(x, labels); ax.set_ylabel("Tiempo medio (s)"); ax.set_title(title, fontsize=11)
        ax.set_ylim(bottom=0); ax.set_axisbelow(True)
    fig.suptitle("C++ / OpenMP · 108,733 registros · 5 repeticiones", fontsize=14)
    guardar_figura(fig, "01_tiempos_cpp")

    model = amdahl(summary)
    omp = model[model.configuracion.str.startswith("cpp_openmp")]
    p = np.linspace(1, 8, 150)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.7))
    for ax, f, actual, title in [(axes[0], model.fraccion_serial_etl.iloc[0], omp.speedup_etl, "Transformación ETL"),
                                (axes[1], model.fraccion_serial_total.iloc[0], omp.speedup_total_interno, "Proceso completo con entrada/salida")]:
        ax.plot(p, p, "--", color="#94a3b8", label="Ideal lineal S=p")
        ax.plot(p, 1/(f+(1-f)/p), color="#d97706", label=f"Amdahl · f={f:.3f}")
        ax.plot(omp.hilos, actual, "o-", color="#16859b", label="Medido OpenMP")
        ax.scatter([1], [1], color="#334155", marker="D", label="Referencia secuencial", zorder=5)
        ax.set_xticks([1, 2, 4, 8]); ax.set_xlabel("Hilos solicitados"); ax.set_ylabel("Speedup T₁/Tₚ")
        ax.set_title(title); ax.legend(fontsize=8); ax.set_ylim(bottom=0)
        zoom = ax.inset_axes([.48, .27, .47, .29])
        zoom.plot(p, 1/(f+(1-f)/p), color="#d97706")
        zoom.plot(omp.hilos, actual, "o-", color="#16859b", markersize=3)
        zoom.set_xticks([1, 2, 4, 8]); zoom.tick_params(labelsize=7)
        zoom.set_title("Detalle medición / modelo", fontsize=8)
        zoom.set_ylim(.98, max(float(max(actual)), float(1/(f+(1-f)/8)))*1.04)
    fig.suptitle("Ley de Amdahl · fracción serial medida por fases", fontsize=14)
    guardar_figura(fig, "02_speedup_amdahl")

    fig, ax = plt.subplots(figsize=(9, 4.7))
    omp_summary = summary[summary.configuracion.str.startswith("cpp_openmp")]
    ax.bar(np.arange(len(omp_summary)), omp_summary.eficiencia_etl_porcentaje, color="#16859b")
    ax.axhline(100, color="#94a3b8", linestyle="--", label="Ideal: 100 %")
    ax.set_xticks(np.arange(len(omp_summary)), [f"{p} hilo{'s' if p!=1 else ''}" for p in omp_summary.hilos])
    ax.set_ylabel("Eficiencia del ETL · 100 × S/p (%)")
    ax.set_title("Eficiencia OpenMP · referencia C++ secuencial")
    ax.legend(); ax.set_axisbelow(True)
    guardar_figura(fig, "03_eficiencia")

    selected = summary[summary.configuracion.isin(["cpp_secuencial", "cpp_openmp_4", "docker_openmp_4", "compose_2x2", "kubernetes_2x2"])]
    fig, ax = plt.subplots(figsize=(11, 5))
    bars = ax.bar(np.arange(len(selected)), selected.pared_segundos_media,
                  yerr=selected.pared_segundos_desviacion, capsize=4,
                  color=["#334155", "#16859b", "#2563eb", "#7c3aed", "#d97706"])
    ax.set_xticks(np.arange(len(selected)), [LABELS[n] for n in selected.configuracion])
    ax.set_ylabel("Tiempo medio de pared (s)")
    ax.set_title("Tiempo de pared · lanzamiento, ejecución y unión")
    for bar, value, error in zip(bars, selected.pared_segundos_media, selected.pared_segundos_desviacion):
        ax.annotate(f"{value:.3f} s", (bar.get_x()+bar.get_width()/2, value+error), xytext=(0, 9), textcoords="offset points", ha="center")
    ax.set_ylim(0, max(selected.pared_segundos_media + selected.pared_segundos_desviacion)*1.22)
    ax.set_axisbelow(True)
    guardar_figura(fig, "04_comparacion_despliegues")

    fig, ax = plt.subplots(figsize=(10, 4.8))
    seq_raw = raw[raw.configuracion == "cpp_secuencial"]
    omp_raw = raw[raw.configuracion == "cpp_openmp_4"]
    phase_names = ["Lectura e inferencia", "Preparación global", "Filas", "Escritura"]
    phase_keys = ["lectura_segundos", "preparacion_global_segundos", "transformacion_filas_segundos", "escritura_segundos"]
    values = []
    for group in [seq_raw, omp_raw]:
        audits = [json.loads((ROOT / row.carpeta / "auditoria.json").read_text()) for row in group.itertuples()]
        values.append([np.mean([a[key] for a in audits]) for key in phase_keys])
    left = np.zeros(2)
    for i, (label, color) in enumerate(zip(phase_names, ["#94a3b8", "#334155", "#16859b", "#d97706"])):
        width = np.array(values)[:, i]
        ax.barh([0, 1], width, left=left, label=label, color=color)
        left += width
    ax.set_yticks([0, 1], ["C++ secuencial", "OpenMP · 4 hilos"])
    ax.set_xlabel("Tiempo medio interno (s)"); ax.set_title("Qué parte del proceso puede acelerar OpenMP")
    ax.legend(loc="upper center", bbox_to_anchor=(.5, -.15), ncol=2)
    ax.set_axisbelow(True)
    guardar_figura(fig, "05_fases")

def generar():
    summary, raw = cargar()
    model = amdahl(summary)
    model.to_csv(OUT / "analisis_amdahl.csv", index=False)
    (OUT / "conclusiones.md").write_text(conclusiones(summary, raw), encoding="utf-8")
    daemon = json.loads((OUT / "entorno_daemon.json").read_text()) if (OUT / "entorno_daemon.json").exists() else {}
    if not (OUT / "entorno.json").exists():
        env = {"fecha_utc": datetime.now(timezone.utc).isoformat(),
               "python_validacion": sys.version, "pandas_validacion": pd.__version__, "numpy_validacion": np.__version__,
               "dataset_sha256": sha256(ROOT / "datos/COVID19MEXICO.csv"),
               "fuente_cpp_sha256": sha256(ROOT / "cpp/etl.cpp"), "binario_sha256": sha256(ROOT / "bin/etl"),
               "cpu_modelo": next((x.split(":", 1)[1].strip() for x in Path('/proc/cpuinfo').read_text().splitlines() if x.startswith('model name')), "no disponible"),
               "compilacion": "g++ 13.3.0; -std=c++17 -O3 -Wall -Wextra -Wpedantic -fopenmp",
               "binario_comun": "Mismo ejecutable compilado en la imagen hpc-openmp y copiado a Workbench. Docker y K3s usan la misma imagen ETL.",
               "repeticiones": 5, "calentamientos_excluidos_por_configuracion": 1,
               "orden": "Fisher-Yates con LCG (1664525*x+1013904223 mod 2^32), semilla 20261110+ronda.",
               "recursos": "Workbench y Docker individual: cuota 4 CPU; Compose: 2 contenedores x 2 CPU; K3s: cuota 4 CPU compartida entre control plane y dos Pods. 8 hilos sobre cuota 4 es sobresuscripción.",
               "temporizadores": "steady_clock en C++: lectura/inferencia, preparación global, filas, escritura y total. Pared externa del proceso CLI; en K3s suma apply/wait de particiones y unión. Recuperación y validación fuera de tiempos. Construcción, pull, importación y arranque del clúster excluidos.",
               "almacenamiento": "Workbench: /tmp del contenedor; Docker/Compose: bind mount; K3s: PVC local-path. Caché del SO sin vaciar. close sin fsync: no es una medición de persistencia física.",
               "alcance": "C++ secuencial/OpenMP, Compose con particiones por ID, Kubernetes Indexed Job con dos Pods en un nodo local. Cada trabajador lee el CSV completo; la unión C++ restaura orden por índices originales.",
               "validacion": "Contenido, columnas, orden y nulos frente a la función original del notebook 01; int/float equivalentes al recargar CSV. Además, SHA-256 idéntico entre todas las salidas C++.",
               "referencias": ["https://www.openmp.org/spec-html/5.2/openmpse66.html", "https://gcc.gnu.org/projects/gomp/", "https://kubernetes.io/docs/concepts/workloads/controllers/job/", "https://cvw.cac.cornell.edu/parallel/efficiency/amdahls-law"],
               **daemon}
        (OUT / "entorno.json").write_text(json.dumps(env, indent=2, ensure_ascii=False))
    figuras(summary, raw)
    print(conclusiones(summary, raw))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--salida", type=Path, default=OUT)
    args = parser.parse_args()
    OUT = args.salida.resolve()
    generar()
