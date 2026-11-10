"""Orquesta ejecuciones reales. El procesamiento ETL y la unión son C++.

Ejecutar desde el anfitrión con acceso a Docker, no desde un kernel sin Docker.
La referencia local se ejecuta en el dev-workbench ya existente.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import pandas as pd
from comun import ROOT, ejecutar, referencia_python, verificar_csv, sha256, exportar_resumenes

REMOTE = "/tmp/proyecto-etl-cpp"
IMAGE = "etl-distribuido-cpp:local"
CONFIGS = [("cpp_secuencial", 1, "seq"), ("cpp_openmp_1", 1, "omp"),
           ("cpp_openmp_2", 2, "omp"), ("cpp_openmp_4", 4, "omp"),
           ("cpp_openmp_8", 8, "omp"), ("docker_openmp_4", 4, "docker"),
           ("compose_2x2", 4, "compose"), ("kubernetes_2x2", 4, "k8s")]

def k(*args, **kwargs):
    return ejecutar(["docker", "exec", "etl-k3s", "kubectl", "-n", "etl-cpp", *args], **kwargs)

def read_remote(container, remote, local):
    data = ejecutar(["docker", "exec", container, REMOTE + "/bin/etl" if container == "dev-workbench"
                    else "/usr/local/bin/etl", "--read-file", remote], binary=True)
    Path(local).write_bytes(data)

def preparar_runtime():
    ejecutar(["docker", "exec", "dev-workbench", "mkdir", "-p", REMOTE + "/bin", REMOTE + "/datos", REMOTE + "/resultados"])
    for source, target in [(ROOT / "bin/etl", "bin/etl"), (ROOT / "datos/COVID19MEXICO.csv", "datos/COVID19MEXICO.csv")]:
        ejecutar(["docker", "cp", source, "dev-workbench:" + REMOTE + "/" + target])
    ejecutar(["docker", "exec", "dev-workbench", "chmod", "755", REMOTE + "/bin/etl"])

def preparar_kubernetes():
    k("delete", "pod", "etl-cpp-lector", "--ignore-not-found=true", "--wait=true")
    for name in ["almacenamiento.yaml", "lector.yaml"]:
        # kubectl dentro de K3s recibe el manifiesto por stdin; no depende de un
        # kubeconfig del anfitrión ni publica credenciales.
        result = subprocess.run(["docker", "exec", "-i", "etl-k3s", "kubectl", "apply", "-f", "-"],
                                input=(ROOT / "k8s/cpp" / name).read_bytes(), capture_output=True)
        if result.returncode:
            raise RuntimeError(result.stderr.decode())
    k("wait", "--for=condition=Ready", "pod/etl-cpp-lector", "--timeout=180s")

def apply_yaml(name):
    result = subprocess.run(["docker", "exec", "-i", "etl-k3s", "kubectl", "apply", "-f", "-"],
                            input=(ROOT / "k8s/cpp" / name).read_bytes(), capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stderr.decode())
    return result.stdout.decode()

def trial(config, folder, trial_id):
    name, threads, mode = config
    folder.mkdir(parents=True, exist_ok=False)
    start = time.perf_counter()
    if mode in {"seq", "omp"}:
        remote = REMOTE + "/resultados/" + trial_id
        args = [REMOTE + "/bin/etl", "--input", REMOTE + "/datos/COVID19MEXICO.csv",
                "--output", remote + ".csv", "--audit", remote + ".json", "--mode", mode, "--threads", threads]
        log = ejecutar(["docker", "exec", "dev-workbench", *args])
        wall = time.perf_counter() - start
        (folder / "ejecucion.log").write_text(log)
        read_remote("dev-workbench", remote + ".json", folder / "auditoria.json")
        read_remote("dev-workbench", remote + ".csv", folder / "COVID19MEXICO_limpio.csv")
        ejecutar(["docker", "exec", "dev-workbench", "rm", "-f", remote + ".json", remote + ".csv"])
        audit = json.loads((folder / "auditoria.json").read_text())
        workers = [audit]
        kernel, global_time, etl, io_total = (audit[x] for x in ["transformacion_filas_segundos", "preparacion_global_segundos", "etl_segundos", "total_segundos"])
        actual = audit["hilos_utilizados"]
    elif mode == "docker":
        args = ["docker", "run", "--rm", "--network=none", "--cpus=4", "--memory=512m", "--user", f"{os.getuid()}:{os.getgid()}",
                "--mount", f"type=bind,src={folder},dst=/resultados", IMAGE]
        log = ejecutar(args)
        wall = time.perf_counter() - start
        (folder / "ejecucion.log").write_text(log)
        audit = json.loads((folder / "auditoria.json").read_text())
        workers = [audit]
        kernel, global_time, etl, io_total = (audit[x] for x in ["transformacion_filas_segundos", "preparacion_global_segundos", "etl_segundos", "total_segundos"])
        actual = audit["hilos_utilizados"]
    elif mode == "compose":
        env = dict(os.environ, ETL_RESULTS_DIR=str(folder), ETL_UID=str(os.getuid()), ETL_GID=str(os.getgid()))
        cmd = ["docker", "compose", "-f", "compose.cpp.yaml", "-p", "etl-cpp-experimento"]
        try:
            log = ejecutar([*cmd, "up", "--no-build", "--force-recreate", "--no-color"], env=env)
            wall = time.perf_counter() - start
            status_text = ejecutar([*cmd, "ps", "-a", "--format", "json"], env=env)
            (folder / "contenedores.jsonl").write_text(status_text)
            for line in status_text.splitlines():
                item = json.loads(line)
                if item.get("ExitCode") != 0:
                    raise RuntimeError(f"Contenedor fallido: {item.get('Service')}")
            (folder / "ejecucion.log").write_text(log)
        finally:
            ejecutar([*cmd, "down"], env=env)
        workers = [json.loads((folder / f"parte_{i}.json").read_text()) for i in range(2)]
        actual = sum(a["hilos_utilizados"] for a in workers)
        kernel = global_time = etl = io_total = None
    else:
        # Eliminar solamente los Jobs de esta entrega, fuera del tiempo medido.
        k("delete", "jobs", "-l", "app=etl-cpp", "--ignore-not-found=true", "--wait=true")
        start = time.perf_counter()
        logs = [apply_yaml("job-particiones.yaml")]
        logs.append(k("wait", "--for=condition=complete", "job/etl-cpp-particiones", "--timeout=180s"))
        logs.append(apply_yaml("job-union.yaml"))
        logs.append(k("wait", "--for=condition=complete", "job/etl-cpp-union", "--timeout=120s"))
        wall = time.perf_counter() - start
        (folder / "ejecucion.log").write_text("\n".join(logs))
        (folder / "jobs.json").write_text(k("get", "jobs", "-o", "json"))
        (folder / "pods.json").write_text(k("get", "pods", "-o", "json"))
        for file in ["COVID19MEXICO_limpio.csv", "parte_0.json", "parte_1.json", "union.json"]:
            data = k("exec", "etl-cpp-lector", "--", "/usr/local/bin/etl", "--read-file", "/resultados/" + file, binary=True)
            (folder / file).write_bytes(data)
        workers = [json.loads((folder / f"parte_{i}.json").read_text()) for i in range(2)]
        actual = sum(a["hilos_utilizados"] for a in workers)
        kernel = global_time = etl = io_total = None
    if actual != threads:
        raise AssertionError(f"Se solicitaron {threads} hilos; se observaron {actual}")
    audit_total = {key: sum(a[key] for a in workers) for key in ["filas_entrada", "filas_salida", "duplicados_exactos_eliminados", "duplicados_id_eliminados", "edades_invalidas_convertidas_a_nulo"]}
    return {"configuracion": name, "hilos": actual, "pared_segundos": wall,
            "lectura_escritura_total_segundos": io_total, "etl_segundos": etl,
            "global_segundos": global_time, "kernel_segundos": kernel, **audit_total}

def resumir(raw):
    rows = []
    for name, group in raw.groupby("configuracion", sort=False):
        item = {"configuracion": name, "hilos": int(group.hilos.iloc[0]), "repeticiones": len(group)}
        for col in ["pared_segundos", "lectura_escritura_total_segundos", "etl_segundos", "global_segundos", "kernel_segundos"]:
            item[col + "_media"] = group[col].mean()
            item[col + "_mediana"] = group[col].median()
            item[col + "_desviacion"] = group[col].std(ddof=1)
        rows.append(item)
    result = pd.DataFrame(rows).set_index("configuracion").loc[[c[0] for c in CONFIGS if c[0] in set(raw.configuracion)]].reset_index()
    baseline = result[result.configuracion == "cpp_secuencial"].iloc[0]
    for name, time_col in [("pared", "pared_segundos_media"), ("etl", "etl_segundos_media"), ("kernel", "kernel_segundos_media")]:
        result["speedup_" + name] = baseline[time_col] / result[time_col]
        result["eficiencia_" + name + "_porcentaje"] = 100 * result["speedup_" + name] / result.hilos
    return result

def metadata():
    info = json.loads(ejecutar(["docker", "inspect", "dev-workbench"]))[0]
    compiler = ejecutar(["docker", "run", "--rm", "--network=none", "jupyterlab_docker_master-openmp:latest", "g++", "--version"]).splitlines()[0]
    kube_version = json.loads(ejecutar(["docker", "exec", "etl-k3s", "kubectl", "version", "-o", "json"]))["serverVersion"]["gitVersion"]
    docker_version = ejecutar(["docker", "version", "--format", "{{.Server.Version}}"]).strip()
    return {
        "fecha_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_sha256": sha256(ROOT / "datos/COVID19MEXICO.csv"),
        "fuente_cpp_sha256": sha256(ROOT / "cpp/etl.cpp"), "binario_sha256": sha256(ROOT / "bin/etl"),
        "imagen_etl": ejecutar(["docker", "image", "inspect", IMAGE, "--format", "{{.Id}}"]).strip(),
        "compilador": compiler,
        "compilacion": compiler + "; -std=c++17 -O3 -Wall -Wextra -Wpedantic -fopenmp",
        "opciones_compilacion": "-std=c++17 -O3 -Wall -Wextra -Wpedantic -fopenmp",
        "cpu_modelo": next((x.split(":", 1)[1].strip() for x in Path('/proc/cpuinfo').read_text().splitlines() if x.startswith('model name')), "no disponible"),
        "cpu_logicas_anfitrion": os.cpu_count(),
        "workbench_cuota_cpu": info["HostConfig"]["NanoCpus"] / 1e9,
        "workbench_imagen": info["Config"]["Image"],
        "kubernetes_version": kube_version, "kubernetes": kube_version,
        "docker_version": docker_version, "docker": docker_version,
        "pandas_validacion": pd.__version__, "numpy_validacion": __import__('numpy').__version__,
        "recursos": "Workbench y Docker individual: cuota 4 CPU; Compose: 2 contenedores x 2 CPU; K3s: cuota 4 CPU compartida por control plane y Pods. OpenMP 8 hilos sobre cuota 4 implica sobresuscripción.",
        "metodo": "1 calentamiento excluido por configuración; repeticiones en rondas de orden aleatorio reproducible con Fisher-Yates/LCG y semilla 20261110+ronda. Cada ejecución relee y exporta CSV. Validación y recuperación de resultados fuera del cronómetro. Construcción, descarga de imágenes y arranque inicial del clúster excluidos. Caché del SO sin vaciar; escritura cerrada sin fsync.",
        "temporizadores": "ETL interno=preparación global+transformación de filas; núcleo=filas; total interno=lectura+ETL+escritura; pared externa=desde lanzamiento hasta finalización de la salida, incluye orquestación y unión. Pared Workbench incluye docker exec sobre contenedor ya activo.",
        "almacenamiento": "Workbench /tmp en filesystem del contenedor; Docker/Compose bind mount; K3s PVC local-path. Diferencias de almacenamiento y consumo del control plane impiden atribuir toda diferencia solo al orquestador.",
        "particionado": "FNV-1a de ID_REGISTRO antes de recortar textos; conserva juntos IDs repetidos. Cada trabajador lee el CSV completo y transforma su partición. Reductor C++ usa índices originales para recuperar el orden.",
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeticiones", type=int, default=5)
    parser.add_argument("--salida", type=Path, default=ROOT / "resultados_experimentos")
    parser.add_argument("--solo", nargs="*", choices=[x[0] for x in CONFIGS])
    args = parser.parse_args()
    if args.repeticiones < 2:
        parser.error("Se requieren al menos dos repeticiones")
    output = args.salida.resolve(); output.mkdir(parents=True, exist_ok=True)
    configs = [c for c in CONFIGS if not args.solo or c[0] in args.solo]
    if configs[0][0] != "cpp_secuencial":
        parser.error("Incluir cpp_secuencial para calcular la referencia")
    reference, reference_audit = referencia_python(ROOT / "datos/COVID19MEXICO.csv")
    exportar_resumenes(reference, output / "resumenes")
    preparar_runtime()
    if any(c[2] == "k8s" for c in configs):
        preparar_kubernetes()
    meta = metadata()
    meta["repeticiones"] = args.repeticiones
    (output / "entorno.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    (output / "auditoria_referencia_python.json").write_text(json.dumps(reference_audit, indent=2))
    raw = []
    for repetition in range(args.repeticiones + 1):
        order = list(configs)
        # Mismo Fisher-Yates/LCG usado en la ejecución registrada de esta entrega.
        seed = 20261110 + repetition
        for i in range(len(order) - 1, 0, -1):
            seed = (1664525 * seed + 1013904223) & 0xffffffff
            j = int(seed / 4294967296 * (i + 1))
            order[i], order[j] = order[j], order[i]
        for config in order:
            name = config[0]
            trial_id = f"{name}_{repetition:02d}"
            folder = output / "ejecuciones" / trial_id
            row = trial(config, folder, trial_id)
            validation = verificar_csv(folder / "COVID19MEXICO_limpio.csv", reference)
            for key, value in reference_audit.items():
                if row[key] != value:
                    raise AssertionError(f"Auditoría incompatible: {name}, {key}")
            validation["auditoria_coincide_referencia"] = True
            (folder / "validacion.json").write_text(json.dumps(validation, indent=2))
            row.update(repeticion=repetition, calentamiento=repetition == 0, validado=True,
                       salida_sha256=validation["sha256_csv"], carpeta=str(folder.relative_to(ROOT)))
            raw.append(row)
            pd.DataFrame(raw).to_csv(output / "mediciones_todas.csv", index=False)
            print(f"{'Calentamiento' if repetition == 0 else f'Repetición {repetition}'} | {name} | pared={row['pared_segundos']:.4f}s | validado", flush=True)
            # Retener datos finales por configuración y evidencias ligeras de cada repetición.
            if repetition != args.repeticiones:
                for file in folder.glob("*.csv*"):
                    file.unlink()
        measured = pd.DataFrame(raw)
        measured = measured[~measured.calentamiento]
        if not measured.empty:
            measured.to_csv(output / "mediciones.csv", index=False)
            resumir(measured).to_csv(output / "resumen_rendimiento.csv", index=False)
    print("Experimentos completos:", output, flush=True)

if __name__ == "__main__":
    main()
