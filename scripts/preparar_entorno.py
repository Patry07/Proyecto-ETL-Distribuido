"""Construye la imagen con el entorno existente e instala un K3s de laboratorio.

No detiene ni recrea dev-workbench, hpc-openmp ni los demás servicios del usuario.
Requiere Docker en el anfitrión. Crea únicamente etl-k3s y la imagen ETL.
"""
import json
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "etl-distribuido-cpp:local"
K3S_IMAGE = "rancher/k3s:v1.34.13-k3s1"

def run(args, **kwargs):
    return subprocess.run([str(x) for x in args], cwd=ROOT, check=True, **kwargs)

def main():
    if not (ROOT / "datos/COVID19MEXICO.csv").is_file():
        raise SystemExit("Colocar COVID19MEXICO.csv en datos/ antes de continuar")
    run(["docker", "inspect", "dev-workbench"], stdout=subprocess.DEVNULL)
    run(["docker", "build", "--network=none", "-f", "Dockerfile.cpp", "-t", IMAGE, "."])
    name = "etl-extraer-" + uuid.uuid4().hex[:8]
    (ROOT / "bin").mkdir(exist_ok=True)
    run(["docker", "create", "--name", name, IMAGE, "--help"], stdout=subprocess.DEVNULL)
    try:
        run(["docker", "cp", name + ":/usr/local/bin/etl", ROOT / "bin/etl"])
    finally:
        run(["docker", "rm", name], stdout=subprocess.DEVNULL)
    state = subprocess.run(["docker", "inspect", "etl-k3s"], capture_output=True, text=True)
    if state.returncode:
        run(["docker", "run", "-d", "--name", "etl-k3s", "--label", "proyecto=etl-cpp", "--privileged",
             "--cpus=4", "--memory=1800m", "--tmpfs", "/run", "--tmpfs", "/var/run", K3S_IMAGE,
             "server", "--disable", "traefik", "--disable", "servicelb", "--disable", "metrics-server",
             "--write-kubeconfig-mode=600"])
    else:
        current = json.loads(state.stdout)[0]
        if current.get("Config", {}).get("Labels", {}).get("proyecto") != "etl-cpp":
            raise SystemExit("Existe un contenedor etl-k3s ajeno a esta entrega; elegir otro nombre")
        if not current["State"]["Running"]:
            run(["docker", "start", "etl-k3s"])
    deadline = time.monotonic() + 180
    while True:
        ready = subprocess.run(["docker", "exec", "etl-k3s", "kubectl", "wait", "--for=condition=Ready",
                                "nodes", "--all", "--timeout=5s"], capture_output=True)
        if ready.returncode == 0:
            break
        if time.monotonic() > deadline:
            raise SystemExit("K3s no está listo: revisar docker logs etl-k3s")
        time.sleep(2)
    with tempfile.TemporaryDirectory(prefix="etl-imagen-") as tmp:
        archive = Path(tmp) / "etl.tar"
        run(["docker", "image", "save", "-o", archive, IMAGE])
        run(["docker", "cp", archive, "etl-k3s:/tmp/etl-imagen.tar"])
        run(["docker", "exec", "etl-k3s", "ctr", "images", "import", "/tmp/etl-imagen.tar"])
        run(["docker", "exec", "etl-k3s", "rm", "-f", "/tmp/etl-imagen.tar"])
    print("Imagen y clúster listos. Ejecutar scripts/experimentos.py desde el anfitrión.")

if __name__ == "__main__":
    main()
