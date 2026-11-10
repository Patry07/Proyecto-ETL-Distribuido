"""Referencia de los avances originales y comprobación del ETL C++."""
from __future__ import annotations
import ast
import hashlib
import json
from pathlib import Path
import subprocess
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
FECHAS = ["FECHA_ACTUALIZACION", "FECHA_INGRESO", "FECHA_SINTOMAS", "FECHA_DEF"]

def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def ejecutar(args, *, log=None, env=None, timeout=240, binary=False):
    result = subprocess.run([str(x) for x in args], cwd=ROOT, env=env,
                            capture_output=True, text=not binary, timeout=timeout)
    if log:
        Path(log).write_text(result.stdout + result.stderr, encoding="utf-8")
    if result.returncode:
        message = result.stderr if not binary else result.stderr.decode(errors="replace")
        raise RuntimeError(f"Falló {args[:4]} (código {result.returncode}):\n{message[-6000:]}")
    return result.stdout

def referencia_python(path):
    """Ejecuta solamente la función original del notebook 01, sin modificarla."""
    notebook = json.loads((ROOT / "01_ETL_Secuencial.ipynb").read_text())
    text = "\n".join("".join(c.get("source", [])) for c in notebook["cells"] if c["cell_type"] == "code")
    tree = ast.parse(text)
    nodes = [n for n in tree.body if
             isinstance(n, ast.FunctionDef) and n.name == "transformar_datos" or
             isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "COLUMNAS_FECHA" for t in n.targets)]
    namespace = {"pd": pd, "np": np}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "referencia_original_01", "exec"), namespace)
    frame = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    result, audit = namespace["transformar_datos"](frame)
    return result.reset_index(drop=True), audit

def verificar_csv(path, reference):
    result = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    for col in FECHAS:
        if col in result:
            result[col] = pd.to_datetime(result[col], errors="coerce")
    # CSV no conserva tipos internos de pandas. Se comparan valores, columnas,
    # orden de filas y posición de nulos, admitiendo int/float equivalentes.
    pd.testing.assert_frame_equal(reference, result.reset_index(drop=True), check_dtype=False)
    return {"filas": len(result), "columnas": len(result.columns),
            "coincide_referencia_python": True, "sha256_csv": sha256(path)}

def exportar_resumenes(frame, folder):
    folder = Path(folder); folder.mkdir(parents=True, exist_ok=True)
    for col, name in [("CLASIFICACION_FINAL_COVID", "covid"), ("CLASIFICACION_FINAL_FLU", "influenza")]:
        if col in frame:
            frame[col].value_counts(dropna=False).sort_index().rename_axis("codigo").reset_index(name="registros").to_csv(
                folder / f"resumen_clasificacion_{name}.csv", index=False)
    if "FECHA_INGRESO" in frame:
        frame["FECHA_INGRESO"].dt.to_period("M").value_counts(dropna=False).sort_index().rename_axis(
            "mes_ingreso").reset_index(name="registros").to_csv(folder / "resumen_mensual_ingresos.csv", index=False)
