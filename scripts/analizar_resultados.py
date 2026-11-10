"""Valida las ejecuciones registradas y calcula métricas sin acceso al daemon."""
import argparse
import json
from pathlib import Path
import pandas as pd
from comun import ROOT, referencia_python, verificar_csv, exportar_resumenes
from experimentos import resumir

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--salida", type=Path, default=ROOT / "resultados_experimentos")
    args = parser.parse_args()
    output = args.salida.resolve()
    reference, expected = referencia_python(ROOT / "datos/COVID19MEXICO.csv")
    exportar_resumenes(reference, output / "resumenes")
    (output / "auditoria_referencia_python.json").write_text(json.dumps(expected, indent=2))
    records = []
    for path in sorted((output / "ejecuciones").glob("*/lanzamiento.json")):
        folder = path.parent
        launch = json.loads(path.read_text())
        mode = launch["modo"]
        if mode in {"compose", "k8s"}:
            audits = [json.loads((folder / f"parte_{i}.json").read_text()) for i in range(2)]
            kernel = global_time = etl = total = None
        else:
            audits = [json.loads((folder / "auditoria.json").read_text())]
            audit = audits[0]
            kernel, global_time, etl, total = [audit[x] for x in
                ["transformacion_filas_segundos", "preparacion_global_segundos", "etl_segundos", "total_segundos"]]
        combined = {key: sum(a[key] for a in audits) for key in expected}
        if combined != expected:
            raise AssertionError(f"Auditoría incompatible: {folder.name}: {combined}")
        threads = sum(a["hilos_utilizados"] for a in audits)
        if threads != launch["hilos"]:
            raise AssertionError("Número de hilos no coincide")
        validation_path = folder / "validacion.json"
        if validation_path.exists():
            validation = json.loads(validation_path.read_text())
        else:
            validation = verificar_csv(folder / "COVID19MEXICO_limpio.csv", reference)
            validation["auditoria_coincide_referencia"] = True
            validation_path.write_text(json.dumps(validation, indent=2))
        row = {"configuracion": launch["configuracion"], "hilos": threads,
               "repeticion": launch["repeticion"], "calentamiento": launch["repeticion"] == 0,
               "pared_segundos": launch["pared_segundos"],
               "lectura_escritura_total_segundos": total, "etl_segundos": etl,
               "global_segundos": global_time, "kernel_segundos": kernel,
               **combined, "validado": True, "salida_sha256": validation["sha256_csv"],
               "carpeta": str(folder.relative_to(ROOT))}
        records.append(row)
        if launch["repeticion"] != 5:
            for file in folder.glob("*.csv*"):
                file.unlink()
    raw = pd.DataFrame(records)
    raw.to_csv(output / "mediciones_todas.csv", index=False)
    measured = raw[~raw.calentamiento]
    measured.to_csv(output / "mediciones.csv", index=False)
    if not measured.empty:
        summary = resumir(measured)
        summary.to_csv(output / "resumen_rendimiento.csv", index=False)
        print(summary[["configuracion", "repeticiones", "pared_segundos_media", "etl_segundos_media", "speedup_etl"]].to_string(index=False))
    print(f"Validaciones completas: {len(raw)}; medidas sin calentamiento: {len(measured)}")

if __name__ == "__main__":
    main()
