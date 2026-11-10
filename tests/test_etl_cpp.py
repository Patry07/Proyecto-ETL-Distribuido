"""Pruebas de contenido, deduplicación entre particiones y CSV válido/inválido."""
import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from comun import referencia_python, verificar_csv

class EtlCppTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.input = self.dir / "original.csv"
        header = [" id_registro ", " FECHA_INGRESO ", "FECHA_DEF", " EDAD ", "PAIS", "CODIGO"]
        first = ["a", "2024-02-29", "9999-99-99", "033", ' México, "centro"\nSur ', "01"]
        rows = [first, first, ["a", "2024-01-02", "9999-99-99", "40", "otro", "1"],
                ["b", "2023-02-29", "2024-01-03", "121", "México", "2"],
                [" C ", "2024-03-01", "", "", " Perú ", "3"],
                ["C", "fecha inválida", "", "-1", "Chile", "3"],
                ["", "2024-03-02", "", "25", "Colombia", "4"],
                ["", "2024-03-03", "", "26", "Argentina", "4"]]
        with self.input.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f, lineterminator="\r\n")
            writer.writerow(header); writer.writerows(rows)
        self.reference, self.audit = referencia_python(self.input)

    def tearDown(self):
        self.tmp.cleanup()

    def run_etl(self, mode="seq", threads=1, shard=None):
        name = f"parte_{shard}" if shard is not None else f"{mode}_{threads}"
        target = self.dir / (name + ".csv")
        audit = self.dir / (name + ".json")
        args = [ROOT / "bin/etl", "--input", self.input, "--output", target,
                "--audit", audit, "--mode", mode, "--threads", str(threads)]
        if shard is not None:
            args += ["--shard-index", str(shard), "--shard-count", "2"]
        subprocess.run([str(x) for x in args], check=True, capture_output=True)
        return target, json.loads(audit.read_text())

    def test_csv_limpieza_y_equivalencia_openmp(self):
        for mode, threads in [("seq", 1), ("omp", 1), ("omp", 2), ("omp", 4), ("omp", 8)]:
            with self.subTest(mode=mode, threads=threads):
                target, audit = self.run_etl(mode, threads)
                verificar_csv(target, self.reference)
                for key, value in self.audit.items():
                    self.assertEqual(audit[key], value, key)
                self.assertEqual(audit["hilos_utilizados"], threads)

    def test_particiones_con_ids_repetidos_y_union_ordenada(self):
        audits = [self.run_etl("omp", 2, i)[1] for i in range(2)]
        target = self.dir / "reunido.csv"
        subprocess.run([str(ROOT / "bin/etl"), "--merge-dir", str(self.dir), "--shard-count", "2",
                        "--output", str(target), "--audit", str(self.dir / "union.json")],
                       check=True, capture_output=True)
        verificar_csv(target, self.reference)
        for key, value in self.audit.items():
            self.assertEqual(sum(a[key] for a in audits), value, key)

    def test_rechaza_csv_malformado_y_parametros_invalidos(self):
        self.input.write_text('ID_REGISTRO,EDAD\na,"sin cierre\n')
        for options in [["--threads", "0"], ["--shard-index", "2", "--shard-count", "2"], []]:
            result = subprocess.run([str(ROOT / "bin/etl"), "--input", str(self.input), *options], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"ERROR:", result.stderr)

if __name__ == "__main__":
    unittest.main()
