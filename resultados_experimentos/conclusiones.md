## Conclusiones obtenidas de esta ejecución

1. El ETL y la unión se implementaron en **C++17**. OpenMP reparte filas entre hilos; Compose y Kubernetes ejecutan dos particiones distintas con dos hilos por trabajador.

2. Se validaron **40 ejecuciones medidas**, más un calentamiento excluido por configuración. Todas conservaron **108,733 filas y 42 columnas**, coincidieron con la referencia Python y produjeron el mismo CSV C++ ordenado. La auditoría no registró duplicados ni edades fuera de rango en este dataset.

3. La transformación secuencial C++ tardó **0.201135 ± 0.012483 s** (media ± desviación estándar muestral). La mejor configuración OpenMP para el ETL completo fue **4 hilos**, con **0.158650 s**, speedup **1.268×** y eficiencia **31.69 %**.

4. La preparación global representa aproximadamente **74.18 %** del tiempo secuencial de transformación. La Ley de Amdahl estima un límite de **1.348×** para ese alcance, aun con muchos hilos. Al incluir lectura y escritura, la fracción serial estimada asciende a **94.98 %** y el límite ideal baja a **1.053×**.

5. Los tiempos de pared de los despliegues incluyen su lanzamiento y la unión. No se deben confundir con el cronómetro interno del bucle OpenMP:

   - Docker · 4 hilos: **1.1715 ± 0.0644 s**; speedup de pared frente al secuencial en Workbench **0.864×**.

   - Compose · 2 × 2: **2.0269 ± 0.1896 s**; speedup de pared frente al secuencial en Workbench **0.499×**.

   - Kubernetes · 2 × 2: **6.9028 ± 0.5996 s**; speedup de pared frente al secuencial en Workbench **0.147×**.

6. Para esta carga, la lectura, deduplicación y escritura limitan la mejora total. En Compose/Kubernetes cada trabajador vuelve a leer el archivo completo, y la unión añade trabajo serial. Los valores medidos describen este diseño y equipo; no demuestran una ventaja general de un orquestador.

7. Kubernetes se probó con **dos Pods en un único nodo local K3s**. Es paralelismo entre contenedores sobre la misma máquina, con memoria compartida por los hilos de cada proceso. No es una prueba entre varias máquinas físicas.

8. El Workbench tiene cuota de **4 CPU**. La prueba de 8 hilos estudia sobresuscripción; su eficiencia S/p expresa escalamiento relativo y no porcentaje de utilización de CPU. El control plane de K3s comparte el presupuesto de CPU del clúster.

9. Las diferencias con Amdahl son residuos del modelo: incluyen variación del sistema, caché, acceso a memoria y sobrecarga de OpenMP. La fracción serial se obtuvo de fases instrumentadas, no se ajustó artificialmente para hacer coincidir las curvas.