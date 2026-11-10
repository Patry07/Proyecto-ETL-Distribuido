# Reutiliza el entorno C/C++ y OpenMP que ya existe en este equipo.
ARG ETL_CPP_BASE=jupyterlab_docker_master-openmp:latest
FROM ${ETL_CPP_BASE} AS build
WORKDIR /opt/etl
COPY cpp/etl.cpp cpp/etl.cpp
RUN g++ -std=c++17 -O3 -Wall -Wextra -Wpedantic -fopenmp cpp/etl.cpp -o /usr/local/bin/etl
FROM scratch
COPY --from=build /usr/local/bin/etl /usr/local/bin/etl
COPY --from=build /lib/x86_64-linux-gnu/libstdc++.so.6 /lib/x86_64-linux-gnu/libstdc++.so.6
COPY --from=build /lib/x86_64-linux-gnu/libgcc_s.so.1 /lib/x86_64-linux-gnu/libgcc_s.so.1
COPY --from=build /lib/x86_64-linux-gnu/libgomp.so.1 /lib/x86_64-linux-gnu/libgomp.so.1
COPY --from=build /lib/x86_64-linux-gnu/libm.so.6 /lib/x86_64-linux-gnu/libm.so.6
COPY --from=build /lib/x86_64-linux-gnu/libc.so.6 /lib/x86_64-linux-gnu/libc.so.6
COPY --from=build /lib64/ld-linux-x86-64.so.2 /lib64/ld-linux-x86-64.so.2
COPY datos/COVID19MEXICO.csv /datos/COVID19MEXICO.csv
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/etl"]
CMD ["--input", "/datos/COVID19MEXICO.csv", "--output", "/resultados/COVID19MEXICO_limpio.csv", "--audit", "/resultados/auditoria.json", "--mode", "omp", "--threads", "4"]
