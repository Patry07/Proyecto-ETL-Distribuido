# Imagen base de Python
FROM python:3.13-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar dependencias
COPY requirements.txt .

# Instalar bibliotecas
RUN pip install --no-cache-dir -r requirements.txt

# Copiar programa ETL
COPY etl_paralelo.py .

# Copiar datos originales
COPY datos/ ./datos/

# Crear carpeta para resultados
RUN mkdir -p resultados

# Ejecutar ETL paralelo
CMD ["python", "etl_paralelo.py"]
