# Ejecución de `quality_script_2.py` con Miniconda

Esta guía explica cómo preparar un entorno basado en Miniconda (desde cero) para ejecutar el motor de calidad `quality_script_2.py`, que combina PySpark y Apache Deequ para evaluar reglas de datos parametrizadas.

## 1. Prerrequisitos del sistema

- Linux (probado en Ubuntu/Pop!_OS).
- Acceso a internet para descargar Miniconda, dependencias y el JAR de Deequ.
- Permisos para instalar paquetes en `$HOME`.

## 2. Instalar Miniconda (si aún no está presente)

```bash
cd ~
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
# Acepte la licencia, elija el directorio (por ejemplo, ~/miniconda3) y permita que conda inicialice su shell.
source ~/miniconda3/bin/activate
```

## 3. Crear el entorno para PySpark + Deequ

```bash
conda create -n spark33 python=3.10 openjdk=11 -y
conda activate spark33
pip install pyspark==3.3.2 pydeequ==1.3.0 pandas==2.2.1
```

> **Nota:** PyDeequ 1.3.0 es compatible con Spark 3.3.x. El script ya referencia la ruta del JAR (`/home/leonelparrales/Spark/deequ-2.0.9-spark-3.1.jar`). Si usas otra ruta, actualiza la constante `DEEQU_JAR_PATH` dentro del archivo.

Descarga el JAR y colócalo en la carpeta del proyecto (si todavía no existe):

```bash
cd /home/leonelparrales/Spark
wget https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.9-spark-3.1/deequ-2.0.9-spark-3.1.jar
```

## 4. Configurar variables de entorno clave

Asegúrate de que `JAVA_HOME` apunte al Java 11 del entorno:

```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
```

(Agrega la línea anterior a tu `~/.bashrc` si quieres que se aplique en futuras sesiones.)

## 5. Ejecutar `quality_script_2.py`

1. Ubícate en la carpeta del proyecto:
   ```bash
   cd /home/leonelparrales/Spark
   ```
2. Activa el entorno (si aún no lo hiciste):
   ```bash
   conda activate spark33
   ```
3. Lanza el script:
   ```bash
   python quality_script_2.py
   ```

El programa realizará lo siguiente automáticamente:
- Construirá la parametrización de reglas y los datos de ejemplo.
- Cargará los registros en una tabla Spark gestionada.
- Ejecutará el motor de calidad (PySpark + Deequ) en una sola pasada.
- Mostrará en consola la tabla resultante de métricas de calidad.
- Guardará un CSV con los resultados en `/home/leonelparrales/Spark/quality_results.csv`.

## 6. Personalización (opcional)

- **Rutas y dominios:** Ajusta las constantes del bloque "Configuration" dentro del script (`quality_script_2.py`) para apuntar a tus bases de datos, dominios de negocio o ruta del JAR.
- **Reglas de negocio:** Modifica la lista `SIMULATED_RULES` para mapear tus reglas reales y sus parámetros (incluyendo SLA, severidad y dominio funcional).

Con estos pasos tendrás un entorno replicable y listo para ejecutar el motor de calidad desde cualquier máquina Linux sin instalaciones previas de Miniconda.
