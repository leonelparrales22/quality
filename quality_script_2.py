#!/usr/bin/env python3
"""Quality rules simulation using PySpark and Deequ.

This script mirrors the logic from quality.ipynb so it can be executed from the
terminal without needing Jupyter. It prepares sample parametrization rules,
loads synthetic log records into a managed Spark table, and runs the
``pyspark_transform`` quality engine to simulate Deequ verifications.
"""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    lower,
    trim,
    date_format,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SPARK_APP_NAME = "quality-simulation-script"
DEEQU_JAR_PATH = "/home/leonelparrales/Spark/deequ-2.0.9-spark-3.1.jar"
QUALITY_GROUP_ID = "CAN_DIG_LOGS_NOTIFICADOR"
DATABASE_NAME = "ecbpprq51_repositorio_si"
TABLE_NAME = "LogsNotificador_Pruebas"
WAREHOUSE_DIR = Path("/home/leonelparrales/Documentos/Repositorios/quality/spark-warehouse")
OUTPUT_CSV_PATH = Path("/home/leonelparrales/Documentos/Repositorios/quality/quality_results.csv")
DATA_DOMAIN_NAME = "Canales_Digitales"

SIMULATED_RULES: List[Dict[str, str]] = [
    {
        "id_calidad_tabla": TABLE_NAME,
        "coleccion": DATABASE_NAME,
        "nombre_columna": "remitenteNotificador",
        "id_calidad_regla": "COM_NULOS",
        "parametros": "",
        "filtros": "remitenteNotificador != 'OBSERVADOR'",
        "estado": True,
        "descripcion": "Todo registro debe tener el remitente notificador. No nulos",
        "umbral": 100.0,
        "macrodominio": "CANDIG",
        "dominio": DATA_DOMAIN_NAME,
        "usuario_cambio": "laparral",
        "fecha_cambio": "2026-02-27 17:20:00",
    },
    {
        "id_calidad_tabla": TABLE_NAME,
        "coleccion": DATABASE_NAME,
        "nombre_columna": "guid_transaccion",
        "id_calidad_regla": "COM_DUPLICADOS",
        "parametros": "",
        "filtros": "remitenteNotificador != 'CARENCIA'",
        "estado": True,
        "descripcion": "Cada transacción debe tener un GUID único",
        "umbral": 90.0,
        "macrodominio": "CANDIG",
        "dominio": DATA_DOMAIN_NAME,
        "usuario_cambio": "laparral",
        "fecha_cambio": "2026-02-27 17:20:00",
    },
    {
        "id_calidad_tabla": TABLE_NAME,
        "coleccion": DATABASE_NAME,
        "nombre_columna": "estadoEnvioNotificacion",
        "id_calidad_regla": "VAL_PERMITIDOS",
        "parametros": "success,failed",
        "filtros": "",
        "estado": False,
        "descripcion": "Estados válidos del flujo de notificación",
        "umbral": 90.0,
        "macrodominio": "CANDIG",
        "dominio": DATA_DOMAIN_NAME,
        "usuario_cambio": "laparral",
        "fecha_cambio": "2026-02-27 17:20:00",
    },
]

SAMPLE_LOGS: List[Dict[str, str]] = [
    {
        "estadoEnvioNotificacion": "success",
        "fechaCarga": "2026-02-20 13:45:10",
        "montoTransaccion": 152.35,
        "documentId": "DOC-1001",
        "guid_transaccion": "0f8fad5b-d9cb-469f-a165-70867728950e",
        "cedula_identidad": "0102030405",
        "correoElectronico": "cliente1@banco.com",
        "edad_cliente": 35,
        "remitenteNotificador": "NOTIFICADOR_APP",
    },
    {
        "estadoEnvioNotificacion": "failed",
        "fechaCarga": "2026-02-20 13:46:03",
        "montoTransaccion": -25.0,
        "documentId": "DOC-1002",
        "guid_transaccion": "0f8fad5b-d9cb-469f-a165-70867728950e",
        "cedula_identidad": "123456789",  # longitud incorrecta
        "correoElectronico": "cliente2@banco",
        "edad_cliente": 17,
        "remitenteNotificador": "OBSERVADOR",
    },
    {
        "estadoEnvioNotificacion": "pending",
        "fechaCarga": "2026-02-20 13:46:35",
        "montoTransaccion": 45.12,
        "documentId": None,
        "guid_transaccion": "e902893a-9d22-3c7e-a7b8-d6e313b71d9f",
        "cedula_identidad": "1098765432",
        "correoElectronico": "cliente3@banco.com",
        "edad_cliente": 45,
        "remitenteNotificador": "NOTIFICADOR_APP",
    },
    {
        "estadoEnvioNotificacion": "success",
        "fechaCarga": "2026-02-20 13:48:20",
        "montoTransaccion": 3100.75,
        "documentId": "DOC-1004",
        "guid_transaccion": "16fd2706-8baf-433b-82eb-8c7fada847da",
        "cedula_identidad": "1231231231",
        "correoElectronico": "cliente4@banco.com",
        "edad_cliente": 101,
        "remitenteNotificador": "CARENCIA",
    },
    {
        "estadoEnvioNotificacion": "success",
        "fechaCarga": "2026-02-20 13:49:02",
        "montoTransaccion": 8.99,
        "documentId": "DOC-1005",
        "guid_transaccion": "9a499d64-72e0-4365-9f28-f7ed05f9b2f0",
        "cedula_identidad": "5556667778",
        "correoElectronico": "cliente5@banco.com",
        "edad_cliente": 60,
        "remitenteNotificador": "NOTIFICADOR_APP",
    },
    {
        "estadoEnvioNotificacion": "unknown",
        "fechaCarga": "2026-02-20 13:52:10",
        "montoTransaccion": 75.0,
        "documentId": "DOC-1006",
        "guid_transaccion": "9a499d64-72e0-4365-9f28-f7ed05f9b2f0",
        "cedula_identidad": "9876543210",
        "correoElectronico": "cliente6@banco.com",
        "edad_cliente": 90,
        "remitenteNotificador": "NOTIFICADOR_APP",
    },
]


# -----------------------------------------------------------------------------
# Spark helpers
# -----------------------------------------------------------------------------
def build_spark_session() -> SparkSession:
    """Create (or reuse) a SparkSession configured with the Deequ JAR."""

    return (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .config("spark.jars", DEEQU_JAR_PATH)
        .getOrCreate()
    )


def cleanup_table_location(database: str, table: str) -> None:
    """Remove the managed table directory to avoid LOCATION_ALREADY_EXISTS."""

    table_path = WAREHOUSE_DIR / f"{database}.db" / table.lower()
    if table_path.exists():
        shutil.rmtree(table_path)
        print(f"Directorio eliminado: {table_path}")
    else:
        print(f"Directorio no encontrado (ya limpio): {table_path}")


def prepare_example_table(spark: SparkSession, data: DataFrame) -> None:
    """Create the example table backing the rules dataset."""

    cleanup_table_location(DATABASE_NAME, TABLE_NAME)
    spark.sql(f"DROP TABLE IF EXISTS {DATABASE_NAME}.{TABLE_NAME}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    data.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.{TABLE_NAME}")
    print(f"Tabla de ejemplo actualizada: {DATABASE_NAME}.{TABLE_NAME}")


# -----------------------------------------------------------------------------
# Quality engine (ported from the notebook)
# -----------------------------------------------------------------------------
def pyspark_transform(spark, input_data, param_dict):
    from typing import Dict, Iterable, List, Tuple

    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.functions import col, trim, lower, current_date, current_timestamp
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DoubleType,
        LongType,
        DateType,
        TimestampType,
    )
    from pyspark import StorageLevel

    output_schema = StructType(
        [
            StructField("periodo", DateType(), True),
            StructField("fecha_proceso", TimestampType(), True),
            StructField("id_calidad_tabla", StringType(), True),
            StructField("columna", StringType(), True),
            StructField("parametros", StringType(), True),
            StructField("umbral", DoubleType(), True),
            StructField("estado_calidad", StringType(), True),
            StructField("total_registros", LongType(), True),
            StructField("registros_fallidos", LongType(), True),
            StructField("porcentaje_cumplimiento", DoubleType(), True),
            StructField("observacion_tecnica", StringType(), True),
        ]
    )

    def get_standardized_rules(raw_input) -> DataFrame:
        source_df = (
            raw_input.get("sql_parametria_quality", list(raw_input.values())[0])
            if isinstance(raw_input, dict)
            else raw_input
        )
        return source_df.select(
            trim(col("id_calidad_tabla")).alias("table_name"),
            trim(col("coleccion")).alias("schema"),
            trim(col("nombre_columna")).alias("column_name"),
            trim(col("id_calidad_regla")).alias("rule_code"),
            trim(col("parametros")).alias("params"),
            trim(col("filtros")).alias("filters"),
            col("estado").cast("boolean").alias("is_active"),
            trim(col("descripcion")).alias("description"),
            col("umbral").cast("double").alias("threshold"),
            trim(col("macrodominio")).alias("macro_domain"),
            trim(col("dominio")).alias("data_domain"),
            trim(col("usuario_cambio")).alias("changed_by"),
            trim(col("fecha_cambio")).alias("changed_at"),
        )

    def get_catalog(raw_input) -> DataFrame:
        catalog_df = (
            raw_input.get("sql_parametria_quality_catalogo")
            if isinstance(raw_input, dict)
            else None
        )
        if catalog_df is None:
            empty_schema = StructType(
                [
                    StructField("id_calidad_regla", StringType(), True),
                    StructField("dimension", StringType(), True),
                    StructField("descripcion", StringType(), True),
                    StructField("metodo", StringType(), True),
                ]
            )
            return spark.createDataFrame([], empty_schema)

        return catalog_df.select(
            trim(col("id_calidad_regla")).alias("id_calidad_regla"),
            trim(col("dimension")).alias("dimension"),
            trim(col("descripcion")).alias("catalog_description"),
            trim(col("metodo")).alias("metodo"),
        )

    def evaluate_rule(rule_code: str, df: DataFrame, column: str, params: str) -> Tuple[int, str]:
        if rule_code == "COM_NULOS":
            passed = df.filter(col(column).isNotNull()).count()
            return passed, ""
        if rule_code == "COM_DUPLICADOS":
            value_counts = df.groupBy(col(column)).count()
            unique_rows = (
                value_counts.filter(col("count") == 1)
                .agg(F.sum("count"))
                .collect()[0][0]
            )
            passed = int(unique_rows) if unique_rows is not None else 0
            return passed, ""
        if rule_code == "VAL_PERMITIDOS":
            allowed = [v.strip() for v in str(params).split(",") if v.strip()]
            if not allowed:
                return 0, "Parámetros vacíos para VAL_PERMITIDOS"
            passed = df.filter(col(column).isin(allowed)).count()
            return passed, ""
        return None, f"Regla {rule_code} no soportada en la simulación"

    standardized_rules = get_standardized_rules(input_data)
    catalog_df = get_catalog(input_data)

    if catalog_df.count() == 0:
        rules_df = standardized_rules
    else:
        rules_df = standardized_rules.join(
            catalog_df, standardized_rules["rule_code"] == catalog_df["id_calidad_regla"], "left"
        )

    target_table = param_dict.get("dataset", TABLE_NAME)
    rules_df = rules_df.filter((col("is_active") == True) & (col("table_name") == target_table))

    active_rules = rules_df.collect()
    if not active_rules:
        return spark.createDataFrame([], schema=output_schema)

    datasets: Dict[Tuple[str, str], List] = {}
    for rule in active_rules:
        datasets.setdefault((rule["schema"], rule["table_name"]), []).append(rule)

    result_rows: List[Tuple] = []

    for (schema_name, table_name), rules in datasets.items():
        full_table_name = f"{schema_name}.{table_name}"
        df_data = spark.table(full_table_name)
        df_data.persist(StorageLevel.MEMORY_AND_DISK)

        for rule in rules:
            rule_dict = rule.asDict()
            column = rule_dict["column_name"]
            filters = rule_dict["filters"]
            threshold = rule_dict["threshold"] or 0.0
            params = rule_dict["params"] or ""
            rule_code = rule_dict["rule_code"]
            dimension = rule_dict.get("dimension")
            metodo = rule_dict.get("metodo")

            scoped_df = df_data if not filters else df_data.filter(filters)
            scoped_df = scoped_df.select("*")
            scoped_df.persist(StorageLevel.MEMORY_ONLY)

            total_records = scoped_df.count()

            if total_records == 0:
                porcentaje = 100.0
                registros_fallidos = 0
                estado_calidad = "CUMPLE"
                observacion = (
                    "Sin registros después de aplicar filtros; regla considerada cumplida."
                )
            else:
                passed_records, warning = evaluate_rule(
                    rule_code, scoped_df, column, params
                )

                if passed_records is None:
                    porcentaje = 0.0
                    registros_fallidos = total_records
                    estado_calidad = "NO_EVALUADA"
                    observacion = warning
                else:
                    porcentaje = (passed_records / total_records) * 100
                    registros_fallidos = total_records - passed_records
                    estado_calidad = "CUMPLE" if porcentaje >= threshold else "NO_CUMPLE"
                    observacion = (
                        "Regla cumplida exitosamente."
                        if estado_calidad == "CUMPLE"
                        else (
                            f"Alerta: Solo el {porcentaje:.2f}% de los datos cumple la regla. "
                            f"Se exigía el {threshold}%."
                        )
                    )
                    if warning:
                        observacion = f"{observacion} {warning}".strip()

            result_rows.append(
                (
                    None,
                    None,
                    table_name,
                    column,
                    params,
                    threshold,
                    estado_calidad,
                    total_records,
                    registros_fallidos,
                    porcentaje,
                    observacion,
                )
            )

            scoped_df.unpersist()

        df_data.unpersist()

    if not result_rows:
        return spark.createDataFrame([], schema=output_schema)

    return (
        spark.createDataFrame(result_rows, schema=output_schema)
        .withColumn("periodo", current_date())
        .withColumn("fecha_proceso", current_timestamp())
    )


# -----------------------------------------------------------------------------
# Main entrypoint
# -----------------------------------------------------------------------------
def main() -> None:
    spark = build_spark_session()

    try:
        print("Construyendo DataFrame de parametrización...")
        pys_ejecucion_reglas_calidad = spark.createDataFrame(SIMULATED_RULES)
        pys_ejecucion_reglas_calidad.show(truncate=False)

        print("\nConstruyendo DataFrame de logs de ejemplo...")
        ejemplo_datos = spark.createDataFrame(SAMPLE_LOGS)
        ejemplo_datos.show(truncate=False)

        print("\nPreparando tabla administrada de ejemplo...")
        prepare_example_table(spark, ejemplo_datos)

        print("\nEjecutando motor de calidad...")
        resultado_calidad = pyspark_transform(
            spark=spark,
            input_data={"sql_parametria_quality": pys_ejecucion_reglas_calidad},
            param_dict={"dataset": TABLE_NAME},
        )
        resultado_calidad.show(truncate=False)

        print(f"\nGuardando resultados en {OUTPUT_CSV_PATH} ...")
        resultado_calidad_for_csv = (
            resultado_calidad.withColumn(
                "periodo", date_format(col("periodo"), "yyyy-MM-dd")
            )
            .withColumn(
                "fecha_proceso",
                date_format(col("fecha_proceso"), "yyyy-MM-dd HH:mm:ss.SSS"),
            )
        )
        resultado_calidad_for_csv.toPandas().to_csv(OUTPUT_CSV_PATH, index=False)
        print("Archivo CSV generado correctamente.")
    finally:
        print("\nFinalizando SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
