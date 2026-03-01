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
WAREHOUSE_DIR = Path("/home/leonelparrales/Spark/spark-warehouse")
OUTPUT_CSV_PATH = Path("/home/leonelparrales/Spark/quality_results.csv")
DATA_DOMAIN_NAME = "Canales_Digitales"

SIMULATED_RULES: List[Dict[str, str]] = [
    {
        "id_grupo_calidad": QUALITY_GROUP_ID,
        "coleccion": DATABASE_NAME,
        "tabla": TABLE_NAME,
        "tipo_regla": "SIN_NULOS",
        "nombre_columna": "documentId",
        "parametros": "",
        "gravedad": "CRITICO",
        "estado": True,
        "descripcion_negocio": "Todo documento debe estar informado",
        "umbral_minimo_sla": 99.0,
        "dominio_datos": DATA_DOMAIN_NAME,
    },
    {
        "id_grupo_calidad": QUALITY_GROUP_ID,
        "coleccion": DATABASE_NAME,
        "tabla": TABLE_NAME,
        "tipo_regla": "DUPLICADOS",
        "nombre_columna": "guid_transaccion",
        "parametros": "",
        "gravedad": "CRITICO",
        "estado": True,
        "descripcion_negocio": "Cada transacción debe tener un GUID único",
        "umbral_minimo_sla": 100.0,
        "dominio_datos": DATA_DOMAIN_NAME,
    },
    {
        "id_grupo_calidad": QUALITY_GROUP_ID,
        "coleccion": DATABASE_NAME,
        "tabla": TABLE_NAME,
        "tipo_regla": "VALORES_PERMITIDOS",
        "nombre_columna": "estadoEnvioNotificacion",
        "parametros": "success,failed",
        "gravedad": "ADVERTENCIA",
        "estado": True,
        "descripcion_negocio": "Estados válidos del flujo de notificación",
        "umbral_minimo_sla": 98.5,
        "dominio_datos": DATA_DOMAIN_NAME,
    },
    {
        "id_grupo_calidad": QUALITY_GROUP_ID,
        "coleccion": DATABASE_NAME,
        "tabla": TABLE_NAME,
        "tipo_regla": "SOLO_POSITIVOS",
        "nombre_columna": "montoTransaccion",
        "parametros": "",
        "gravedad": "ADVERTENCIA",
        "estado": True,
        "descripcion_negocio": "Los montos no pueden ser negativos",
        "umbral_minimo_sla": 99.5,
        "dominio_datos": DATA_DOMAIN_NAME,
    },
    {
        "id_grupo_calidad": QUALITY_GROUP_ID,
        "coleccion": DATABASE_NAME,
        "tabla": TABLE_NAME,
        "tipo_regla": "LONGITUD_EXACTA",
        "nombre_columna": "cedula_identidad",
        "parametros": "10",
        "gravedad": "ADVERTENCIA",
        "estado": True,
        "descripcion_negocio": "La cédula debe tener 10 dígitos",
        "umbral_minimo_sla": 99.0,
        "dominio_datos": DATA_DOMAIN_NAME,
    },
    {
        "id_grupo_calidad": QUALITY_GROUP_ID,
        "coleccion": DATABASE_NAME,
        "tabla": TABLE_NAME,
        "tipo_regla": "FORMATO_REGEX",
        "nombre_columna": "correoElectronico",
        "parametros": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$",
        "gravedad": "ADVERTENCIA",
        "estado": True,
        "descripcion_negocio": "El correo debe cumplir con el formato estándar",
        "umbral_minimo_sla": 97.0,
        "dominio_datos": DATA_DOMAIN_NAME,
    },
    {
        "id_grupo_calidad": QUALITY_GROUP_ID,
        "coleccion": DATABASE_NAME,
        "tabla": TABLE_NAME,
        "tipo_regla": "RANGO_VALORES",
        "nombre_columna": "edad_cliente",
        "parametros": "18,100",
        "gravedad": "ADVERTENCIA",
        "estado": True,
        "descripcion_negocio": "La edad del cliente debe estar entre 18 y 100 años",
        "umbral_minimo_sla": 96.0,
        "dominio_datos": DATA_DOMAIN_NAME,
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
def pyspark_transform(spark: SparkSession, input_data: Dict, param_dict: Dict) -> DataFrame:
    """Quality engine that orchestrates Deequ checks based on parametrization."""

    os.environ["SPARK_VERSION"] = "3.2"

    import pydeequ  # noqa: F401 - validates availability
    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationResult, VerificationSuite

    DIMENSION_BY_RULE = {
        "SIN_NULOS": "Completitud",
        "DUPLICADOS": "Unicidad",
        "VALORES_PERMITIDOS": "Validez",
        "SOLO_POSITIVOS": "Validez",
        "LONGITUD_EXACTA": "Conformidad",
        "FORMATO_REGEX": "Conformidad",
        "RANGO_VALORES": "Validez",
    }

    RULE_REGISTRY = {
        "SIN_NULOS": lambda check, column, params: check.isComplete(column),
        "DUPLICADOS": lambda check, column, params: check.isUnique(column),
        "VALORES_PERMITIDOS": lambda check, column, params: (
            check.isContainedIn(
                column,
                [v.strip() for v in str(params).split(",") if v.strip()],
            )
            if params
            else None
        ),
        "SOLO_POSITIVOS": lambda check, column, params: check.isNonNegative(column),
        "LONGITUD_EXACTA": lambda check, column, params: (
            check.satisfies(
                f"length({column}) = {params}", f"length({column}) = {params}"
            )
            if params
            else None
        ),
        "FORMATO_REGEX": lambda check, column, params: (
            check.hasPattern(column, params) if params else None
        ),
        "RANGO_VALORES": lambda check, column, params: (
            check.satisfies(
                f"{column} >= {params.split(',')[0]} AND {column} <= {params.split(',')[1]}",
                f"{column} >= {params.split(',')[0]} AND {column} <= {params.split(',')[1]}",
            )
            if params and "," in params
            else None
        ),
    }

    def metric_keys_for_rule(rule: Dict[str, str]) -> List[Tuple[str, str | None]]:
        column = rule["column_name"]
        rule_type = rule["rule_type"]
        params = rule["params"]

        if rule_type == "SIN_NULOS":
            return [("Completeness", column)]
        if rule_type == "DUPLICADOS":
            return [("Uniqueness", column)]
        if rule_type in {"VALORES_PERMITIDOS", "SOLO_POSITIVOS", "FORMATO_REGEX"}:
            return [("Compliance", column)]
        if rule_type == "LONGITUD_EXACTA":
            return [("Compliance", f"length({column}) = {params}")]
        if rule_type == "RANGO_VALORES":
            if params and "," in params:
                lower_val, upper_val = params.split(",")
                return [
                    (
                        "Compliance",
                        f"{column} >= {lower_val} AND {column} <= {upper_val}",
                    )
                ]
        return []

    output_schema = StructType(
        [
            StructField("periodo", DateType(), True),
            StructField("fecha_proceso", TimestampType(), True),
            StructField("id_grupo_calidad", StringType(), True),
            StructField("nombre_tabla", StringType(), True),
            StructField("dominio_datos", StringType(), True),
            StructField("nombre_columna", StringType(), True),
            StructField("dimension_calidad", StringType(), True),
            StructField("tipo_regla", StringType(), True),
            StructField("parametros", StringType(), True),
            StructField("gravedad", StringType(), True),
            StructField("descripcion_negocio", StringType(), True),
            StructField("umbral_minimo_sla", DoubleType(), True),
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
            trim(col("id_grupo_calidad")).alias("group_id"),
            trim(col("coleccion")).alias("schema"),
            trim(col("tabla")).alias("table"),
            trim(col("tipo_regla")).alias("rule_type"),
            trim(col("nombre_columna")).alias("column_name"),
            trim(col("parametros")).alias("params"),
            trim(col("gravedad")).alias("severity"),
            trim(col("descripcion_negocio")).alias("business_description"),
            col("umbral_minimo_sla").cast("double").alias("sla_threshold"),
            trim(col("dominio_datos")).alias("data_domain"),
            trim(lower(col("estado").cast("string"))).alias("is_active_str"),
        )

    def process_single_dataset(
        schema_name: str, table_name: str, dataset_rules: Iterable
    ) -> DataFrame | None:
        full_table_name = f"{schema_name}.{table_name}"
        try:
            required_cols = {rule["column_name"] for rule in dataset_rules}
            if required_cols:
                query = f"SELECT {', '.join(required_cols)} FROM {full_table_name}"
            else:
                query = f"SELECT * FROM {full_table_name}"

            df_data = spark.sql(query)

            # Subir a memoria para evitar doble lectura
            df_data.persist(StorageLevel.MEMORY_AND_DISK)

            total_records = df_data.count()

            suite = VerificationSuite(spark).onData(df_data)

            dataset_rules_dicts = [rule.asDict() for rule in dataset_rules]
            constraint_plans = []

            for idx, rule in enumerate(dataset_rules_dicts):
                rule_type = rule["rule_type"]
                constraint_fn = RULE_REGISTRY.get(rule_type)
                if not constraint_fn:
                    continue

                check_level = (
                    CheckLevel.Error
                    if str(rule["severity"]).lower() == "error"
                    else CheckLevel.Warning
                )
                check_name = f"{rule_type}:{rule['column_name']}:{idx}"
                check_obj = Check(spark, check_level, check_name)

                constraint_result = constraint_fn(
                    check_obj, rule["column_name"], rule["params"]
                )
                if constraint_result is None:
                    continue

                suite.addCheck(check_obj)
                constraint_plans.append(
                    {
                        "rule": rule,
                        "check_name": check_name,
                    }
                )

            if not constraint_plans:
                df_data.unpersist()
                return None

            verification_result = suite.run()

            metrics_map: Dict[tuple, float] = {}
            for metric in VerificationResult.successMetricsAsDataFrame(
                spark, verification_result
            ).collect():
                metrics_map[(metric["name"], metric["instance"])] = metric["value"]

            check_map: Dict[str, Tuple[str, str]] = {}
            for result in VerificationResult.checkResultsAsDataFrame(
                spark, verification_result
            ).collect():
                check_map[result["check"]] = (
                    result["constraint_status"],
                    result["constraint_message"],
                )

            mapped_results: List[Tuple] = []
            for plan in constraint_plans:
                rule = plan["rule"]
                check_name = plan["check_name"]
                status, message = check_map.get(
                    check_name, ("Unknown", "Regla no evaluada")
                )

                if status == "Success":
                    estado_traducido = "CUMPLE"
                elif status == "Failure":
                    estado_traducido = "NO_CUMPLE"
                else:
                    estado_traducido = "DESCONOCIDO"

                metric_value = None
                for metric_key in metric_keys_for_rule(rule):
                    if metric_key in metrics_map:
                        metric_value = metrics_map[metric_key]
                        break

                if metric_value is None and isinstance(message, str):
                    match = re.search(r"Value:\s*(0?\.\d+|1(?:\.0+)?)", message)
                    if match:
                        metric_value = float(match.group(1))

                porcentaje = float(metric_value * 100) if metric_value is not None else None
                registros_fallidos = (
                    int(round(total_records * (1 - (porcentaje / 100.0))))
                    if porcentaje is not None
                    else None
                )

                if status == "Failure":
                    if porcentaje is not None:
                        observacion = (
                            f"Alerta: Solo el {porcentaje:.2f}% de los datos cumple la regla. "
                            f"Se exigía el {rule.get('sla_threshold')}%."
                        )
                    else:
                        observacion = "Fallo en la validación de la regla."
                elif status == "Success":
                    observacion = "Regla cumplida exitosamente."
                else:
                    observacion = message

                mapped_results.append(
                    (
                        None,
                        None,
                        QUALITY_GROUP_ID,
                        full_table_name,
                        rule.get("data_domain"),
                        rule["column_name"],
                        DIMENSION_BY_RULE.get(rule["rule_type"], "Desconocida"),
                        rule["rule_type"],
                        rule["params"],
                        rule["severity"],
                        rule.get("business_description"),
                        rule.get("sla_threshold"),
                        estado_traducido,
                        total_records,
                        registros_fallidos,
                        porcentaje,
                        observacion,
                    )
                )

            if not mapped_results:
                df_data.unpersist()
                return None

            result_df = (
                spark.createDataFrame(mapped_results, schema=output_schema)
                .withColumn("periodo", current_date())
                .withColumn("fecha_proceso", current_timestamp())
            )

            df_data.unpersist()
            return result_df

        except Exception as e:
            import traceback

            if "df_data" in locals():
                df_data.unpersist()

            traceback.print_exc()
            print(f"Error procesando {schema_name}.{table_name}: {str(e)}")
            return None

    df_rules = get_standardized_rules(input_data)
    active_rules = df_rules.filter(
        (col("group_id") == QUALITY_GROUP_ID)
        & (col("is_active_str").isin("true", "1", "si"))
    ).collect()

    if not active_rules:
        return spark.createDataFrame([], schema=output_schema)

    datasets: Dict[Tuple[str, str], List] = {}
    for rule in active_rules:
        datasets.setdefault((rule["schema"], rule["table"]), []).append(rule)

    final_frames: List[DataFrame] = []
    for (schema_name, table_name), rules in datasets.items():
        result_df = process_single_dataset(schema_name, table_name, rules)
        if result_df is not None:
            final_frames.append(result_df)

    if not final_frames:
        return spark.createDataFrame([], schema=output_schema)

    from functools import reduce

    return reduce(DataFrame.unionAll, final_frames)


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
