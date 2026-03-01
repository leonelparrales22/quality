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
CATALOG_CSV_PATH = Path("/home/leonelparrales/Documentos/Repositorios/quality/catalogo_metodos.csv")
DATA_DOMAIN_NAME = "Canales_Digitales"

SIMULATED_RULES: List[Dict[str, str]] = [
    {
        "id_calidad_tabla": TABLE_NAME,
        "coleccion": DATABASE_NAME,
        "nombre_columna": "remitenteNotificador",
        "id_calidad_regla": "COM_NULOS",
        "parametros": "",
        "filtros": "",
        "estado": True,
        "descripcion": "Todo registro debe tener el remitente notificador.",
        "umbral": 99.0,
        "macrodominio": "CANALES",
        "dominio": DATA_DOMAIN_NAME,
        "usuario_cambio": "laparral",
        "fecha_cambio": "2026-02-27 17:20:00",
    },
    {
        "id_calidad_tabla": TABLE_NAME,
        "coleccion": DATABASE_NAME,
        "nombre_columna": "guid",
        "id_calidad_regla": "COM_DUPLICADOS",
        "parametros": "",
        "filtros": "",
        "estado": True,
        "descripcion": "Cada transacción debe tener un GUID único",
        "umbral": 100.0,
        "macrodominio": "CANALES",
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
        "estado": True,
        "descripcion": "Estados válidos del flujo de notificación",
        "umbral": 98.5,
        "macrodominio": "CANALES",
        "dominio": DATA_DOMAIN_NAME,
        "usuario_cambio": "laparral",
        "fecha_cambio": "2026-02-27 17:20:00",
    },
    {
        "id_calidad_tabla": TABLE_NAME,
        "coleccion": DATABASE_NAME,
        "nombre_columna": "montoTransaccion",
        "id_calidad_regla": "VAL_POSITIVOS",
        "parametros": "",
        "filtros": "",
        "estado": True,
        "descripcion": "Los montos no pueden ser negativos",
        "umbral": 99.5,
        "macrodominio": "CANALES",
        "dominio": DATA_DOMAIN_NAME,
        "usuario_cambio": "laparral",
        "fecha_cambio": "2026-02-27 17:20:00",
    },
    {
        "id_calidad_tabla": TABLE_NAME,
        "coleccion": DATABASE_NAME,
        "nombre_columna": "documentId",
        "id_calidad_regla": "CONF_LONG_EXACTA",
        "parametros": "10",
        "filtros": "",
        "estado": True,
        "descripcion": "La cédula debe tener 10 dígitos",
        "umbral": 99.0,
        "macrodominio": "CANALES",
        "dominio": DATA_DOMAIN_NAME,
        "usuario_cambio": "laparral",
        "fecha_cambio": "2026-02-27 17:20:00",
    },
    {
        "id_calidad_tabla": TABLE_NAME,
        "coleccion": DATABASE_NAME,
        "nombre_columna": "emailOrigen",
        "id_calidad_regla": "CONF_REGEX",
        "parametros": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$",
        "filtros": "",
        "estado": True,
        "descripcion": "El correo debe cumplir con el formato estándar",
        "umbral": 97.0,
        "macrodominio": "CANALES",
        "dominio": DATA_DOMAIN_NAME,
        "usuario_cambio": "laparral",
        "fecha_cambio": "2026-02-27 17:20:00",
    },
    {
        "id_calidad_tabla": TABLE_NAME,
        "coleccion": DATABASE_NAME,
        "nombre_columna": "codigoEstadoNotificacion",
        "id_calidad_regla": "VAL_RANGO_VALORES",
        "parametros": "200,500",
        "filtros": "",
        "estado": True,
        "descripcion": "Coidgos HTTP permitidos",
        "umbral": 96.0,
        "macrodominio": "CANALES",
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
        "documentId": "1234567890",
        "guid": "GUID-0001",
        "remitenteNotificador": "NOTIFICADOR_APP",
        "emailOrigen": "cliente1@banco.com",
        "codigoEstadoNotificacion": 200,
    },
    {
        "estadoEnvioNotificacion": "failed",
        "fechaCarga": "2026-02-20 13:46:03",
        "montoTransaccion": -25.0,
        "documentId": "9876543",
        "guid": "GUID-0002",
        "remitenteNotificador": None,
        "emailOrigen": "cliente2@banco",
        "codigoEstadoNotificacion": 503,
    },
    {
        "estadoEnvioNotificacion": "pending",
        "fechaCarga": "2026-02-20 13:46:35",
        "montoTransaccion": 45.12,
        "documentId": "1111111111",
        "guid": "GUID-0003",
        "remitenteNotificador": "NOTIFICADOR_APP",
        "emailOrigen": "cliente3@banco.com",
        "codigoEstadoNotificacion": 404,
    },
    {
        "estadoEnvioNotificacion": "success",
        "fechaCarga": "2026-02-20 13:48:20",
        "montoTransaccion": 3100.75,
        "documentId": "0987654321",
        "guid": "GUID-0004",
        "remitenteNotificador": "NOTIFICADOR_APP",
        "emailOrigen": "cliente4@banco.com",
        "codigoEstadoNotificacion": 201,
    },
    {
        "estadoEnvioNotificacion": "success",
        "fechaCarga": "2026-02-20 13:49:02",
        "montoTransaccion": 8.99,
        "documentId": "2222222222",
        "guid": "GUID-0005",
        "remitenteNotificador": "NOTIFICADOR_APP",
        "emailOrigen": "cliente5@banco.com",
        "codigoEstadoNotificacion": 450,
    },
    {
        "estadoEnvioNotificacion": "unknown",
        "fechaCarga": "2026-02-20 13:52:10",
        "montoTransaccion": 75.0,
        "documentId": "3333333333",
        "guid": "GUID-0006",
        "remitenteNotificador": "NOTIFICADOR_APP",
        "emailOrigen": "cliente6@banco",
        "codigoEstadoNotificacion": 600,
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

    os.environ["SPARK_VERSION"] = "3.3"

    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationResult, VerificationSuite

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

    def normalize_method(method: str) -> str:
        value = (method or "").strip()
        if value.endswith("()"):
            value = value[:-2]
        return value.lower()

    def build_length_expression(column: str, params: str) -> str:
        return f"length({column}) = {params.strip()}"

    def build_range_expression(column: str, params: str) -> Tuple[str, List[str]]:
        bounds = [v.strip() for v in str(params).split(",") if v.strip()]
        return f"{column} >= {bounds[0]} AND {column} <= {bounds[1]}", bounds

    RULE_REGISTRY = {
        "iscomplete": lambda check, column, params: check.isComplete(column),
        "collectnulls": lambda check, column, params: check.isComplete(column),
        "isunique": lambda check, column, params: check.isUnique(column),
        "iscontainedin": lambda check, column, params: (
            check.isContainedIn(
                column,
                [v.strip() for v in str(params).split(",") if v.strip()],
            )
            if params and any(v.strip() for v in str(params).split(","))
            else None
        ),
        "isnonnegative": lambda check, column, params: check.isNonNegative(column),
        "haspattern": lambda check, column, params: (
            check.hasPattern(column, params) if params and params.strip() else None
        ),
        "between": lambda check, column, params: (
            check.satisfies(
                build_range_expression(column, params)[0],
                build_range_expression(column, params)[0],
            )
            if params and len([v.strip() for v in str(params).split(",") if v.strip()]) == 2
            else None
        ),
        "lengthequals": lambda check, column, params: (
            check.satisfies(build_length_expression(column, params), build_length_expression(column, params))
            if params and params.strip()
            else None
        ),
        "notnullorempty": lambda check, column, params: (
            check.satisfies(
                f"trim({column}) != '' AND {column} IS NOT NULL",
                f"trim({column}) != '' AND {column} IS NOT NULL",
            )
        ),
    }

    def metric_keys_for_rule(rule: Dict[str, str], method_key: str, params: str) -> List[Tuple[str, str]]:
        column = rule["column_name"]
        if method_key == "iscomplete" or method_key == "collectnulls":
            return [("Completeness", column)]
        if method_key == "isunique":
            return [("Uniqueness", column)]
        if method_key == "iscontainedin" or method_key == "isnonnegative" or method_key == "haspattern":
            return [("Compliance", column)]
        if method_key == "between":
            expression, _ = build_range_expression(column, params)
            return [("Compliance", expression)]
        if method_key == "lengthequals":
            expression = build_length_expression(column, params)
            return [("Compliance", expression)]
        if method_key == "notnullorempty":
            return [("Compliance", f"trim({column}) != '' AND {column} IS NOT NULL")]
        return []

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

    def get_catalog() -> DataFrame:
        empty_schema = StructType(
            [
                StructField("id_calidad_regla", StringType(), True),
                StructField("dimension", StringType(), True),
                StructField("catalog_description", StringType(), True),
                StructField("metodo", StringType(), True),
            ]
        )

        if not CATALOG_CSV_PATH.exists():
            print(f"Catálogo no encontrado en {CATALOG_CSV_PATH}. Continuando sin dimensiones.")
            return spark.createDataFrame([], empty_schema)

        raw_catalog = (
            spark.read.options(header=True, inferSchema=False)
            .csv(str(CATALOG_CSV_PATH))
            .select(
                trim(col("id_calidad_regla")).alias("id_calidad_regla"),
                trim(col("dimension")).alias("dimension"),
                trim(col("descripcion")).alias("catalog_description"),
                trim(col("metodo")).alias("metodo"),
            )
        )

        return raw_catalog

    standardized_rules = get_standardized_rules(input_data)
    catalog_df = get_catalog()

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

    datasets: Dict[Tuple[str, str, str], List] = {}
    for rule in active_rules:
        filtro = rule["filters"] or ""
        datasets.setdefault((rule["schema"], rule["table_name"], filtro), []).append(rule)

    result_rows: List[Tuple] = []

    for (schema_name, table_name, filter_condition), rules in datasets.items():
        full_table_name = f"{schema_name}.{table_name}"
        df_data = None
        try:
            df_data = spark.table(full_table_name)
            if filter_condition:
                df_data = df_data.filter(filter_condition)
            df_data.persist(StorageLevel.MEMORY_AND_DISK)

            total_records = df_data.count()

            suite = VerificationSuite(spark).onData(df_data)
            constraint_plans: List[Dict[str, str]] = []
            skipped_rules: List[Tuple[Dict[str, str], str]] = []

            for idx, rule in enumerate(rules):
                rule_dict = rule.asDict()
                method_key = normalize_method(rule_dict.get("metodo"))
                constraint_fn = RULE_REGISTRY.get(method_key)
                if not constraint_fn:
                    skipped_rules.append((rule_dict, f"Método '{rule_dict.get('metodo')}' no soportado"))
                    continue

                constraint = constraint_fn(Check(spark, CheckLevel.Warning, f"{rule_dict['rule_code']}:{rule_dict['column_name']}:{idx}"), rule_dict["column_name"], rule_dict["params"] or "")
                if constraint is None:
                    skipped_rules.append((rule_dict, f"Parámetros inválidos para el método {rule_dict.get('metodo')}."))
                    continue

                suite.addCheck(constraint)
                constraint_plans.append(
                    {
                        "rule": rule_dict,
                        "check_name": constraint.description,
                        "method_key": method_key,
                    }
                )

            if constraint_plans:
                verification_result = suite.run()
                metrics_map: Dict[Tuple[str, str], float] = {}
                for metric in VerificationResult.successMetricsAsDataFrame(spark, verification_result).collect():
                    metrics_map[(metric["name"], metric["instance"] or "")] = metric["value"]

                check_map: Dict[str, Tuple[str, str]] = {}
                for result in VerificationResult.checkResultsAsDataFrame(spark, verification_result).collect():
                    check_map[result["check"]] = (
                        result["constraint_status"],
                        result["constraint_message"],
                    )

                for plan in constraint_plans:
                    rule_dict = plan["rule"]
                    method_key = plan["method_key"]
                    threshold = rule_dict["threshold"] or 0.0
                    params = rule_dict["params"] or ""

                    status, message = check_map.get(plan["check_name"], ("Unknown", "Regla no evaluada"))

                    metric_value = None

                    # 1. Búsqueda exacta
                    for metric_key in metric_keys_for_rule(rule_dict, method_key, params):
                        lookup_key = (metric_key[0], metric_key[1] or "")
                        if lookup_key in metrics_map:
                            metric_value = metrics_map[lookup_key]
                            break

                    # 2. Búsqueda parcial
                    if metric_value is None:
                        for (m_name, m_instance), m_val in metrics_map.items():
                            if m_name == "Compliance" and rule_dict["column_name"] in (m_instance or ""):
                                metric_value = m_val
                                break

                    # 3. Fallback por regex
                    if metric_value is None and isinstance(message, str):
                        match = re.search(r"Value:\s*(0?\.\d+|1(?:\.0+)?)", message)
                        if match:
                            metric_value = float(match.group(1))

                    if total_records == 0:
                        porcentaje = 100.0
                        registros_fallidos = 0
                        estado_calidad = "CUMPLE"
                        observacion = "Sin registros tras aplicar filtros; regla considerada cumplida."
                    else:
                        estado_calidad = "CUMPLE" if status == "Success" else "NO_CUMPLE"

                        if metric_value is not None:
                            porcentaje = metric_value * 100
                            registros_fallidos = int(round(total_records * (1 - metric_value)))
                        else:
                            porcentaje = 0.0
                            registros_fallidos = total_records
                            estado_calidad = "NO_EVALUADA"

                        if estado_calidad == "CUMPLE":
                            observacion = "Regla cumplida exitosamente."
                        elif estado_calidad == "NO_CUMPLE" and metric_value is not None:
                            observacion = (
                                f"Alerta: Solo el {porcentaje:.2f}% de los datos cumple la regla. "
                                f"Se exigía el {threshold}%."
                            )
                        else:
                            observacion = message

                    result_rows.append(
                        (
                            None,
                            None,
                            table_name,
                            rule_dict["column_name"],
                            params,
                            threshold,
                            estado_calidad,
                            total_records,
                            registros_fallidos,
                            porcentaje,
                            observacion,
                        )
                    )

            for rule_dict, warning in skipped_rules:
                params = rule_dict["params"] or ""
                threshold = rule_dict["threshold"] or 0.0
                porcentaje = 0.0
                registros_fallidos = total_records if total_records else 0
                result_rows.append(
                    (
                        None,
                        None,
                        table_name,
                        rule_dict["column_name"],
                        params,
                        threshold,
                        "NO_EVALUADA",
                        total_records,
                        registros_fallidos,
                        porcentaje,
                        warning,
                    )
                )

        except Exception as e:
            import traceback

            traceback.print_exc()
            error_msg = f"Error fatal procesando {full_table_name}: {str(e)}"
            for rule in rules:
                rule_dict = rule.asDict()
                result_rows.append(
                    (
                        None,
                        None,
                        table_name,
                        rule_dict["column_name"],
                        rule_dict["params"] or "",
                        rule_dict["threshold"] or 0.0,
                        "ERROR_EJECUCION",
                        0,
                        0,
                        0.0,
                        error_msg[:250],
                    )
                )
        finally:
            if df_data is not None:
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
