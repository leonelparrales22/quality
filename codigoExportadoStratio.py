import os


def pyspark_transform(spark, input_data, param_dict):
    import re
    from typing import Dict, List, Tuple, Iterable, Optional, Union

    from pyspark.sql import DataFrame
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

    os.environ["SPARK_VERSION"] = "3.2"

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

    def metric_keys_for_rule(rule: Dict[str, str]) -> List[Tuple[str, Optional[str]]]:
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

    def map_severity_to_checklevel(sev: str) -> CheckLevel:
        sev_norm = (sev or "").strip().lower()
        # Parametría: CRITICO / ADVERTENCIA
        if sev_norm in {"critico", "crítico", "critical", "error"}:
            return CheckLevel.Error
        return CheckLevel.Warning

    def process_single_dataset(
        schema_name: str, table_name: str, dataset_rules: Iterable
    ) -> Optional[DataFrame]:
        full_table_name = f"{schema_name}.{table_name}"
        df_data = None
        try:
            required_cols = {rule["column_name"] for rule in dataset_rules}
            query = (
                f"SELECT {', '.join(required_cols)} FROM {full_table_name}"
                if required_cols
                else f"SELECT * FROM {full_table_name}"
            )

            df_data = spark.sql(query)

            # Subir a memoria para evitar doble lectura
            df_data.persist(StorageLevel.MEMORY_AND_DISK)
            total_records = df_data.count()

            suite = VerificationSuite(spark).onData(df_data)

            dataset_rules_dicts = [rule.asDict() for rule in dataset_rules]
            constraint_plans: List[Dict[str, str]] = []

            for idx, rule in enumerate(dataset_rules_dicts):
                rule_type = rule["rule_type"]
                constraint_fn = RULE_REGISTRY.get(rule_type)
                if not constraint_fn:
                    continue

                check_level = map_severity_to_checklevel(rule.get("severity"))
                check_name = f"{rule_type}:{rule['column_name']}:{idx}"
                check_obj = Check(spark, check_level, check_name)

                constraint_result = constraint_fn(
                    check_obj, rule["column_name"], rule["params"]
                )
                if constraint_result is None:
                    continue

                suite.addCheck(check_obj)
                constraint_plans.append({"rule": rule, "check_name": check_name})

            if not constraint_plans:
                df_data.unpersist()
                return None

            verification_result = suite.run()

            metrics_map: Dict[Tuple[str, Optional[str]], float] = {}
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

                porcentaje = (
                    float(metric_value * 100) if metric_value is not None else None
                )
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
                        "{{{P_ID_GRUPO_CALIDAD}}}",
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

            if df_data is not None:
                df_data.unpersist()
            traceback.print_exc()
            print(f"Error procesando {schema_name}.{table_name}: {str(e)}")
            return None

    df_rules = get_standardized_rules(input_data)
    active_rules = df_rules.filter(
        (col("group_id") == "{{{P_ID_GRUPO_CALIDAD}}}")
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
