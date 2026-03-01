import os

def pyspark_transform(spark, input_data, param_dict):
    import re
    from typing import Dict, Iterable, List, Tuple

    from pyspark import StorageLevel
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import col, trim, lower, current_date, current_timestamp
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    # 1. Configuración del entorno nativo para Deequ en Stratio
    os.environ["SPARK_VERSION"] = "3.2"

    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationResult, VerificationSuite

    # 2. Esquema de la tabla de salida
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

    # 3. Diccionario nativo de funciones Spark-Deequ
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
            check.satisfies(
                build_length_expression(column, params),
                build_length_expression(column, params)
            )
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
        if method_key in ("iscomplete", "collectnulls"):
            return [("Completeness", column)]
        if method_key == "isunique":
            return [("Uniqueness", column)]
        if method_key in ("iscontainedin", "isnonnegative", "haspattern"):
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

    # 4. Funciones para leer la parametrización de Stratio (input_data)
    def get_standardized_rules(raw_input) -> DataFrame:
        # Busca las reglas por nombre de entrada (o toma la primera si no se encuentra)
        input_key = param_dict.get("rules_input_name", "sql_parametria_quality")
        source_df = raw_input.get(input_key, list(raw_input.values())[0]) if isinstance(raw_input, dict) else raw_input
        
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
        # Busca el catálogo por nombre de entrada o infiere cuál es la segunda tabla
        input_key = param_dict.get("catalog_input_name", "catalogo_metodos")
        
        if isinstance(raw_input, dict) and input_key in raw_input:
            raw_catalog = raw_input[input_key]
        elif isinstance(raw_input, dict) and len(raw_input) > 1:
            keys = list(raw_input.keys())
            rules_key = param_dict.get("rules_input_name", "sql_parametria_quality")
            cat_keys = [k for k in keys if k != rules_key]
            raw_catalog = raw_input[cat_keys[0]] if cat_keys else raw_input[keys[1]]
        else:
            empty_schema = StructType([
                StructField("id_calidad_regla", StringType(), True),
                StructField("dimension", StringType(), True),
                StructField("catalog_description", StringType(), True),
                StructField("metodo", StringType(), True),
            ])
            return spark.createDataFrame([], empty_schema)

        return raw_catalog.select(
            trim(col("id_calidad_regla")).alias("id_calidad_regla"),
            trim(col("dimension")).alias("dimension"),
            trim(col("descripcion")).alias("catalog_description"),
            trim(col("metodo")).alias("metodo"),
        )

    standardized_rules = get_standardized_rules(input_data)
    catalog_df = get_catalog(input_data)

    if catalog_df.count() == 0:
        rules_df = standardized_rules
    else:
        rules_df = standardized_rules.join(
            catalog_df, standardized_rules["rule_code"] == catalog_df["id_calidad_regla"], "left"
        )

    # Filtrar solo las reglas activas. Si se pasa "dataset" en param_dict, se evalúa solo esa tabla.
    target_table = param_dict.get("dataset")
    if target_table:
        rules_df = rules_df.filter((col("is_active") == True) & (col("table_name") == target_table))
    else:
        rules_df = rules_df.filter(col("is_active") == True)

    active_rules = rules_df.collect()
    if not active_rules:
        return spark.createDataFrame([], schema=output_schema)

    # 5. Agrupación por (esquema, tabla, filtros) - Pushdown Predicate
    datasets: Dict[Tuple[str, str, str], List] = {}
    for rule in active_rules:
        filtro = rule["filters"] or ""
        datasets.setdefault((rule["schema"], rule["table_name"], filtro), []).append(rule)

    result_rows: List[Tuple] = []

    # 6. Motor de Evaluación 
    for (schema_name, table_name, filter_condition), rules in datasets.items():
        full_table_name = f"{schema_name}.{table_name}"
        df_data = None
        try:
            df_data = spark.table(full_table_name)
            if filter_condition:
                df_data = df_data.filter(filter_condition)
            
            # Caching en RAM (Extrema Optimización)
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

                constraint = constraint_fn(
                    Check(spark, CheckLevel.Warning, f"{rule_dict['rule_code']}:{rule_dict['column_name']}:{idx}"), 
                    rule_dict["column_name"], 
                    rule_dict["params"] or ""
                )
                
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

                    # 6.1 Extracción Exacta
                    for metric_key in metric_keys_for_rule(rule_dict, method_key, params):
                        lookup_key = (metric_key[0], metric_key[1] or "")
                        if lookup_key in metrics_map:
                            metric_value = metrics_map[lookup_key]
                            break

                    # 6.2 Extracción Parcial
                    if metric_value is None:
                        for (m_name, m_instance), m_val in metrics_map.items():
                            if m_name == "Compliance" and rule_dict["column_name"] in (m_instance or ""):
                                metric_value = m_val
                                break

                    # 6.3 Extracción Regex
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

    # 7. Retorno del DataFrame final enriquecido con tiempo
    return (
        spark.createDataFrame(result_rows, schema=output_schema)
        .withColumn("periodo", current_date())
        .withColumn("fecha_proceso", current_timestamp())
    )