from pyspark.sql.functions import when, col, collect_list, array_join, row_number
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd

def extraer_responsabilidades_rut(input_spark_df,
                                 nombre_campo_rut,
                                 spark,
                                 dbutils
                                 ):
    """
    Función que recibe un dataframe de spark, el nombre del campo con el númeor de identificación y extrae las responsabilidades (16, 33, 47 y 48), el tipo de contribuyente, la cantidad de establecimientos de la tabla int_personas y los agrega al dataframe de entrada.
    El campo marca_resp_52 contiene Sí o No dependiendo si cuenta con la respondabilidad 52.
    """
    # from pyspark.sql.functions import col, collect_list, array_join, row_number
    # from pyspark.sql import functions as F
    # from pyspark.sql.window import Window
    # import pandas as pd
    
    # Convertir a Spark DataFrame si es pandas DataFrame, si no, lanzar error
    if not hasattr(input_spark_df, "select"):
        
        if isinstance(input_spark_df, pd.DataFrame):
            input_spark_df = spark.createDataFrame(input_spark_df)
            print("Dataframe convertido de pandas a spark")
        else:
            raise TypeError("input_spark_df debe ser un Spark DataFrame o un pandas DataFrame.")
    print(input_spark_df.printSchema())
    
    # Agrupar todos los 'numero_identificacion' en una lista
    nits_list = [str(row['numero_identificacion']) for row in input_spark_df.select('numero_identificacion').distinct().collect()]
    nits_str_B_df = ', '.join(nits_list)

    # 2. Leer la tabla desde la base de datos int_personas y filtrar por los NITs de interés
    query_int_personas = f"""
    SELECT DISTINCT NUM_NIT, FEC_CAMBIO, IND_TIENE_RESP_47, IND_TIENE_RESP_48, IND_TIENE_RESP_33, IND_TIENE_RESP_16, IND_TIENE_RESP_52, NOM_TIPO_CONTRIBUYENTE, NUM_ESTABLECIMIENTOS
    FROM cat_prod.sq_silver_negocio_transversal.int_personas
    WHERE NUM_NIT IN ({nits_str_B_df})
    """

    # Ejecutar la consulta y mantener el resultado como Spark DataFrame
    df_infor_rut_int_personas = spark.sql(query_int_personas)

    cols_check = ['IND_TIENE_RESP_47', 'IND_TIENE_RESP_48', 'IND_TIENE_RESP_33', 'IND_TIENE_RESP_16']

    # 1. Identify Duplicate NITs (Same as original)
    duplicated_nits_df = (
        df_infor_rut_int_personas.groupBy('NUM_NIT')
        .count()
        .filter(F.col('count') > 1)
        .select('NUM_NIT')
    )

    # 2. Split Data (Same as original)
    df_non_dupes = df_infor_rut_int_personas.join(duplicated_nits_df, on='NUM_NIT', how='left_anti')
    df_dupes = df_infor_rut_int_personas.join(duplicated_nits_df, on='NUM_NIT', how='inner')

    # -----------------------------------------------------
    # 3. REVISED CLEANING LOGIC FOR DUPLICATED ROWS
    # -----------------------------------------------------

    # A. Create a window specification: Partition by NIT, order by an existing timestamp/ID column (or simulate the 'last' row).
    # Assuming you fixed "ROW_ID" to an actual column like "FECHA_ACTUALIZACION" or used monotonically_increasing_id
    window_spec = Window.partitionBy("NUM_NIT").orderBy(F.col("FEC_CAMBIO").desc()) 
    # Make sure "FECHA_ACTUALIZACION" exists or is created (e.g., using F.monotonically_increasing_id())

    df_dupes_ranked = df_dupes.withColumn("rank", F.row_number().over(window_spec))

    # B. Filter to keep the designated "last" row (rank 1).
    # df_dupes_cleaned now holds ONLY the single best row for each duplicated NIT.
    # This single row is the one you want to keep as the representative.
    df_dupes_cleaned = df_dupes_ranked.filter(F.col("rank") == 1).drop("rank")

    # C. REMOVE THE NULL-CHECK LOOP (It would incorrectly delete the best row if it contained a null)
    # The logic to clean duplicate groups is now complete.

    # -----------------------------------------------------
    # 4. Merge the Data Back Together
    # -----------------------------------------------------
    # Union the unique rows with the single, best-determined duplicate row.
    df_infor_rut_int_personas_clean = df_non_dupes.unionByName(df_dupes_cleaned)

    # Eliminar duplicados por NUM_NIT, conservando la primera ocurrencia
    window_spec = Window.partitionBy('NUM_NIT').orderBy(F.monotonically_increasing_id())
    df_infor_rut_int_personas_clean = df_infor_rut_int_personas_clean.withColumn(
        "rn", F.row_number().over(window_spec)
    ).filter(F.col("rn") == 1).drop("rn")

    # Nueva columna para marcar IND_TIENE_RESP_52
    df_infor_rut_int_personas_clean = df_infor_rut_int_personas_clean.withColumn(
        'marca_resp_52',
        F.when(F.col('IND_TIENE_RESP_52') == True, 'SÍ').otherwise('No')
    )

    # Realizar un join entre el DataFrame base y el DataFrame filtrado de int_personas usando el NIT
    df_joined_final = input_spark_df.join(
        df_infor_rut_int_personas_clean,
        input_spark_df['numero_identificacion'] == df_infor_rut_int_personas_clean['NUM_NIT'],
        how='inner'
    ).withColumn(
    "NOM_TIPO_CONTRIBUYENTE",
    F.when(F.col("NOM_TIPO_CONTRIBUYENTE").rlike("(?i)persona nat"), "Persona Natural")
    .when(F.col("NOM_TIPO_CONTRIBUYENTE").rlike("(?i)persona jur"), "Persona Jurídica")
    .otherwise(F.col("NOM_TIPO_CONTRIBUYENTE"))
)

    return df_joined_final