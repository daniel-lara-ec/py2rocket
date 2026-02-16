"""
Workflow generado desde JSON de Rocket

Workflow: pl-transformacion-Zp-Mdp-capa
ID: 67d9dbbc-3d7b-4611-ba2f-aaefdb431a10
"""

from py2rocket import pipeline, build
from py2rocket.core.operations import filter
from py2rocket.core.operations import parquet
from py2rocket.core.operations import parquet_output
from py2rocket.core.operations import sql
from py2rocket.core.operations import trigger
from py2rocket.core.pipeline import OutputWriter
from py2rocket.core.pipeline import UIPosition


@pipeline(
    name="pl-transformacion-Zp-Mdp-capa",
    execution_engine="Hybrid",
    params={"P_NOMBRE_TABLA": "tmp_leads_cev_v2"},
    workflow_id="67d9dbbc-3d7b-4611-ba2f-aaefdb431a10",
    project_id="196c1c2d-5dfd-4756-ba37-80aa50d0f742",
    group_id="99beb8c9-32e7-465f-9081-137cea8adee6",
    asset_id="3d3d44bf-96bd-4f65-b731-44f14fecdbb9",
    parameters_lists=[
        "Environment",
        "SparkResources",
        "SparkConfigurations",
        "ParamsRecepcionRemesas",
    ],
    group_name="/home/cda-paucordo-sandbox/developer/recepcion-remesas/transformaciones/remesas/execution/data/mdt-execution/layers/13-layer-clientes-exterior",
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """
    # Input nodes
    load_catalogo_cantones = sql(
        name="Load_Catalogo_Cantones",
        query="""
SELECT DISTINCT dpaParroquia
FROM dsc_medios_pago.ZP_BP_Mdp_TD_InecGeoDpaParroquia
""",
        force_native_query=False,
        cache_table=False,
        description="",
        priority=10,
        ui_position=UIPosition(x=225, y=692),
    )
    load_emigracion = sql(
        name="Load_Emigracion",
        query="""
SELECT
    parroq AS dpaParroquia
    ,COUNT(1) AS conteoMigrantes
FROM dsc_medios_pago.ZP_BP_Mdp_TD_InecCenso2022Emigracion
WHERE e03 + (2025-e01) >= 18
AND e03 != 999
GROUP BY parroq
""",
        force_native_query=False,
        cache_table=False,
        description="",
        ui_position=UIPosition(x=229, y=869),
    )
    load_localizacion = sql(
        name="Load_Localizacion",
        query="""
SELECT
    codigoIdentificacionInternoCliente,
    provinciaDomicilioCliente AS provincia,
    codigoParroquiaDomicilioCliente,
    ROW_NUMBER() OVER (
        PARTITION BY
            codigoIdentificacionInternoCliente
        ORDER BY
            codigoParroquiaDomicilioCliente
    ) AS conteoDatos
FROM
    prd_gob_reg_clientes.ZP_BP_Par_Cli_TN_ClienteLocalizacion
WHERE
    tipoDireccion = 'Domicilio'
    AND codigoPaisDomicilioCliente = 'EC'
    AND UPPER(TRIM(codigoProvinciaDomicilioCliente)) IN (
        '01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24'
    )
""",
        force_native_query=False,
        cache_table=False,
        description="",
        priority=100,
        ui_position=UIPosition(x=239, y=593),
    )
    load_poblacion = sql(
        name="Load_Poblacion",
        query="""
SELECT
    parroq AS dpaParroquia
    ,COUNT(1) AS conteoPoblacion
FROM dsc_medios_pago.ZP_BP_Mdp_TD_InecCenso2022Poblacion
WHERE p03 >= 18
GROUP BY parroq
""",
        force_native_query=False,
        cache_table=False,
        description="",
        ui_position=UIPosition(x=227, y=780),
    )
    load_sharedigital = sql(
        name="Load_ShareDigital",
        query="""
SELECT
    codigoIdentificacionInternoCliente
    ,share_dig
    ,periodo
FROM dsc_medios_pago.ZP_BP_Mdp_TM_CapaShareDigital
""",
        force_native_query=False,
        cache_table=False,
        description="",
        ui_position=UIPosition(x=423, y=288),
    )
    pi_geolocalizacion = parquet(
        name="Pi_Geolocalizacion",
        path="""
s3a://s3-lagodatos-noprod-04/data/tmp/analitica/20014/layerRemesasGeolocalizacionProvEcu
""",
        is_recursive_enabled=True,
        paths=[
            {
                "path": None,
                "subdirGlobFilter": None,
                "subdirRegexFilter": None,
                "excludeGlobFilter": None,
                "excludeRegexFilter": None,
            }
        ],
        metadata_column_enabled=True,
        enable_filter_pattern=True,
        path_glob_filter="*.parquet",
        description="",
        ui_position=UIPosition(x=236, y=483),
    )
    sql_datosbasicos = sql(
        name="SQL_DatosBasicos",
        query="""
SELECT
    LPAD(TRIM(codigoIdentificacionInternoCliente),16,'0') AS codigoIdentificacionInternoCliente
    ,LPAD(TRIM(numeroIdentificacionCliente),14,'0') AS numeroIdentificacionCliente
FROM prd_gob_reg_clientes.ZP_BP_Par_Cli_TN_ClienteDatosBasicos_shist
WHERE
    periodo = LAST_DAY('{{{P_FECHA_CORTE}}}')
    AND numeroIdentificacionCliente not rlike '(?i)\beli\b|\bel\b|li|e'
    AND TRIM(numeroIdentificacionCliente) is not null
    AND UPPER(TRIM(segmentoCliente)) = 'PERSONAS'
    AND UPPER(TRIM(estadoCliente)) = 'S'
    AND UPPER(TRIM(marcaClienteFallecido)) = 'NO FALLECIDO'
    AND autorizacionTratamientoDatosPersonalesCliente = true
    AND LPAD(TRIM(numeroIdentificacionCliente),14,'0') NOT IN (
        SELECT
            LPAD(TRIM(numeroIdentificacionCliente),14,'0') AS numeroIdentificacionCliente
        FROM prd_gob_reg_clientes.ZP_BP_Par_Cli_TN_ClienteDatosBasicos_shist
        WHERE
            periodo = LAST_DAY('{{{P_FECHA_CORTE}}}')
            AND numeroIdentificacionCliente not rlike '(?i)\beli\b|\bel\b|li|e'
            AND TRIM(numeroIdentificacionCliente) is not null
            AND UPPER(TRIM(segmentoCliente)) = 'PERSONAS'
            AND UPPER(TRIM(estadoCliente)) = 'S'
            AND UPPER(TRIM(marcaClienteFallecido)) = 'NO FALLECIDO'
            AND autorizacionTratamientoDatosPersonalesCliente = true
        GROUP BY LPAD(TRIM(numeroIdentificacionCliente),14,'0')
        HAVING COUNT(*) > 1
    )
""",
        force_native_query=False,
        cache_table=False,
        description="Carga y transformación de los datos básicos",
        priority=110,
        ui_position=UIPosition(x=234, y=381),
    )

    # Transformation nodes
    f_localizacionnoecuador = filter(
        name="F_LocalizacionNoEcuador",
        quote_sql=False,
        filter_exp="periodo = '{{{P_FECHA_CORTE}}}'",
        inputs=pi_geolocalizacion,
        description="",
        ui_position=UIPosition(x=416, y=483),
    )
    t_crucedatosparr = trigger(
        name="T_CruceDatosParr",
        sql="""
SELECT
    A.dpaParroquia
    ,B.conteoPoblacion
    ,C.conteoMigrantes
    ,C.conteoMigrantes/B.conteoPoblacion*100 AS tasaMigracion
FROM Load_Catalogo_Cantones AS A
LEFT JOIN Load_Poblacion AS B
ON A.dpaParroquia = B.dpaParroquia
LEFT JOIN Load_Emigracion AS C
ON A.dpaParroquia = C.dpaParroquia
""",
        quote_sql=False,
        discard_conditions="",
        replace_with_input_dataframe=False,
        inputs=[load_catalogo_cantones, load_poblacion, load_emigracion],
        description="",
        priority=60,
        ui_position=UIPosition(x=428, y=859),
    )
    f_priorizacionparroquias = filter(
        name="F_PriorizacionParroquias",
        quote_sql=False,
        filter_exp="""
tasaMigracion > (
        SELECT
            percentile_approx(tasaMigracion,0.70)
        FROM T_CruceDatosParr
    )
""",
        inputs=t_crucedatosparr,
        description="",
        ui_position=UIPosition(x=429, y=741),
    )
    f_registrounico = filter(
        name="F_RegistroUnico",
        quote_sql=False,
        filter_exp="conteoDatos=1",
        inputs=load_localizacion,
        description="",
        priority=110,
        ui_position=UIPosition(x=419, y=593),
    )
    f_trx = filter(
        name="F_Trx",
        quote_sql=False,
        filter_exp="""
periodo = '{{{P_FECHA_CORTE}}}'
AND (share_dig = 1
    OR
    share_dig IS NULL)
""",
        inputs=load_sharedigital,
        description="",
        ui_position=UIPosition(x=601, y=288),
    )
    t_clientesnoecuador = trigger(
        name="T_ClientesNoEcuador",
        sql="""
SELECT
    A.codigoIdentificacionInternoCliente
FROM SQL_DatosBasicos AS A
LEFT ANTI JOIN F_LocalizacionNoEcuador AS B
ON A.codigoIdentificacionInternoCliente = B.cifOrdenante
""",
        quote_sql=False,
        discard_conditions="",
        replace_with_input_dataframe=False,
        inputs=[sql_datosbasicos, f_localizacionnoecuador],
        description="",
        ui_position=UIPosition(x=416, y=381),
    )
    t_crucepotencialparroquia = trigger(
        name="T_CrucePotencialParroquia",
        sql="""
SELECT
    A.codigoIdentificacionInternoCliente
FROM F_RegistroUnico AS A
INNER JOIN F_PriorizacionParroquias  AS B
ON A.codigoParroquiaDomicilioCliente = B.dpaParroquia
""",
        quote_sql=False,
        discard_conditions="",
        replace_with_input_dataframe=False,
        inputs=[f_registrounico, f_priorizacionparroquias],
        description="",
        ui_position=UIPosition(x=596, y=645),
    )
    t_filtrotrxnofisicas = trigger(
        name="T_FiltroTrxNoFisicas",
        sql="""
SELECT
    A.codigoIdentificacionInternoCliente
FROM T_ClientesNoEcuador AS A
INNER JOIN F_Trx AS B
ON A.codigoIdentificacionInternoCliente = B.codigoIdentificacionInternoCliente
""",
        quote_sql=False,
        discard_conditions="",
        replace_with_input_dataframe=False,
        inputs=[t_clientesnoecuador, f_trx],
        description="",
        ui_position=UIPosition(x=597, y=382),
    )
    t_origeninmigracion = trigger(
        name="T_OrigenInmigracion",
        sql="""
SELECT
    A.codigoIdentificacionInternoCliente
FROM T_FiltroTrxNoFisicas AS A
INNER JOIN T_CrucePotencialParroquia AS B
ON A.codigoIdentificacionInternoCliente = B.codigoIdentificacionInternoCliente
""",
        quote_sql=False,
        discard_conditions="",
        replace_with_input_dataframe=False,
        inputs=[t_crucepotencialparroquia, t_filtrotrxnofisicas],
        description="",
        ui_position=UIPosition(x=596, y=482),
    )
    transformacion = trigger(
        name="Transformacion",
        sql="""
SELECT
    codigoIdentificacionInternoCliente
    ,CAST('{{{P_FECHA_CORTE}}}' AS DATE) AS periodo
    ,DATE_FORMAT('{{{P_FECHA_CORTE}}}', 'yyyyMM') AS codigoPeriodo
    ,from_utc_timestamp(current_timestamp(), 'America/Bogota') AS fechaIngesta
FROM T_OrigenInmigracion
""",
        quote_sql=False,
        discard_conditions="",
        replace_with_input_dataframe=False,
        inputs=t_origeninmigracion,
        outputs_writer=[
            OutputWriter(
                output_step_name="Po_Guardado",
                table_name="{{{P_NOMBRE_TABLA}}}",
                partition_by="periodo",
                check_if_empty=True,
            )
        ],
        description="",
        ui_position=UIPosition(x=764, y=484),
    )

    # Output nodes
    po_guardado = parquet_output(
        name="Po_Guardado",
        path="s3a://s3-lagodatos-noprod-04/data/tmp/analitica/recepcionremesas/20014/",
        save_options="",
        inputs=transformacion,
        description="",
        ui_position=UIPosition(x=765, y=377),
        include_supported_data_relations=False,
        include_debug_options=False,
    )


if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "original_rebuilt.json")
