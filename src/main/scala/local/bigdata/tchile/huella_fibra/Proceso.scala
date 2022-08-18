package local.bigdata.tchile.huella_fibra

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Proceso {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------------------------------
    // INICIO PARAMETRIZACION
    // ---------------------------------------------------------------

    // BASE

    val conf = new SparkConf().setMaster("yarn").setAppName("B2B").set("spark.driver.allowMultipleContexts", "true").set("spark.yarn.queue", "default").set("spark.sql.crossJoin.enabled", "true").set("spark.sql.debug.maxToStringFields", "1000").set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val spark: SparkSession = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "/usr/hdp/current/spark-client/conf/hive-site.xml").config("spark.submit.deployMode", "cluster").enableHiveSupport().getOrCreate()
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    import spark.implicits._

    // PARAMETROS

    val currentDate = java.time.LocalDateTime.now
    val processYear = currentDate.getYear.toString
    val processMonth = currentDate.getMonthValue.toString.reverse.padTo(2, '0').reverse
    val processDay = currentDate.getDayOfMonth.toString.reverse.padTo(2, '0').reverse

    // ACCESO EXADATA

    val exaDriver = "oracle.jdbc.OracleDriver"
    val exaUrl = "jdbc:oracle:thin:@(DESCRIPTION =(ENABLE=BROKEN)(ADDRESS=(PROTOCOL=TCP)(HOST=smt-scan.tchile.local)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=EXPLOTA)))"
    val exaUser = args(0)
    val exaPass = args(1)

    // DESTINO EXADATA

    val schema = "PRODUCTO_ASIGNADO"
    val table = "HUELLA_FIBRA_B2B"

    // PATHS

    val conformadoBasePath = "/modelos/producto_asignado/huella_fibra_b2b/"
    val conformadoFullPath = s"$conformadoBasePath/year=$processYear/month=$processMonth/day=$processDay"

    val inventarioActualBasePath = "/apps/hive/warehouse/stg_prospectos.db/inventario_osp_actual_norm_full/"
    val inventarioActualPath = fs.listStatus(new Path(inventarioActualBasePath)).filter(_.isDirectory()).map(_.getPath.toString).sortWith(_ > _)(0)
    val direccionesNormalizadasPath = "/warehouse/staging/datalakeb2b/fil_direcciones_normalizadas"
    val parqueFijoSuscriptorProductoPath = s"/modelos/producto_asignado/parque_fijo_suscriptor_producto_cg/conformado/year=$processYear/month=$processMonth"
    val competenciaFibraOpticaPath = s"/modelos/gestion_recursos/competencia_fibra_optica/year=$processYear/month=$processMonth"
    val estadoProyectoAvbaPath = "/modelos/gestion_recursos/estado_proyecto_avba/conformado"
    val segmentoPath = s"/data/clientes/segmento_empresas/b2b/salesforce/empresa/raw_sgm_v3/conformado/year=$processYear/month=$processMonth"
    val razonesSocialesPath = "/data/ventas/preventa_empresas/b2b/salesforce/contactabilidad/raw_eqx_razones_sociales"
    val carteraEmpresasPath = "/warehouse/staging/datalakeb2b/fil_cartera_empresa_v2"
    val tenenciaFijaMovilBasePath = "/data/parque_asignado/parque/b2b/scel/tenenciaFijoMovil/conformado"
    val tenenciaFijaMovilBaseLastYear = fs.listStatus(new Path(tenenciaFijaMovilBasePath)).filter(_.isDirectory()).map(_.getPath.toString).sortWith(_ > _)(0)
    val tenenciaFijaMovilPath = fs.listStatus(new Path(tenenciaFijaMovilBaseLastYear)).filter(_.isDirectory()).map(_.getPath.toString).sortWith(_ > _)(0)

    // ---------------------------------------------------------------
    // FIN PARAMETRIZACION
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO PROCESO
    // ---------------------------------------------------------------

    // 1

    val inventarioActualBase = spark.read.orc(inventarioActualPath).
      select(
        $"OSP_CALLE".as("CALLE"),
//        $"OSP_ALTURA_ORIGEN",
        $"OSP_ALTURA".as("ALTURA"),
        $"OSP_CPN".as("COMPLEMENTO"),
        $"OSP_DEPTO".as("DEPTO"),
        $"OSP_AREATEL".as("AREATEL_CONTRATADO"),
        $"OSP_IDAVBA".cast("int").as("IDAVBA"),
        $"OSP_LAT".cast("decimal(24,20)").as("LATITUD"),
        $"OSP_LON".cast("decimal(24,20)").as("LONGITUD"),
//        $"ID_CALLE_XYGO",
        $"COMUNA_XYGO".as("COMUNA"),
        $"OSP_PLANTA_GL".as("OC"),
//        $"OSP_ELEMENTO_RED_GL",
        $"ID_DIRECCION_XYGO".cast("int"),
        $"ID_DEPENDENCIA_XYGO".cast("int"),
        $"OSP_EQPT_DISPONIBLES".as("VACANCIA")
//        $"OSP_EQ_ID"
      )

    val inventarioActualA = inventarioActualBase.
      withColumn("REGISTRO", row_number().over(Window.partitionBy("AREATEL_CONTRATADO").orderBy(desc("AREATEL_CONTRATADO")))).
      filter(
        length($"AREATEL_CONTRATADO") =!= 0
        and $"REGISTRO" === 1
      )

    val inventarioActualB = inventarioActualBase.
      filter($"AREATEL_CONTRATADO" === "").
      withColumn(
        "REGISTRO",
        row_number().
          over(
            Window.
            partitionBy(concat($"ID_DIRECCION_XYGO", $"ID_DEPENDENCIA_XYGO")).
            orderBy(concat($"ID_DIRECCION_XYGO", $"ID_DEPENDENCIA_XYGO").desc)
        )
      ).
      filter($"REGISTRO" === 1)

    val inventarioActual = inventarioActualA.union(inventarioActualB)

    // 2

    val direccionesNormalizadas = spark.read.orc(direccionesNormalizadasPath).
      withColumn("ID_DIRECCION", trim($"ID_DIRECCION")).
      withColumn("ID_DEPENDENCIA", trim($"ID_DEPENDENCIA")).
      groupBy("ID_DIRECCION", "ID_DEPENDENCIA").
      count().
      select("ID_DIRECCION", "ID_DEPENDENCIA")

    // 3

    val parqueFijoSuscriptorProducto = spark.read.parquet(parqueFijoSuscriptorProductoPath).
      withColumnRenamed("cnt_identification_doc_number", "RUT_CONTRATADO").
      withColumnRenamed("CUSTOMER_SUB_TYPE_DESC", "SEGMENTO_CONTRATADO").
      filter($"VALID_IND" === 1).
      groupBy("service_id", "RUT_CONTRATADO", "SEGMENTO_CONTRATADO").
      agg(
        collect_set("main_product_family").cast("string").as("PRODUCTOS_CONTRATADOS"),
        collect_set("pba_dw_speed").cast("string").as("VELOCIDAD_CONTRATADA")
      )

    // 4

    val competenciaFibraOptica = spark.read.parquet(competenciaFibraOpticaPath).
      withColumn("CLARO", coalesce($"IND_CLARO", lit(0))).
      withColumn("ENTEL", coalesce($"IND_ENTEL", lit(0))).
      withColumn("GTD", coalesce($"IND_GTD", lit(0))).
      withColumn("MUNDO", coalesce($"IND_MUNDOPACIFICO", lit(0))).
      withColumn("TELSUR", coalesce($"IND_TELSUR", lit(0))).
      withColumn("VTR", coalesce($"IND_VTR", lit(0))).
      withColumn("WOM", coalesce($"IND_WOM", lit(0))).
      withColumnRenamed("IND_COMPETENCIA", "COMPETENCIA").
      select(
        "ID_DIRECCION",
        "CLARO",
        "ENTEL",
        "GTD",
        "MUNDO",
        "TELSUR",
        "VTR",
        "WOM",
        "COMPETENCIA"
      ).distinct()

    // 5

    val estadoProyectoAvba = spark.read.parquet(estadoProyectoAvbaPath).
      select(
        $"estado_hp".as("ESTADO_DESPLIEGUE"),
        $"fecha_liberacion_hp".as("FECHA_LIBERACION"),
        $"id_avba".as("IDENTIFICADOR")
      )

    // 6

    val direccionesNormalizadasAll = spark.read.orc(direccionesNormalizadasPath).
      withColumn("rut10", substring(concat(lit("0000000000"), $"rut_cliente"), -10, 10)).
      withColumn("ID_DIRECCION", trim($"ID_DIRECCION")).
      withColumn("ID_DEPENDENCIA", trim($"ID_DEPENDENCIA")).
//      withColumn("RUT_CLIENTE", trim($"RUT_CLIENTE")).
//      withColumn("LLAVE2", concat($"ID_DIRECCION", lit("|"), $"ID_DEPENDENCIA")).
//      withColumn("LLAVE", concat($"LLAVE2", lit("|"), $"RUT_CLIENTE")).
//      withColumn("CONCATENA", concat($"nombre_calle", $"numero_municipal", $"numero_dependencia", $"nombre_comuna")).
      select(
        "rut10",
        "id_direccion",
        "id_dependencia"
//        "llave",
//        "llave2",
//        "rut_cliente",
//        "nombre_calle",
//        "can",
//        "numero_municipal",
//        "numero_dependencia",
//        "torre_dependencia",
//        "nombre_comuna",
//        "nombre_provincia",
//        "nombre_region",
//        "codigo_postal",
//        "concatena",
//        "tipo_edificacion",
//        "latitud",
//        "longitud",
//        "id_block",
//        "osp_idavba"
      ).distinct()

    // 7

    val segmento = spark.read.parquet(segmentoPath).
      select(
        $"rut10",
        $"Segmento_Propuesto".as("SEGMENTO")
      )

    // 8

    val razonesSociales = spark.read.orc(razonesSocialesPath).
      select(
        $"Rut10",
        $"Razon_Social".as("RAZON_SOCIAL")
      ).distinct()

    // 9

    val carteraEmpresas = spark.read.orc(carteraEmpresasPath).
      withColumn("rut10", substring(concat(lit("0000000000"), $"rut_hija"), -10, 10)).
      select(
        $"rut10",
        $"nombre_gerente_comercial".as("GERENTE"),
        $"nombre_subgerente_am".as("SUBGERENTE"),
        $"nombre_jefe_comercial_am".as("JEFE"),
        $"nombre_supervisor_am".as("SUPERVISOR"),
        $"nombre_am".as("EJECUTIVO")
      )

    // 10

    val tenenciaFijaMovil = spark.read.parquet(tenenciaFijaMovilPath).
      withColumn("rut10", substring(concat(lit("0000000000"), $"rut_cliente"), -10, 10)).
//      withColumn("SSPP", lit(null)).
//      withColumn("FIJO_SSPP", lit(null)).
//      withColumn("DIG_SSPP", lit(null)).
//      withColumn("DIG_FIJO", lit(null)).
      withColumnRenamed("pq_voz", "VOZ").
      withColumnRenamed("pq_bam", "BAM").
      withColumnRenamed("pq_m2m", "M2M").
      withColumnRenamed("pq_stb", "STB").
      withColumnRenamed("pq_baf", "ADSL").
      withColumnRenamed("pq_fo", "FO").
      withColumnRenamed("pq_tv", "TV").
      select(
        "rut10",
        "VOZ",
        "BAM",
        "M2M",
        "STB",
        "ADSL",
        "FO",
        "TV"
//        "SSPP",
//        "FIJO_SSPP",
//        "DIG_SSPP",
//        "DIG_FIJO"
      )

    // 11

    val conformado = inventarioActual.
      join(direccionesNormalizadas, inventarioActual("ID_DEPENDENCIA_XYGO") === direccionesNormalizadas("ID_DEPENDENCIA") and inventarioActual("ID_DIRECCION_XYGO") === direccionesNormalizadas("ID_DIRECCION")).
      join(parqueFijoSuscriptorProducto, inventarioActual("AREATEL_CONTRATADO") === parqueFijoSuscriptorProducto("service_id"), "left").
      join(competenciaFibraOptica, inventarioActual("ID_DIRECCION_XYGO") === competenciaFibraOptica("ID_DIRECCION"), "left").
      join(estadoProyectoAvba, inventarioActual("IDAVBA") === estadoProyectoAvba("IDENTIFICADOR"), "left").
      join(direccionesNormalizadasAll, inventarioActual("ID_DIRECCION_XYGO") === direccionesNormalizadasAll("ID_DIRECCION") and inventarioActual("ID_DEPENDENCIA_XYGO") === direccionesNormalizadasAll("ID_DEPENDENCIA"), "left").
      join(segmento, direccionesNormalizadasAll("Rut10") === segmento("Rut10"), "left").
      join(razonesSociales, direccionesNormalizadasAll("Rut10") === razonesSociales("Rut10"), "left").
      join(carteraEmpresas, direccionesNormalizadasAll("Rut10") === carteraEmpresas("Rut10"), "left").
      join(tenenciaFijaMovil, direccionesNormalizadasAll("Rut10") === tenenciaFijaMovil("Rut10"), "left").
      withColumn(
        "RUTDV",
        concat(substring(direccionesNormalizadasAll("Rut10"), 0, 9).cast("long").cast("string"), lit("-"), substring(direccionesNormalizadasAll("Rut10"), -1, 1))
      ).
      withColumn("SEMANAS_LIBERACION", ceil(datediff(current_date(), $"FECHA_LIBERACION") / 7).cast("int")).
      withColumn(
        "ESTADO_FIBRA",
        when(!$"AREATEL_CONTRATADO".isNull, lit("Ocupada")).
        otherwise(
          when($"VACANCIA" > 0, lit("Disponible"))
            when($"VACANCIA" === 0, lit("Sin Vacancia"))
            when($"VACANCIA".isNull, lit("No Disponible"))
        )
      ).
      withColumn("MES_GRATIS", when($"ENTEL" =!= 0 or $"MUNDO" =!= 0 or $"WOM" =!= 0, 1).otherwise(0)).
      withColumn(
        "DIRECCION_COMERCIAL",
        when(!direccionesNormalizadas("ID_DIRECCION").isNull or $"SEGMENTO_CONTRATADO".isin("Corporaciones", "Gran Empresa", "Mediana", "Pequeña", "Micro"), 1).
          otherwise(0)
      ).
      withColumn("FECHA_PROCESO", current_date()).
      filter(
        $"DIRECCION_COMERCIAL" === 1
        and $"SEGMENTO".isin("MEDIANA", "PEQUEÑA", "MICRO")
      ).
      select(
        direccionesNormalizadasAll("Rut10").as("RUT"),
        $"RUTDV",
        $"SEGMENTO",
        $"RAZON_SOCIAL",
        $"VOZ",
        $"BAM",
        $"M2M",
        $"STB",
        $"ADSL",
        $"FO",
        $"TV",
//        $"SSPP",
//        $"FIJO_SSPP",
//        $"DIG_SSPP",
//        $"DIG_FIJO",
        $"CALLE",
        $"ALTURA",
        $"COMPLEMENTO",
        $"DEPTO",
        $"COMUNA",
        $"OC",
        $"LATITUD",
        $"LONGITUD",
        $"IDAVBA",
        $"ESTADO_DESPLIEGUE",
        $"SEMANAS_LIBERACION",
        $"FECHA_LIBERACION".cast("date"),
        $"ESTADO_FIBRA",
        $"VACANCIA",
        $"AREATEL_CONTRATADO",
        $"PRODUCTOS_CONTRATADOS",
        $"VELOCIDAD_CONTRATADA",
        $"RUT_CONTRATADO",
        $"SEGMENTO_CONTRATADO",
        $"ID_DIRECCION_XYGO",
        $"ID_DEPENDENCIA_XYGO",
        $"COMPETENCIA",
        $"CLARO",
        $"ENTEL",
        $"GTD",
        $"MUNDO",
        $"TELSUR",
        $"VTR",
        $"WOM",
        $"MES_GRATIS",
        $"GERENTE",
        $"SUBGERENTE",
        $"JEFE",
        $"SUPERVISOR",
        $"EJECUTIVO",
        $"DIRECCION_COMERCIAL",
        $"FECHA_PROCESO"
      ).distinct().cache()

    // ---------------------------------------------------------------
    // FIN PROCESO
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO ESCRITURA HDFS
    // ---------------------------------------------------------------

    conformado.repartition(1).write.mode("overwrite").parquet(conformadoFullPath)

//    df.
//      repartition(1).
//      write.mode("overwrite").
//      option("delimiter", "|").
//      option("header", "true").
//      csv(conformadoBasePath)

    // ---------------------------------------------------------------
    // FIN ESCRITURA HDFS
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO ESCRITURA HIVE
    // ---------------------------------------------------------------

    spark.sql(s"DROP TABLE IF EXISTS $schema.$table")

    spark.sql(
      s"""CREATE EXTERNAL TABLE $schema.$table(
         |RUT string,
         |RUTDV string,
         |SEGMENTO string,
         |RAZON_SOCIAL string,
         |VOZ int,
         |BAM int,
         |M2M int,
         |STB int,
         |ADSL int,
         |FO int,
         |TV int,
         |CALLE string,
         |ALTURA string,
         |COMPLEMENTO string,
         |DEPTO string,
         |COMUNA string,
         |OC string,
         |LATITUD decimal(24,20),
         |LONGITUD decimal(24,20),
         |IDAVBA int,
         |ESTADO_DESPLIEGUE string,
         |SEMANAS_LIBERACION int,
         |FECHA_LIBERACION date,
         |ESTADO_FIBRA string,
         |VACANCIA int,
         |AREATEL_CONTRATADO string,
         |PRODUCTOS_CONTRATADOS string,
         |VELOCIDAD_CONTRATADA string,
         |RUT_CONTRATADO string,
         |SEGMENTO_CONTRATADO string,
         |ID_DIRECCION_XYGO int,
         |ID_DEPENDENCIA_XYGO int,
         |COMPETENCIA int,
         |CLARO int,
         |ENTEL int,
         |GTD int,
         |MUNDO int,
         |TELSUR int,
         |VTR int,
         |WOM int,
         |MES_GRATIS int,
         |GERENTE string,
         |SUBGERENTE string,
         |JEFE string,
         |SUPERVISOR string,
         |EJECUTIVO string,
         |DIRECCION_COMERCIAL int,
         |FECHA_PROCESO date
         |)
         |PARTITIONED BY
         |(year string, month string, day string)
         |STORED AS PARQUET
         |LOCATION '$conformadoBasePath'""".stripMargin)

    spark.sql(s"MSCK REPAIR TABLE $schema.$table")

    // ---------------------------------------------------------------
    // FIN ESCRITURA HIVE
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO ESCRITURA EXADATA
    // ---------------------------------------------------------------

    val doTheTruncate = s"CALL $schema.DO_THE_TRUNCATE('$table')"

    spark.read.format("jdbc").option("url", exaUrl).option("driver", exaDriver).option("sessionInitStatement", doTheTruncate).option("dbtable", s"$schema.$table").option("user", exaUser).option("password", exaPass).option("numPartitions", 1).load().show(1)

    conformado.write.format("jdbc").option("numPartitions", 2).option("url", exaUrl).option("dbtable", s"$schema.$table").option("user", exaUser).option("password", exaPass).option("driver", exaDriver).mode("append").save()

    // ---------------------------------------------------------------
    // END ESCRITURA EXADATA
    // ---------------------------------------------------------------

    spark.close()
  }
}