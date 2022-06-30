__all__ = ["start_spark"]

from pyspark import SparkContext
from pyspark.sql import SparkSession
from os import environ, sysconf
from os.path import abspath, curdir, splitext, join
from utils.os import get_total_memory, get_num_cores, choose_storage_device


ncores = sysconf("SC_NPROCESSORS_ONLN")
membytes = sysconf("SC_PAGESIZE") * sysconf("SC_PHYS_PAGES")
memgibs = membytes // (1024**3)

OVERHEAD_FRACTION = 0.1
CORES_PER_EXECUTOR = 5
PARALLELISM_PER_CORE = 2


def start_spark(
    deploy_mode="standalone",
    local_dir=None,
    executor_instances=None,
    executor_memory_gb=None,
    overhead_memory=OVERHEAD_FRACTION,
    executor_cores=CORES_PER_EXECUTOR,
    parallelism=PARALLELISM_PER_CORE,
    extra_conf={},
):
    builder = SparkSession.builder

    if deploy_mode == "cluster":
        # trust the cluster configuration except unless explicit values are given

        # TODO accept overrides
        return SparkSession.builder.enableHiveSupport().getOrCreate()

    if deploy_mode == "standalone":
        # Mostly reverse-engieneered from http://spark-configuration.luminousmen.com/

        if not local_dir:
            mount_points = choose_storage_device(return_all_possible=True)
            local_dir = ",".join(
                join(mount_point, "spark") for mount_point in mount_points
            )

        ncores = get_num_cores()
        memgibs = get_total_memory()

        executor_cores = min(executor_cores, ncores - 1)  # cannot exceed total cores -1

        if not executor_instances:
            executor_instances = ncores // executor_cores

        if not executor_memory_gb:
            executor_memory_gb = int(
                ((memgibs / executor_instances) - 1) * (1 - OVERHEAD_FRACTION)
            )

        if overhead_memory < 1:
            overhead_memory = int(executor_memory_gb * OVERHEAD_FRACTION * 1024)

        executor_instances = max(
            executor_instances - 1, 1
        )  # Leaving 1 for application manager

        default_parallelism = parallelism * executor_cores * executor_instances

        conf = {
            "spark.local.dir": local_dir,
            "spark.default.parallelism": default_parallelism,
            "spark.executor.memory": f"{executor_memory_gb}g",
            "spark.executor.instances": executor_instances,
            "spark.driver.cores": executor_cores,
            "spark.executor.cores": executor_cores,
            "spark.driver.memory": f"{executor_memory_gb}g",
            "spark.driver.maxResultSize": f"{executor_memory_gb}g",
            "spark.driver.memoryOverhead": f"{overhead_memory}m",
            "spark.executor.memoryOverhead": f"{overhead_memory}m",
            "spark.memory.fraction": "0.8",
            "spark.dynamicAllocation.enabled": "false",
            "spark.sql.adaptive.enabled": "true",
            "spark.scheduler.barrier.maxConcurrentTasksCheck.maxFailures": "5",
            "spark.rdd.compress": "true",
            "spark.shuffle.compress": "true",
            "spark.shuffle.spill.compress": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.driver.port": "11000",
            "spark.ui.port": "4040",
            "spark.hadoop.fs.s3a.multiobjectdelete.enable": "false",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.sql.parquet.filterPushdown": "true",
            "spark.sql.parquet.mergeSchema": "false",
            "fs.s3a.connection.maximum": "200",
            "spark.hadoop.fs.s3a.multipart.threshold": "2097152000",
            "spark.hadoop.fs.s3a.multipart.size": "1048576000",
            "spark.hadoop.fs.s3a.connection.timeout": "3600000",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
            "spark.speculation": "false",
            # "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark",
            # "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark",
            **extra_conf,
        }

        builder = SparkSession.builder
        for key, value in conf.items():
            if value:
                builder = builder.config(key, value)

        spark = builder.enableHiveSupport().master("local[*]").getOrCreate()

        sparkConf = spark.sparkContext.getConf().getAll()
        print("\n".join(f"{k}:\t{v}" for k, v in sparkConf if len(v) < 500))

        return spark
