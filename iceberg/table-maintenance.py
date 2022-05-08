
from pendulum import datetime
from datetime import timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

with DAG(
    'iceberg-table-maintenance',
    start_date=datetime(2022, 5, 7, tz='Asia/Seoul'),
    schedule_interval='0 12 * * *',
    catchup=False
) as dag:
    k = KubernetesPodOperator(
        name='test',
        task_id='test',
        namespace='airflow',
        image='harbor.ingtra.net/library/spark:3.2.1',
        cmds=['bash'],
        arguments=['-c', dedent("""
            cat << __EOF > execute.sql
                SHOW TABLES
            __EOF
            
            spark-sql
                --master "k8s://https://kubernetes.default.svc" \
                --driver-memory 1G \
                --executor-memory 2G \
                --executor-cores 1 \
                --num-executors 1 \
                --conf spark.driver.extraJavaOptions="-XX:+UseShenandoahGC -XX:ShenandoahGCHeuristics=compact" \
                --conf spark.executor.extraJavaOptions="-XX:+UseShenandoahGC -XX:ShenandoahGCHeuristics=compact" \
                --conf spark.executor.memoryOverhead=3G \
                --conf spark.shuffle.io.preferDirectBufs=false \
                --conf spark.network.io.preferDirectBufs=false \
                --conf spark.eventLog.gcMetrics.youngGenerationGarbageCollectors="Shenandoah Cycles" \
                --conf spark.eventLog.gcMetrics.oldGenerationGarbageCollectors="Shenandoah Pauses" \
                --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
                --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                --conf spark.sql.catalog.iceberg.type=hive \
                --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore.mdc.ingtra.net:9083 \
                --conf spark.sql.catalog.iceberg.hadoop.fs.s3a.endpoint=http://minio.mdc.ingtra.net \
                --conf spark.sql.catalog.iceberg.warehouse=s3a://iceberg/ \
                --conf spark.sql.catalog.iceberg.hive.metastore.warehouse.dir=s3a://iceberg/ \
                --conf spark.hadoop.fs.s3a.access.key=spark \
                --conf spark.hadoop.fs.s3a.secret.key=sparkuser \
                --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
                --conf spark.hadoop.fs.s3a.fast.upload=true \
                --conf spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled=true \
                --conf spark.kubernetes.container.image="harbor.ingtra.net/library/spark:3.2.1" \
                --conf spark.kubernetes.container.image.pullPolicy=Always \
                --conf spark.kubernetes.executor.podTemplateFile=http://minio.mdc.ingtra.net/bins/spark/executor_pod_template.yaml
                -f execute.sql
        """)],
        do_xcom_push=False
    )