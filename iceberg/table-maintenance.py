
from pendulum import datetime
from datetime import timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

def create_operator(name:str, sql:str):
    return KubernetesPodOperator(
        name=name,
        task_id=name,
        namespace="airflow",
        service_account_name="spark",
        image="harbor.ingtra.net/library/spark:3.2.1",
        security_context={
            "runAsUser": 0,
            "runAsGroup": 0
        },
        cmds=["bash"],
        arguments=["-c", dedent(f"""
            cat << __EOF > execute.sql
            {sql.strip()}
            __EOF
            
            $SPARK_HOME/bin/spark-sql \
                --master "k8s://https://kubernetes.default.svc" \
                --driver-memory 1G \
                --executor-memory 2G \
                --executor-cores 1 \
                --num-executors 1 \
                --conf spark.driver.host=$(hostname -i) \
                --conf spark.driver.extraJavaOptions="-XX:+UseShenandoahGC -XX:ShenandoahGCHeuristics=compact" \
                --conf spark.executor.extraJavaOptions="-XX:+UseShenandoahGC -XX:ShenandoahGCHeuristics=compact" \
                --conf spark.executor.memoryOverhead=4G \
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
                --conf spark.kubernetes.namespace=airflow \
                --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
                --conf spark.kubernetes.container.image="harbor.ingtra.net/library/spark:3.2.1" \
                --conf spark.kubernetes.container.image.pullPolicy=Always \
                --conf spark.kubernetes.executor.podTemplateFile=http://minio.mdc.ingtra.net/bins/spark/executor_pod_template.yaml \
                -f execute.sql
        """)],
        do_xcom_push=False
    )

TABLES = [
    'twitter.sampled_stream'
]

with DAG(
    "iceberg-table-maintenance",
    start_date=datetime(2022, 5, 7, tz="Asia/Seoul"),
    schedule_interval="0 12 * * *",
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    default_args={
        "execution_timeout": timedelta(hours=6)
    }
) as dag:
    for table in TABLES:
        start = DummyOperator(task_id="start")

        rewrite = create_operator(f'{table}_rewrite', dedent(f"""
            CALL iceberg.system.rewrite_data_files(table => '{table}', where => 'created_at_ts < now() - INTERVAL 12 HOURS')
        """))

        expire_snapshots = create_operator(f'{table}_expire_snapshots', dedent(f"""
            CALL iceberg.system.expire_snapshots('{table}', DATE '9999-12-31')
        """))

        remove_orphan = create_operator(f'{table}_remove_orphan', dedent(f"""
           CALL iceberg.system.remove_orphan_files('{table}', DATE '{{{{ prev_ds }}}}')
        """))

        start >> rewrite >> expire_snapshots >> remove_orphan
