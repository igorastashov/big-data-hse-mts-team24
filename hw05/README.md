# Практическое задание №5. Airflow


## Apache Airflow

```
# Настроен Spark c Yarn
# Запущен hive --service metastore
```

```
# Вход на сервер в jump node
# Переходим на name node под hadoop1
ssh team@176.109.91.35
ssh team-24-nn
sudo -i -u hadoop1
```


```
# Активируем ранее созданное в hw04 виртуальное окружение
# Установим зависимости
source .venv/bin/activate
pip install hdfs
pip install pendulum
pip install apache-airflow

deactivate
```


```
# Создадим путь до дагов
mkdir -p /home/hadoop1/airflow/dags
nano ~/.profile

export AIRFLOW_HOME=/home/hadoop1/airflow/dags

source ~/.profile
source .venv/bin/activate
```

```
# Создадим nano airflow/dags/my_dag.py
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import urllib.request
from pyspark.sql import SparkSession
import ssl

from onetl.connection import HDFS
from onetl.file import FileUploader
from onetl.db import DBWriter
from onetl.connection import Hive
import pendulum
import os

with DAG(
    "my_dag",
    start_date=pendulum.datetime(2025, 09, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["example"],
) as dag:
    local_data_path = "/home/hadoop1/input/titanic.csv"
    def extract_data():
        if not os.path.exists('/home/hadoop1/input/'):
            os.makedirs('/home/hadoop1/input/')
        ssl._create_default_https_context = ssl._create_unverified_context
        input_url = "https://calmcode.io/static/data/titanic.csv"

        urllib.request.urlretrieve(input_url, local_data_path)

    def load_data():
        spark = SparkSession.builder \
            .master("yarn") \
            .appName("spark-with-yarn") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("spark.hive.metastore.uris", "thrift://176.109.91.35:9084") \
            .enableHiveSupport() \
            .getOrCreate()

        hdfs = HDFS(host="192.168.1.99", port=9870)
        fu = FileUploader(connection=hdfs, target_path="/input")
        fu.run([local_data_path])

        df = spark.read.options(delimiter=",", header=True).csv("/input/titanic.csv")

        df = df.repartition(90, "age") 
        hive = Hive(spark=spark, cluster="test")
        writer = DBWriter(connection=hive, table="test.test_airflow", options={"if_exists": "replace_entire_table", "partitionBy": "age"})
        writer.run(df)

        spark.stop()

    extract_task = PythonOperator(task_id="extract_task", python_callable=extract_data)
    load_task = PythonOperator(task_id="load_task", python_callable=load_data)

    extract_task >> load_task
```



```
# Запустим airflow
airflow standalone


# В другом терминале на локальной машине
# Откроем туннель
ssh -L 8080:127.0.0.1:8080 team@176.109.91.35 -t ssh -L 8080:127.0.0.1:8080 192.168.1.99

# Зайдем на localhost:8080
```


```
# Таблица тут
hdfs dfs -ls /user/hive/warehouse/
```
