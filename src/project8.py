import psycopg2

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

TOPIC_NAME_IN = 'student.topic.cohort11.prepsiy_in'
TOPIC_NAME_OUT = 'student.topic.cohort11.prepsiy_out'


def create_table_target():

    conn = psycopg2.connect(dbname='de', user='jovyan', password='jovyan', host='localhost')

    cursor = conn.cursor()
    cursor.execute("""
        DROP TABLE IF EXISTS public.subscribers_feedback;
        
        CREATE TABLE public.subscribers_feedback (
            id serial4 NOT NULL,
            restaurant_id text NOT NULL,
            adv_campaign_id text NOT NULL,
            adv_campaign_content text NOT NULL,
            adv_campaign_owner text NOT NULL,
            adv_campaign_owner_contact text NOT NULL,
            adv_campaign_datetime_start int8 NOT NULL,
            adv_campaign_datetime_end int8 NOT NULL,
            datetime_created int8 NOT NULL,
            client_id text NOT NULL,
            trigger_datetime_created int4 NOT NULL,
            feedback varchar NULL,
            CONSTRAINT id_pk PRIMARY KEY (id)
        );
    """)
    conn.commit()

    cursor.close()
    conn.close()

# Метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):

    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()

    # записываем df в PostgreSQL с полем feedback
    df.withColumn('feedback', lit('')) \
        .write \
        .mode('append') \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://localhost:5432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'public.subscribers_feedback') \
        .option('user', 'jovyan') \
        .option('password', 'jovyan') \
        .save()


    # создаём df для отправки в Kafka. Сериализация в json.
    kafka_df = df.select(to_json(
        struct('restaurant_id',
               'adv_campaign_id',
               'adv_campaign_content',
               'adv_campaign_owner',
               'adv_campaign_owner_contact',
               'adv_campaign_datetime_start',
               'adv_campaign_datetime_end',
               'client_id',
               'datetime_created',
               'trigger_datetime_created')) \
        .alias('value'))

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    kafka_df.write \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('kafka.sasl.jaas.config',
                'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";') \
        .option('topic', TOPIC_NAME_OUT) \
        .save()

    # очищаем память от df
    df.unpersist()

# Необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# Создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# Читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";') \
    .option('subscribe', TOPIC_NAME_IN) \
    .load()

# Определяем схему входного сообщения для json
incomming_message_schema = StructType([
        StructField("restaurant_id", StringType()),
        StructField("adv_campaign_id", StringType()),
        StructField("adv_campaign_content", StringType()),
        StructField("adv_campaign_owner", StringType()),
        StructField("adv_campaign_owner_contact", StringType()),
        StructField("datetime_created", LongType()),
        StructField("adv_campaign_datetime_start", LongType()),
        StructField("adv_campaign_datetime_end", LongType()),
    ])

# Определяем текущее время в UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

# Десериализуем из value сообщения json
# Фильтруем по времени старта и окончания акции
filtered_read_stream_df = restaurant_read_stream_df \
    .withColumn('value', col('value').cast(StringType())) \
    .withColumn('value', from_json(col('value'), incomming_message_schema)) \
    .selectExpr('value.*') \
    .filter(lit(current_timestamp_utc).between(col('adv_campaign_datetime_start'), col('adv_campaign_datetime_end')))

# Вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
    .option('driver', 'org.postgresql.Driver') \
    .option('dbtable', 'subscribers_restaurants') \
    .option('user', 'student') \
    .option('password', 'de-student') \
    .load() \
    .select('restaurant_id', 'client_id') \
    .distinct()

# Джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid).
# Добавляем время создания события.
result_df = filtered_read_stream_df \
    .join(subscribers_restaurant_df, 'restaurant_id', 'inner') \
    .withColumn('trigger_datetime_created',  lit(current_timestamp_utc))

# Создание таргет-таблицы
create_table_target()

# Запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()