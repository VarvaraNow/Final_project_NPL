import json
import boto3
import zipfile
import pendulum
import clickhouse_driver
from io import BytesIO
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
@dag(
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 4, 10, 23),
    max_active_runs=1,
    catchup=True
)
def insert_yandex_s3_to_clickhouse():
    @task
    def s3_to_clickhouse():
        def date_column_to_datetime(list_with_rows):
            '''
            Функция для перевода колонок str date в datetime
            если в наименовании есть timestamp
            '''
            # Ищем есть ли timestamp колонки
            list_columns_timestamp = [x for x in list_with_rows[0].keys()
                                      if 'timestamp' in x]
            # Трансформируем значения, если есть timestamp колонки
            for row in list_with_rows:
                for column in list_columns_timestamp:
                    try:
                        row[column] = datetime.strptime(row[column],
                                                        '%Y-%m-%d %H:%M:%S.%f')
                    except:
                        row[column] = datetime.strptime(row[column],
                                                        '%Y-%m-%d %H:%M:%S')
            return list_with_rows
        # Открываем config файл с переменными для работы скрипта 
        config = json.load(open('dags/config_lab06.json'))
        # Подключаемся к кликхаусу
        client = clickhouse_driver.Client(host=config['click_ip'],
                                          port=config['click_port'])
        # Создаем сессию в Yandex Cloud
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            region_name=config['s3_region'],
            endpoint_url=config['s3_url'],
            aws_access_key_id=config['s3_key_id'],
            aws_secret_access_key=config['s3_key_access'])
        # Формируем путь к файлу в бакете в зависимости 
        # от даты за которую грузим данные
        context = get_current_context()
        start = context["data_interval_start"]
        print(f"Начинаем работу за {start}")
        date_part_key = start.strftime("year=%Y/month=%m/day=%d/hour=%H/")
        
        # Итерируемся по каждой таблице, cкачиваем все данные
        # за конкретный час, далее вставляем в БД
        for name_data in config['list_tables']:
            # Формируем переменные
            name_file = name_data + '.jsonl' # Наименование файла
            name_table = f"default.{name_data}_lab06" # Наименование таблицы
            key = date_part_key + name_file + '.zip' # Ключ к файлу в бакете
            print(f"Подключаюсь к {key}")
            # Подключаемся к бакету
            response_s3 = s3_client.get_object(Bucket=config['s3_bucket'], Key=key)
            # Распаковываем аврхив
            zip_bytes = BytesIO(response_s3["Body"].read())
            zip_data = zipfile.ZipFile(zip_bytes)
            # Открываем jsonl файл
            jsonl_data = zip_data.open(name_file)
            list_rows = jsonl_data.read().decode('utf-8').splitlines()
            # Обрабатываем json в лист со словарями
            data_for_click = [json.loads(line) for line in list_rows]
            # Транфсормируем даты в формат datetime
            data_for_click = date_column_to_datetime(data_for_click)
            print(f"Количество строк: {len(data_for_click)}")
            # Отправляем данные в нужную таблицу
            response_click = client.execute(f"INSERT into {name_table} VALUES",
                                            data_for_click)
            print(f"Добавлено {response_click} строк в {name_table}")
    s3_to_clickhouse()
run = insert_yandex_s3_to_clickhouse()