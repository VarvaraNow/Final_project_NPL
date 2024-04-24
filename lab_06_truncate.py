import pendulum
import clickhouse_driver
from airflow.decorators import dag, task

@dag(
    start_date=datetime.datetime(2024, 4, 10, 23),
    catchup=False
)
def truncate_clickhouse():
    @task
    def truncate():
        # Открываем config файл с переменными для работы скрипта 
        config = json.load(open('dags/config_lab06.json'))
        # Подключаемся к кликхаусу
        client = clickhouse_driver.Client(host=config['click_ip'],
                                          port=config['click_port'])
        for name_data in config['list_tables']:
            name_table = f"default.{name_data}_lab06" # Наименование таблицы
            client.execute(f"TRUNCATE TABLE {name_table}")
    truncate()


actual_dag = truncate_clickhouse()