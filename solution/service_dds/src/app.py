import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from dds_loader.dds_message_processor_job import DdsMessageProcessor
from dds_loader.repository.dds_repository import DdsRepository

app = Flask(__name__)

config = AppConfig()


@app.get('/health')
def hello_world():
    return 'healthy'

if __name__ == '__main__':
    app.logger.setLevel(logging.ERROR)

    # Инициализируем конфиг. Для удобства, вынесли логику получения значений переменных окружения в отдельный класс.
    config = AppConfig()

    proc = DdsMessageProcessor(
        consumer=config.kafka_consumer(),
        producer=config.kafka_producer(),
        dds_repository=DdsRepository(config.pg_warehouse_db()),
        batch_size=100,        
        logger=app.logger
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
