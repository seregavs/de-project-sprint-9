import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor
from cdm_loader.repository.cdm_repository import CdmRepository

app = Flask(__name__)

config = AppConfig()

@app.get('/health')
def hello_world():
    return 'healthy'

if __name__ == '__main__':
    app.logger.setLevel(logging.ERROR)

    # Инициализируем конфиг. Для удобства, вынесли логику получения значений переменных окружения в отдельный класс.
    config = AppConfig()

    proc = CdmMessageProcessor(
        consumer=config.kafka_consumer(),
        dds_repository=CdmRepository(config.pg_warehouse_db()),
        batch_size=100,        
        logger=app.logger
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)