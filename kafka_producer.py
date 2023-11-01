
from confluent_kafka import Producer
import psycopg2

# Конфигурация Kafka producer
kafka_config = {
    'bootstrap.servers': 'kafka:9092',  # Укажите адрес вашего Kafka брокера
    'client.id': 'kafka-producer'
}


db_config = {'dbname':'de_db',
            'user':'de_app',
            'password':'de_password',
            'host':'localhost',
            'port':'5432'}

# Имя топика Kafka, в который будут отправляться сообщения
kafka_topic = 'your_kafka_topic'

# Установка соединения с Kafka producer
producer = Producer(kafka_config)

try:
    # Установка соединения с базой данных
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()

    # Извлечение данных из базы данных (пример: из таблицы "products")
    cursor.execute("SELECT id, name FROM table_products")
    products = cursor.fetchall()

    # Отправка сообщений в Kafka для каждого продукта
    for product_id, product_name in products:
        # Создание сообщения, которое включает ID продукта
        message = f"Product ID: {product_id}, Name: {product_name}"

        # Отправка сообщения в топик Kafka
        producer.produce(kafka_topic, key=str(product_id), value=message)

    # Ожидание, пока все сообщения будут отправлены
    producer.flush()

    # Закрытие соединения с базой данных
    cursor.close()
    connection.close()

except (Exception, psycopg2.Error) as error:
    print("Ошибка при работе с базой данных:", error)
except Exception as error:
    print("Ошибка при отправке сообщений в Kafka:", error)
