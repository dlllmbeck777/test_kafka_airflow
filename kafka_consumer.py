from confluent_kafka import Consumer, KafkaError
import psycopg2
from datetime import datetime

# Конфигурация Kafka consumer
kafka_config = {
    'bootstrap.servers': 'kafka:9092',  # Укажите адрес вашего Kafka брокера
    'group.id': 'kafka-consumer-group',
    'auto.offset.reset': 'earliest',  # Начните с самого первого доступного сообщения
    'enable.auto.commit': True

}

# Имя топика Kafka, который вы хотите обработать
kafka_topic = 'your_kafka_topic'

# Конфигурация PostgreSQL
db_config = {'dbname':'de_db',
            'user':'de_app',
            'password':'de_password',
            'host':'localhost',
            'port':'5432'}


# Создайте Kafka consumer
consumer = Consumer(kafka_config)

# Подпишитесь на топик
consumer.subscribe([kafka_topic])

# Установите соединение с базой данных
connection = psycopg2.connect(**db_config)
cursor = connection.cursor()
cnt = 1

try:
    while True:
        msg = consumer.poll(1.0)  # Чтение сообщения с таймаутом 1 секунда

        if msg is None:
            # continue
            break

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'Получено конец раздела: {msg.topic()} [{msg.partition()}]')
            else:
                print(f'Ошибка при чтении сообщения: {msg.error()}')
        else:
            # Получите ID продукта из сообщения (предположим, что ID продукта находится в ключе сообщения)
            product_id = msg.key()

            # Получите текущую дату и время
            current_datetime = datetime.now()
            
            # print(cnt,' Product_id: ',product_id, ' current_datetime: ', current_datetime)

            # Запишите текущую дату в колонку processed_at в таблице с продуктами
            update_query = f"UPDATE table_products SET processed_at = '{current_datetime}' WHERE id::text = {product_id.decode('utf-8')}::text"
            cursor.execute(update_query)
            connection.commit()

            # Подтвердите прочтение сообщения (Ack)
            consumer.commit()
        # cnt+=1

except KeyboardInterrupt:
    pass

finally:
    # Закройте Kafka consumer
    consumer.close()

    # Закройте соединение с базой данных
    cursor.close()
    connection.close()