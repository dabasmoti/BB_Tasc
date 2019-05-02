import pika
from main import main 


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='BB')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    main(body)


channel.basic_consume(
    queue='BB', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
