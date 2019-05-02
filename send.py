import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='BB')
msg = "#".join(sys.argv[1:4])
print(msg)
channel.basic_publish(exchange='', routing_key='BB', body=msg)
print(" [x] Sent '{} - {}' ".format(sys.argv[1],sys.argv[2]))
connection.close()
