import uuid
import pickle
import pika
from flask import Flask
from flask import request, abort


RMQ_QUEUE = 'demormq'
RMQ_HOST = 'localhost'

app = Flask(__name__)


def publish_message(rmq_host, rmq_queue, rmq_message):
    """
    Helper for publishing messages to RabbitMQ
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rmq_host))
    channel = connection.channel()
    channel.queue_declare(queue=rmq_queue,
                          durable=True,
                          exclusive=False,
                          auto_delete=False)

    channel.basic_publish(exchange='',
                          routing_key=rmq_queue,
                          body=rmq_message)
    connection.close()


@app.route('/incoming', methods=['POST'])
def post_incoming_handler():
    """
    /incoming URI processor
    """
    if request.is_json:
        data_obj = dict()
        json_obj = request.get_json()
        data_obj['uuid'] = str(uuid.uuid4())
        data_obj['data'] = json_obj
        rmq_message = pickle.dumps(data_obj)
        publish_message(RMQ_HOST, RMQ_QUEUE, rmq_message)
        return "Object was published with tx_id '%s'" % (data_obj['uuid'])
    else:
        return abort(400)


if __name__ == "__main__":
    app.run()
