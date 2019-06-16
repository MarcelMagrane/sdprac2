import pywren_ibm_cloud as pywren
import pika, os, random, json, sys


def my_function_master(n_slaves):
    global i
    config = json.loads(os.environ.get('PYWREN_CONFIG', ''))
    AMQP=config['rabbitmq']['amqp_url']
    params = pika.URLParameters(AMQP)
    connection = pika.BlockingConnection(params)
    
    channel = connection.channel() # start a channel
    channel.queue_declare(queue='ids') # Declare a queue
    
    channel.exchange_declare(exchange='valors', exchange_type='fanout')
    channel.queue_declare(queue='response')
    channel.queue_bind(exchange='valors', queue='response')
    i = 0
    
    def callback(channel, method_frame, header_frame, body):
        print(f'Sent to go: {body.decode("latin1")}')
        channel.basic_publish(exchange='', routing_key=body.decode("latin1"), body='go')
        channel.stop_consuming()
            
            
    def callback2(channel, method_frame, header_frame, body):
        print(f'Recieved number: {body.decode("latin1")}')
        channel.stop_consuming()
        
    
    while i < n_slaves:
        i = i+1
        channel.basic_consume(callback, queue='ids', no_ack=True)
        channel.start_consuming()
        channel.basic_consume(callback2, queue='response', no_ack=True)
        channel.start_consuming()
    
    print('Sent to stop all')
    channel.basic_publish(exchange='valors', routing_key='', body='stop')
    channel.queue_delete('ids')
    channel.queue_delete('response')
    connection.close()


def my_function_slave(ide):
    global ended
    config = json.loads(os.environ.get('PYWREN_CONFIG', ''))
    AMQP=config['rabbitmq']['amqp_url']
    params = pika.URLParameters(AMQP)
    connection = pika.BlockingConnection(params)
    
    channel = connection.channel() # start a channel
    channel.basic_publish(exchange='',
                      routing_key='ids',
                      body=str(ide))
    
    channel.exchange_declare(exchange='valors', exchange_type='fanout')

    channel.queue_declare(queue=str(ide))

    channel.queue_bind(exchange='valors', queue=str(ide))
    l=list()
    
    ended=0
    
    def callback(channel, method_frame, header_frame, body):
        global ended
        answer = body.decode("latin1")
        if answer == 'go':
            ended=1
            message = str(random.randint(1,1000))
            print(f'{ide} is sending random number to exchange valors: {message}')
            channel.basic_publish(exchange='valors', routing_key='', body=message)
        elif answer == 'stop':
            channel.stop_consuming()
            channel.queue_delete(str(ide))
        else:
            l.append(answer)
            if ended == 0:
                print(f'Sent again missing ids')
                channel.basic_publish(exchange='', routing_key='ids', body=str(ide))
    
    channel.basic_consume(callback, queue=str(ide), no_ack=True)
    channel.start_consuming()
    connection.close()
    
    return l


if __name__ == '__main__':
    total = int(sys.argv[1])
    pw = pywren.ibm_cf_executor(runtime_memory=256, rabbitmq_monitor=True)
    AMQP=pw.config['rabbitmq']['amqp_url']
    params = pika.URLParameters(AMQP)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange='valors', exchange_type='fanout')

    pw.call_async(my_function_master, total)
    pw.map(my_function_slave, range(total))
    
    llista=(pw.get_result())
    
    for line in llista:
        print(line)
    
    print(len(llista))



