"""bbq_producer is a producer that is streaming
smoker temperatures for the smoker and 2 different foods.
Each temperature is sent to a different queue and picked 
up by that queue's listener.
Author: Zak Davlin """
#Import needed Modules
import pika
import sys
import webbrowser
import csv
import time
#Show Rabbit MQ admin page if show_offer is True
def offer_rabbitmq_admin_site(show_offer):
    """Offer to open the RabbitMQ Admin website"""
    if show_offer==True:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()
#Declare a host name
host="localhost"
#Set Queueclear to true to clear the Queue
Queueclear=True
def offer_Queueclear(Queueclear):
    """Offer to open the RabbitMQ Admin website"""
    if Queueclear==True:
        ans = input("Would you like to clear the queues? y or n ")
        print()
        if ans.lower() == "y":
            # create a blocking connection to the RabbitMQ server
            conn = pika.BlockingConnection(pika.ConnectionParameters(host))
            # use the connection to create a communication channel
            ch = conn.channel()
            #delete the queues
            ch.queue_delete("01-smoker")
            ch.queue_delete("02-food-A")
            ch.queue_delete("03-food-B")
            print("Queue cleared!")

offer_Queueclear(Queueclear)
""" Create csv function to read from file and turn it into a message"""
#opens smoker File
with open("smoker-temps.csv",'r') as file:
    reader=csv.reader(file,delimiter=",")
#read in rows 
    for row in reader:
     #Read just the timestamp and store it
        fstringtime=f"{row[0]}"
        #Read smoker reading and store it
        smokertemp=f"{row[1]}"
        #Read Food A reading and store it
        foodA=f"{row[2]}"
        #Read Food B reading and store it
        foodB=f"{row[3]}"
        #Set up messages 
        smoker_message=f"{fstringtime},{smokertemp}"
        foodA_message=f"{fstringtime},{foodA}"
        foodB_message=f"{fstringtime},{foodB}"
        #Get those messages rolling out!
        send_message(host,"01-smoker",smoker_message)
        send_message(host,"02-food-A",foodA_message)
        send_message(host,"03-food-B",foodB_message)
        #Slow down the output to one message every 30 seconds
        time.sleep(30)

