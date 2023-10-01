from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError
from faker import Faker
import random


class EventHubStreaming:
    def __init__(self, cnn_string, event_hub_name) -> None:
        self.cnn_string = cnn_string
        self.event_hub_name = event_hub_name

    def generate_transactions_cc(self, transactions_quantity:int) -> list:
        """
        Generates a list of mock credit card transactions with random data.
        Args: 
            transactions_quantity (int): The number of transactions to generate.
        """
        try:
            fake = Faker()
            list_transactions = list()

            for _ in range(transactions_quantity):
                #UserInfo        
                username = fake.user_name()
                name = fake.name()
                email = fake.email()
                city = fake.city()
                country = fake.country()

                #CreditCard
                credit_card_number = fake.credit_card_number()
                credit_card_provider = fake.credit_card_provider()
                credit_card_expiration = fake.credit_card_expire()
                purchase_amount = round(random.uniform(10.0, 1000.0), 2)

                list_transactions.append({
                    'username': username,
                    'name': name,
                    'email': email,
                    'city': city,
                    'country': country,
                    'credit_card_number': credit_card_number,
                    'credit_card_provider': credit_card_provider,
                    'credit_card_expiration': credit_card_expiration,
                    'purchase_amount': purchase_amount
                })
            return list_transactions
        
        except Exception as e:
            print(f"Error: {str(e)}")
            
    
    def insert_batch_events(self, batch_size:int):
        """
        Inserts a batch of events into the Azure Event Hub.
        Args:
            batch_size (int): The number of events to generate and insert into the Event Hub. 
        """
        event_data_list = [EventData(format(i)) for i in self.generate_transactions_cc(batch_size)]
        producer = EventHubProducerClient.from_connection_string(conn_str=self.cnn_string, eventhub_name=self.event_hub_name)

        with producer:
            try:
                producer.send_batch(event_data_list)
            except ValueError:  # size exceeds limit.
                print("size of the event data list exceeds the size limit of a single send")
            except EventHubError as eh_err:
                print("sending error: ", eh_err)
            except Exception as e:
                print(f"Error: {str(e)}")

