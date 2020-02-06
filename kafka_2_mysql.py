import mysql.connector
import time
import json
import logging
from kafka import KafkaConsumer


class Kafka2MySQL:

    def __init__(self, cursor, cnx):
        self.cursor = cursor
        self.cnx = cnx

    def create_mysql_table(self, cursor):
        """Creates the `Classifieds` MySQL table if it does not already exist

        Parameters
        ----------
        cursor : MySQL cursor object

        Returns
        -------
        bool
            a boolean indicating query success/failure
        """
        try:
            cursor.execute("CREATE TABLE IF NOT EXISTS Classifieds ("
                           "id BINARY(16) PRIMARY KEY, "
                           "customer_id BINARY(16) NOT NULL,"
                           "created_at TIMESTAMP NOT NULL,"
                           "text TEXT NOT NULL,"
                           "ad_type ENUM('Free', 'Premium', 'Platinum') NOT NULL,"
                           "price DECIMAL(10,2) NOT NULL DEFAULT 0,"
                           "currency CHAR(3) NOT NULL,"
                           "payment_type VARCHAR(50) NOT NULL,"
                           "payment_cost DECIMAL(5,2) NOT NULL DEFAULT 0"
                           ") ENGINE=InnoDB DEFAULT CHARSET=utf8")
            logging.info('MySQL Table ready.')
            return True
        except mysql.connector.Error as err:
            print("MySQL Error: {}".format(err))
            return False

    def insert_into_classifieds(self, cursor, cnx, records_to_insert):
        """Inserts classifieds into the `Classifieds` MySQL table

        Parameters
        ----------
        cursor : MySQL cursor object
        cnx : A connection to the MySQL server
        records_to_insert : list
            List of records to INSERT (multiple INSERT)

        Returns
        -------
        bool
            a boolean indicating query success/failure
        """
        try:
            mysql_insert_query = """INSERT IGNORE INTO Classifieds (id, customer_id, created_at, text, ad_type, price, 
            currency, payment_type, payment_cost) VALUES (unhex(%s), unhex(%s), %s, %s, %s, %s, %s, %s, %s) """
            cursor.executemany(mysql_insert_query, records_to_insert)
            cnx.commit()
            logging.info('MySQL INSERT query OK.')
            return True
        except mysql.connector.Error as err:
            print("MySQL Error: {}".format(err))
            return False

    def iterate_kafka_job(self, cursor, cnx, kfk_server, kfk_topic):
        """Main function for handling Kafka messages and MySQL inserts

        Parameters
        ----------
        cursor : MySQL cursor object
        cnx : A connection to the MySQL server
        kfk_server : string (or list of ‘host[:port]’ strings)
            The server that the Kafka consumer should contact to bootstrap initial cluster metadata
        kfk_topic : string
            Kafka topic to subscribe to
        """
        # Create the Kafka Consumer and set to consume earliest messages and auto-commit offsets
        logging.info('Connecting to Kafka.')
        consumer = KafkaConsumer(
            kfk_topic,
            group_id='challenge_group',
            bootstrap_servers=[kfk_server],
            client_id='challenge_client',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )

        # List (buffer) for storing Kafka messages
        kafka_msg_buffer = []
        # Timer for counting 10 second intervals
        timer = time.time()
        for message in consumer:
            try:
                logging.info("offset=%d, value=%s" % (message.offset, message.value.decode('utf-8')))
                msg = json.loads(message.value.decode('utf-8'))
                if 'id' not in msg:
                    continue
                msg_id = msg["id"].strip()
                msg_customer_id = msg["customer_id"].strip()
                msg_created_at = msg["created_at"].strip()
                msg_text = msg["text"].strip()
                msg_ad_type = msg["ad_type"].strip()
                if msg_ad_type != 'Free':
                    msg_price = msg["price"]
                    msg_currency = msg["currency"].strip()
                    msg_payment_type = msg["payment_type"].strip()
                    msg_payment_cost = msg["payment_cost"]
                else:
                    msg_price = msg_currency = msg_payment_type = msg_payment_cost = ''

                # Data are now ready to be appended to the list
                kafka_msg_buffer.append((
                    msg_id, msg_customer_id, msg_created_at, msg_text, msg_ad_type, msg_price, msg_currency,
                    msg_payment_type, msg_payment_cost))

                # On times with low activity, check every 10 seconds if there is data on the buffer (list), then commit
                if (time.time() - timer > 10) and (len(kafka_msg_buffer) > 0):
                    logging.info('10 seconds passed. Dumping Kafka buffer to MySQL.')
                    self.insert_into_classifieds(cursor, cnx, kafka_msg_buffer)
                    kafka_msg_buffer = []
                    timer = time.time()

                # On times with high activity, commit as soon as the buffer reaches 100 messages
                elif len(kafka_msg_buffer) >= 100:
                    logging.info('Already received 100 messages. Dumping Kafka buffer to MySQL.')
                    self.insert_into_classifieds(cursor, cnx, kafka_msg_buffer)
                    kafka_msg_buffer = []

            except json.decoder.JSONDecodeError:
                print("Sorry, invalid JSON.")