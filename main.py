from kafka_2_mysql import Kafka2MySQL
import mysql.connector
import configparser
import argparse
import logging


def main():
    # Provide a '-v' or '--verbose' argument for the script
    parser = argparse.ArgumentParser(description='XE Data Engineer Challenge - as tackled by Fotis Ntouskas')
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    # Load connections details from config file
    config = configparser.ConfigParser()
    config.read('config.ini')
    mysql_host = config['MYSQL']['HOST']
    mysql_schema = config['MYSQL']['DB']
    mysql_uid = config['MYSQL']['USERNAME']
    mysql_pass = config['MYSQL']['PASSWORD']
    kafka_server = config['KAFKA']['SERVER']
    kafka_topic = config['KAFKA']['TOPIC']
    logging.info('Config loaded.')

    try:
        logging.info('Connecting to MySQL.')
        # Connect to MySQL Server
        cnx = mysql.connector.connect(host=mysql_host, user=mysql_uid, passwd=mysql_pass, database=mysql_schema)
        cnx.set_charset_collation(charset='utf8')
        cursor = cnx.cursor()
        logging.info('Connection to MySQL established.')

        # Create class instance
        kafka_2_mysql = Kafka2MySQL(cursor, cnx)
        # Create MySQL Table if not exists
        kafka_2_mysql.create_mysql_table(cursor)
        # Initiate main job
        kafka_2_mysql.iterate_kafka_job(cursor, cnx, kafka_server, kafka_topic)

        cnx.close()

    except mysql.connector.Error as er:
        print("MySQL Error: {}".format(er))


if __name__ == "__main__":
    main()
