import unittest
from kafka_2_mysql import Kafka2MySQL
import mysql.connector
import configparser


class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Load connections details from config file
        config = configparser.ConfigParser()
        config.read('config.ini')
        mysql_host = config['MYSQL']['HOST']
        mysql_schema = config['MYSQL']['DB']
        mysql_uid = config['MYSQL']['USERNAME']
        mysql_pass = config['MYSQL']['PASSWORD']
        kafka_server = config['KAFKA']['SERVER']
        kafka_topic = config['KAFKA']['TOPIC']

        # Connect to MySQL Server
        try:
            cls.cnx = mysql.connector.connect(host=mysql_host, user=mysql_uid,
                                              passwd=mysql_pass, database=mysql_schema)
            cls.cnx.set_charset_collation(charset='utf8')
            cls.cursor = cls.cnx.cursor()
        except mysql.connector.Error as er:
            print("MySQL Error: {}".format(er))

        cls.test_object = Kafka2MySQL(cls.cursor, cls.cnx)

    @classmethod
    def tearDownClass(cls):
        cls.cursor.execute("DELETE FROM Classifieds WHERE HEX(id)='0123456789abcdeffedcba9876543210' AND "
                            "text = 'THIS ROW WAS INSERTED BY UNIT TESTS -- PLEASE IGNORE'")
        cls.cnx.commit()
        cls.cnx.close()

    def test_create_mysql_table(self):
        self.assertEqual(self.test_object.create_mysql_table(self.cursor), True)

    def test_insert_into_classifieds(self):
        kafka_msg_buffer = [['0123456789abcdeffedcba9876543210', '0123456789abcdeffedcba9876543210',
                             '1985-02-06T10:30:00.0000000Z', 'THIS ROW WAS INSERTED BY UNIT TESTS -- PLEASE IGNORE',
                             'Free', '', '', '', '']]
        self.assertEqual(self.test_object.insert_into_classifieds(self.cursor, self.cnx, kafka_msg_buffer), True)


if __name__ == '__main__':
    unittest.main()
