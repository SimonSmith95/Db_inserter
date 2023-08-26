import mysql.connector
import numpy as np
import pandas as pd
from PyQt5.QtWidgets import QApplication, QErrorMessage


class DbInserter:
    def __init__(self, hup_list):
        """
        This class is used to insert a dataframe into a sql database.
        :param hup_list: [host, username, password]
        """
        # Input params
        self.host = hup_list[0]
        self.user = hup_list[1]
        self.pwd = hup_list[2]
        self.database_name = 'name_of_database'
        self.table_name = 'name_of_table_in_database'
        self.connection_open = False

    def insert_df(self, df):
        try:
            # Making a comma separated string with all the columns in the dataframe.
            columns = ', '.join(df.columns)
            placeholders = ['%s' for i in df.columns]
            placeholders = ', '.join(placeholders)
            query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});"
            chunk_size = 1000
            for _, chunk in df.groupby(np.arange(len(df)) // chunk_size):
                # Making the chunk of data into a list of tuples.
                values = [tuple(x) for x in chunk.to_numpy()]
                connection = mysql.connector.connect(host=self.host, user=self.user, password=self.pwd,
                                                     database=self.database_name)
                cursor = connection.cursor()
                # Keeping track if connection is open to close if error.
                self.connection_open = True
                # Inserting the data.
                cursor.executemany(query, values)
                connection.commit()
                del values
                # Closing the connection to free up database resource between chunks.
                cursor.close()
                connection.close()
                self.connection_open = False


        except mysql.connector.Error as error:
            connection.rollback()
            # Open window to inform user of insertion error.
            self.inform_of_error(error)

        finally:
            if self.connection_open:
                cursor.close()
                connection.close()

    @staticmethod
    def inform_of_error(error):
        app = QApplication([])
        error_dialog = QErrorMessage()
        error_dialog.showMessage(f'Insertion into database failed! (error: {error})')
        app.exec_()



