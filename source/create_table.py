from unicodedata import name
import pandas as pd
import happybase as hb
import sys
import os
import datetime

# Create hbase table
if __name__ == '__main__':

    connection = hb.Connection('localhost')
    connection.open()
    connection.create_table('Ligue1', {'Metadata': dict(), 'Statistics': dict(), 'Betting': dict()})
    connection.close()

