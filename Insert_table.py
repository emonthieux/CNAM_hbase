import pandas as pd
import happybase as hb
import sys
import os
import datetime

folder = sys.argv[1]

Metadata = ["Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "HTHG", "HTAG", "HTR"]

Statistics = ["HS", "AS", "HST", "AST", "HC", "AC", "HF", "AF", "HY", "AY", "HR", "AR"]

Betting = ["B365H", "B365D", "B365A", "BWH", "BWD", "BWA", "IWH", "IWD", "IWA", "PSH", "PSD", "PSA", "WHH", "WHD", "WHA"]

def csv2hbase(df, table):
    for index, row in df.iterrows():
        # Get timestamp
        dt = datetime.datetime.strptime(row['date'], '%d/%m/%Y')
        ts = int(dt.timestamp())
        # Dict of data
        dict_data = {"Metadata:" + i: row[i] for i in Metadata}
        dict_data.update({"Statistics:" + i: row[i] for i in Statistics})
        dict_data.update({"Beting:" + i: row[i] for i in Betting})
        # Insert row
        table.put(row['HomeTeam'] + "-" + row['AwayTeam'], dict_data, timestamp=ts)

    
def parse_folder(folder, table):
    for files in os.listdir(folder):
        if files.endswith(".csv"):
            df = pd.read_csv(os.path.join(folder, files))
            csv2hbase(df, table)
    
def main():
    connection = hb.Connection('localhost')
    connection.open()
    table = connection.table('test')
    parse_folder(folder)
    connection.close()