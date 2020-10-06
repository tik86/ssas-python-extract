import arrow
import clr
import re
import System.Data.OleDb as ADONET
import os
import psycopg2
import pandas as pd
import csv
import datetime



class Ssas:
    def __init__(self):
     self.data = []
     self.headers = []
     self.status = ""

    def debug(self, text, on=False):
        if on:
            print(text)

    def execute(self, query, server, catalog, date_format, datestart, dateend,):
        debugging = True

        self.status = "Connecting to Ssas"
        self.debug("Connecting to Ssas", debugging)

        connStr = "Provider=MSOLAP.8; Data Source='{0}'; Initial Catalog='{1}'; ".format(server, catalog)
        connection = ADONET.OleDbConnection(connStr)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandTimeout = 0;

        startDtMdx = datestart
        endDtMdx = dateend

        command.CommandText = query % (startDtMdx, endDtMdx)

        self.debug("Executing Query", debugging)

        rows = command.ExecuteReader()

        self.data = []

        for row in rows:
            self.data.append([row[i] for i in range(rows.FieldCount)])

        d = 0
        for lst in self.data:
            i = 0
            for item in lst:
                if re.match('^(\[)', str(item)):
                    self.data[d].pop(i)
                i += 1
            d += 1

        data_list = []

        for i, x in enumerate(self.data):
            x = map(str, x)
            x = [s.replace(',', '\,') for s in x]
            joiner = '","'.join(x)
            st = '"' + joiner + '"\n'
            data_list.append(st)

        self.data = data_list
        # self.debug(self.data, debugging)

        cols = [rows.GetName(i) for i in range(rows.FieldCount)]

        # Remove any Square brackets from list
        ic = 0
        for i in cols:
            for repl in ['[', ']']:
                i = i.replace(repl, '')
                cols[ic] = i.replace(repl, '')
            ic += 1

        # split list into list of lists remove anything containing the word caption
        for idx, val in enumerate(cols):
            if 'MEMBER_CAPTION' in val:
                cols.pop(idx)

            cols[idx] = cols[idx].split('.')

            for i, v in enumerate(cols[idx]):
                if v == 'MEMBER_UNIQUE_NAME':
                    cols[idx].pop(i)

        # get last item from array is the header
        cols2 = [x[-1].replace(' ', '_') for x in cols]

        self.headers = ','.join(map(str, cols2)) + '\n'
        #print(self.headers)

        self.data.insert(0, self.headers)

        self.write_to_file('test', self.data)

    def get(self, query, server, catalog, date_format, datestart, dateend, individual=False):

        if individual:
            # Generate a list of dates
            start = datetime.datetime(2017,7,1)
            end = datetime.datetime(2019,1,15)

            index = pd.date_range(start, end)

            datelist = [arrow.get(str(x), 'YYYY-MM-DD HH:mm:ss').format('YYYYMMDD') for x in index]

            for dt in datelist:
                print(dt)
                self.execute(query, server, catalog, date_format, dt, dt)

        self.execute(query, server, catalog, date_format, datestart, dateend)

    def write_to_file(self, name, data):

        f = open('./Ssas/data/{0}.csv'.format(name), 'wb')
        for text in data:
            f.write(text.encode('utf-8'))
        f.close()

        df = pd.read_csv('./Ssas/data/{0}.csv'.format(name), usecols=[0,7,8,9,10,11])

        df.to_csv('./Ssas/data/{0}.csv'.format(name), encoding='utf-8', index=False)

        self.insert_to_database()

    def insert_to_database(self):
        conn = psycopg2.connect("dbname='dbname' user='user' password = 'pw' host = 'host'")
        cur = conn.cursor()

        textData = open("./Ssas/data/test.csv", "r")
        cur.copy_expert("COPY Ssas.mae_all FROM STDOUT NULL '' CSV HEADER" , textData)
        conn.commit()
        print("Insert Complete")