from ast import Pass

from datetime import datetime as dt

from time import sleep as sun



import json as j

import mysql.connector as mc

import requests as req


def asetup(con):
  table = 'book_of_life'
  
  table1 = 'book_of_life_jr'

  cols = (

    'id INT NOT NULL AUTO_INCREMENT',

    'address VARCHAR(100) NOT NULL',

    'balance VARCHAR(100) NOT NULL',

    'PRIMARY KEY(id)'

  )
  
  if not is_table(con, table):
    test = db_kr8_table(con, table, cols)
  else:
    test = True
    
  if not is_table(con, table1):
    test1 = db_kr8_table(con, table1, cols)
  else:
    test1 = True
    
  return test and test1


def db_ins(con, table, cols, val):
  try:

    if ((typeof(cols) == 'Tuple') and (typeof(val) == 'List')):

      cus = con.cursor()

      var = vls = ''

      for x in cols:

        var += f"{x}, "

        vls += "%s, "

      var = var[:-2]

      vls = vls[:-2]

      query = f"INSERT INTO {table} ({var}) VALUES ({vls})"



      cus.executemany(query, val)

      con.commit()

    else:

      return 'err 501(invalid content)'
      
  except mc.Error as e:

    return e.sqlstate



def db_kr8_table(con, table, cols):

  try:

    if typeof(cols) == 'Tuple':

      cus = con.cursor()

      var = ''

      for x in cols:

        var += f"{x}, "

      var = var[:-2]

      query = f"CREATE TABLE {table}({var})"

      cus.execute(query)

      con.commit()

      return is_table(con, table)

    else:

      return 'Error 501(invalid content)'

  except mc.Error as e:

    return e.sqlstate


def is_table(con, table):
  cus   = con.cursor()
  query = "SHOW TABLES"

  cus.execute(query)

  tables = cus.fetchall()

  for x in tables:


    if x[0] == table:

      return True

  return False


def to_json(api):

  raw = req.get(api)

  code = raw.status_code

  if code == 200:

    par = raw.text

    return j.loads(par)

  else:

    return code


def to_Json(api):

  raw = req.get(api)

  code = raw.status_code

  if code == 200:

    return raw.text

  else:

    return code



def typeof(data):

  if isinstance(data, (int)):

    return "Integer"

  elif isinstance(data, (str)):

    return "String"

  elif isinstance(data, (tuple)):

    return "Tuple"

  elif isinstance(data, (list)):

    return "List"

  elif isinstance(data, (dict)):

    return "Dict"



def main(con, hgt):

  time_ = dt(2016, 1, 1)

  limit = dt.timestamp(time_)



  api = f'https://btcscan.org/api/block-height/{hgt}'

  hsh = to_Json(api)



  api = f'https://btcscan.org/api/block/{hsh}/txids'

  trx = to_json(api)



  #i = 1

  for trn in trx:

    api = f'https://btcscan.org/api/tx/{trn}'

    txd = to_json(api)



    if 'scriptpubkey_address' in txd['vout'][0]:

      adr = txd['vout'][0]['scriptpubkey_address']

      atn = f"https://btcscan.org/api/address/{adr}/txs"

      atr = to_json(atn)

      if (atr[0]['status']['block_time'] < limit):

        bpi = f"https://api.blockcypher.com/v1/btc/main/addrs/{adr}/balance"

        bal = to_json(bpi)

        try:
        
          cols  = (
              'address',
              'balance'
            )

          if (bal['final_balance'] > 1000):
            
            table = book_of_life
            
            val   = [
              (adr, (bal['final_balance'] / 100000000))
            ]
            
            #print(f"{adr} -> {bal['final_balance'] / 100000000}")
            
            db_ins(con, table, cols, val)

          elif (bal['final_balance'] > 0):

            table = book_of_life_jr
            
            val   = [
              (adr, (bal['final_balance'] / 100000000))
            ]
            
            db_ins(con, table, cols, val)

        except:

          print(bal)



if __name__ == "__main__":

  con = mc.connect(

    host     = 'sql6.freesqldatabase.com',
    database = 'sql6521075',
    user     = 'sql6521075',
    password = '9sRn4eZQ4u'

  )

  if asetup(con):
    for i in range(100):
      main(con, i)



  