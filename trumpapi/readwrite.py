# -*- coding: utf-8 -*-

from equitable.infrastructure import sysenv
from process import SymbolCacher

from equitable.db.psyw import SmartDB
import datetime as dt
from equitable.errors import existence as ex
def _DB_con_maker(DBcon = None):
    """If given a Database Connection, it will return the same.  If not, it will 
    connect to Trump, and retun a connection to that.
    """
    if DBcon is None:
        DBcon = SmartDB('Trump')
    return DBcon

def BuildSQL_Series_Query(Security,DBcon = None):
    db = _DB_con_maker(DBcon)
    s = Security.lower()
    
    if db.table_exists(s):
        raise  ex.NoSecurity(Security)
        
    return "SELECT date,datetime,final FROM " + s + ";"
    
def GetDescription(Security,DBcon = None):
    db = _DB_con_maker(DBcon)
    s = Security.lower()
    if not db.table_exists(s):
        raise  ex.NoSecurity(Security)
    q = "SELECT description FROM _symbolinfo WHERE symbol = '" + s + "';"
    return db.getOneCell(q)

def BuildSQL_Raw_Query(Security,DBcon = None):
    db = _DB_con_maker(DBcon)
    s = Security.lower()

    if not db.table_exists(s):
        raise  ex.NoSecurity(Security)
    
    q = "SELECT column_name FROM information_schema.columns WHERE table_name = '" + s + "' and column_name LIKE 'vp_%' ORDER BY column_name;"
    vps = db.getOneColumn(q)
    vps = ",".join(vps)
    q = "SELECT date,datetime,final, " + str(vps) + " FROM " + Security + " ORDER BY datetime;"
    return q

def GetRaw(Security,DBcon = None):
    db = _DB_con_maker(DBcon)
    s = Security.lower()
    q = BuildSQL_Raw_Query(s,db)    
    data = db.getRowsDicts(q)
    return data

def _GetXasList(Security,X,DBcon = None):
    db = _DB_con_maker(DBcon)
    s = Security.lower()
    q = BuildSQL_Raw_Query(s,db)    
    data = db.getRowsLists(q)
    return [d[X] for d in data]

def GetDataAsList(Security,DBcon = None):
    return _GetXasList(Security,2,DBcon)

def GetDateTimeIndexAsList(Security,DBcon = None):
    return _GetXasList(Security,1,DBcon)

def GetDateIndexAsList(Security,DBcon = None):
    return _GetXasList(Security,0,DBcon)
   
def GetSeriesAsDF(Security,DBcon = None):
    import pandas as pd
    df = pd.DataFrame(_GetXasList(Security,slice(1,3),DBcon))
    df.columns = ['datetime_ind',Security]
    df['datetime_ind'] = pd.to_datetime(df['datetime_ind'])
    df = df.set_index('datetime_ind')
    return df

def GetSeries(Security,DBcon = None):
    import pandas as pd
    i,d = zip(*_GetXasList(Security,slice(1,3),DBcon))
    i = pd.to_datetime(i)
    s = pd.Series(data=d,index=i)
    return s

def InsertOverRide(Security,DateTimeStamp,MasterOR='Null',FailSafeOR='Null',Username="Not Specified",Comment="No Comment",DBcon = None):
    db = _DB_con_maker(DBcon)
    s = Security.lower()
    
    if type(DateTimeStamp) is dt.datetime:
        ds = DateTimeStamp.strftime("%Y-%m-%d %H:%M:%s")
    else:
        ds = DateTimeStamp

    m = str(MasterOR)
    f = str(FailSafeOR)

    q = "INSERT INTO _humanoverride (symbol,datetime,masteror,failsafeor,comment,username) VALUES ('" + s + "','" + ds + "'," + m + "," + f + ",'" + Comment + "','" + Username + "');"
    db.ex(q)

    sc = SymbolCacher()
    sc.Process(s)
    
if __name__ == '__main__':
    s = GetSeries('price_46428d108_ca')
    #print BuildSQL_Series_Query('CADUSD')
    #print BuildSQL_Raw_Query('CADUSD')
    #print GetRaw('CADUSD')
    #print GetDataAsList('CADUSD')
#    d = GetSeriesAsDF('CADUSD')
#    print d.head(15)
#    InsertOverRide('CADUSD','1950-10-09',FailSafeOR=1.1,Username="Not Specified",Comment="No Comment")
#    d = GetSeriesAsDF('CADUSD')
#    print d.head(15)    
     #print GetSeriesAsDF('price_px_last_46428d108_ca')