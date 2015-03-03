
import pandas as pd
from pandas.core import strings as ps
from os import listdir
from os.path import isfile, join
from equitable.bpsdl.bbconfig.Folders import SECURITYSOURCES as s
from equitable.bpsdl.pymti import PI_SearchByAnidYellowFast as elidsearch
    
class EconBloombergList(object):
    def __init__(self):
        files = [f for f in listdir(s) if isfile(join(s,f))]
        filecontents = []
        for f in files:
            with open(join(s,f)) as fi:
                filecontents.append(fi.readlines())
        
        filecontents = [item.replace("\n","").upper().split("|") for sublist in filecontents for item in sublist]
        
        df = pd.DataFrame(filecontents)
        df = df.drop([2,4,5,6,7],axis=1)
        df.columns = ['BB','Field','eSym']
        df.Field = ps.str_replace(df.Field," ","_")
        
        df['YellowKey'] = [x.split(" ")[-1] for x in df.BB]
        df['BB'] = [" ".join(x.split(" ")[:-1]) for x in df.BB]
        
        df = df.sort(['BB','YellowKey','Field','eSym'])
        df = df.set_index(['BB','YellowKey','Field'])
        df = df.drop_duplicates()
        
        df['elid'] = [None] * len(df)
        
        self.bbtoecon = df
        self.econtobb = df.reset_index().set_index('eSym')
    def PrintLen(self,n):
        for i in self.bbtoecon.index:
            if len(self.bbtoecon.xs(i)) >= n:
                print self.bbtoecon.xs(i)
    def eSymbols(self):
        for i in self.bbtoecon.index:
            yield i[0],i[1],i[2],list(self.bbtoecon.xs(i)['eSym'])
    def bbSymbols(self):
        for i in self.econtobb.index:
            yield i,self.econtobb.loc[i]
    def bbSymbol(self,eSym):
        tmp = self.econtobb.loc[eSym]
        return tmp.BB,tmp.YellowKey,tmp.Field
    def eSymbol(self,BB,YellowKey,Field):
        """returns a list of econ table symbols"""
        return list(self.bbtoecon.xs((BB,YellowKey,Field))['eSym'])
    def elidfrombb(self,BB,YellowKey):
        return elidsearch(Presets=[BB,YellowKey.title()],AllowPrompt=False).GetID()
    def elidfromecon(self,eSym):
        BB,YellowKey,_ = self.bbSymbolAndField(eSym)
        guess = elidsearch(Presets=[BB]).GetID()
        if guess:
            return guess
        return elidsearch(Presets=[BB,YellowKey.title()],AllowPrompt=False).GetID()
    def bbSymbolAndField(self,eSym):
        """returns a tuple of the bloomberg symbol with yellow key and field"""
        tmp = eSym.upper()
        tmp = self.econtobb.loc[tmp]
        return tmp.BB,tmp.YellowKey,tmp.Field
    def econexists(self,eSym):
        tmp = eSym.upper()    
        return tmp in self.econtobb.index

freqmap = {"ANNUAL" : 'A',
           "BUSINESS" : 'B',
           "d" : 'D',
           "D" : 'D',
           "DAILY" : 'D',
           "m" : 'M',
           "M" : 'M',
           "MONTHLY" : 'M',
           "q" : 'Q',
           "Q" : 'Q',
           "QUARTERLY" : 'Q',
           "W" : 'W',
           "WEEKLY(FRIDAY)" : 'W-FRI',
           "WEEKLY(MONDAY)" : 'W-MON',
           "WEEKLY(WEDNESDAY)" : 'W-WED',
           "y" : 'A',
           "Y" : 'A'}

from equitable.db import pgw

def PickAppropriateFreq(d):
    mx = max(d)
    mn = min(d)
    
    Checked = []
    for freq in ['BAS','AS','BA','A','BQS','QS','BQ','Q','BMS','MS','BM','M','W','B','D']:
        Checked.append(freq)
        desired_indx = pd.date_range(start=mn,end=mx,freq=freq)
        new_indx_s = set(desired_indx)
        cur_indx_s = set(d)
        
        if cur_indx_s.issubset(new_indx_s):
            print "Checked frequencies : {0}".format(", ".join(Checked))
            return freq
    return None
                    
def ConfirmFreq(eSym,freq,verbose=False):
    #TODO Make this connection happen once, instead of n times.
    g = pgw.dbConnect('General')
    q = "SELECT date FROM econ WHERE name ='{}' ORDER BY date;".format(eSym)
    dates = g.query(q).getresult()
    dates = [d[0] for d in dates]
    dates = pd.to_datetime(dates)
    
    if len(dates) != 0:
        desired_indx = pd.date_range(start=min(dates),end=max(dates),freq=freq)
        
        new_indx_s = set(desired_indx)
        cur_indx_s = set(dates)
        
        if not cur_indx_s.issubset(new_indx_s):
            msg = "Reindexing will cause dataloss for : {0}\n".format(eSym)
            bad_dates = new_indx_s.difference(cur_indx_s)
            bad_dates = ",\n".join([str(d) for d in bad_dates])
            msg += "Data associated with {0} would be dropped. The data is supposed to be frequency {1}".format(bad_dates,freq)
            if verbose:
                print msg
            return PickAppropriateFreq(dates)
        else:
            return freq
    else:
        return None
    
    
        
if __name__ == '__main__':
    #ebl = EconBloombergList()
#    for a,b,c,d in ebl.eSymbols():
#        print a,b,c
#        print "  " + ", ".join(d)
#        print ""
#    for a,b in ebl.bbSymbols():
#        print a,b.BB,b.YellowKey,b.Field
    print ConfirmFreq('COINCIDENT.US','BM')
#,columns = ["BB","Field","Freq","EconSym"]

#GeneralDB = pgw.dbConnect('General')
#qry = "SELECT DISTINCT name FROM econ_info WHERE source = 'BLOOMBERG' ORDER BY name;"
#econs = GeneralDB.query(qry).getresult()
#GeneralDB.close()
#
#BloombergDB = pgw.dbConnect('Bloomberg')
#qry = "SELECT DISTINCT anid FROM bpsdl_masterseclist;"
#bbs = BloombergDB.query(qry).getresult()
#BloombergDB.close()
#
#econs = [e[0] for e in econs]
#bbs = [b[0] for b in bbs]
