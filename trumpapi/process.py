import pandas as pd

import datetime as dt

import equitable.errors.existence as errex

from equitable.infrastructure import sysenv
from equitable.infrastructure import sendemail as mail
from methods import TrumpMethods
from funcs import vp

import pickle as p

import sys, traceback

import hashlib as h

from equitable.bpsdl import Getter as bbf

from equitable.db.psyw import SmartDB

BBFetchAPI = bbf.BBapi(clean=False,IgnoreOS=True)

class EquitableData(object):
    def __init__(self):
        self.desired_indx = None #Set, just so that it will always exist.
        self.data = None
    def FetchData(self,ins):
        DuplicateIndexHandler = None #Possible Pycrit Parameter
        indexfilter = None #Possible Pycrit Parameter

        for i in range(1,4):
            if ins['pycrit' + str(i)]:
                print "  Running PyCrit : " + ins['pycrit' + str(i)][:20] + "..."
                exec(ins['pycrit' + str(i)])

        if ins['instructiontype'] == 'BBFetch':
            security = BBFetchAPI.GetSecurity(ins['elid'])
            self.data = security.GetDataMostRecentFetchDaily(ins['valuefieldname'],KeepTimeZone=False)
        elif ins['instructiontype'] == 'BBFetchBulk':
                   
            security = BBFetchAPI.GetSecurity(ins['elid'])
            self.data = security.GetDataMostRecentFetchBulk(ins['valuefieldname'],indexfilter=indexfilter,columntoindex=ins['datefieldname'],datacolumn=ins['keyfieldvalue'])
                           
        elif ins['instructiontype'] == 'DB':
            
            db = ins['database']
            
            self.db = SmartDB(db)
            
            if ins['sqlcrit']:
                oc = "AND " + ins['sqlcrit']
            else:
                oc = ''
            
            if ins['keyfieldname']:
                where = " WHERE " + ins['keyfieldname'] + "='" + ins['keyfieldvalue'] + "' "
            else:
                where = ''
                
            q = "SELECT " + ins['datefieldname'] + "," + ins['valuefieldname'] + " FROM " + ins['tablename'] + where + oc + " ORDER BY " + ins['datefieldname'] + ";"
            
            results = self.db.getRowsTuples(q)

            if len(results) == 0:
                raise errex.NoData("Problem with init for EquitableData, no results from: "+q)
    
            data = [row[1] for row in results]
            #indx = [dt.datetime.strptime(row[0],'%Y-%m-%d') for row in results]
            #indx = [dt.datetime.strptime(parse(row[0]).strftime('%Y-%m-%d'),'%Y-%m-%d') for row in results]
            indx = [row[0] for row in results]
            self.data = pd.Series(data,indx)
            self.data = self.data.convert_objects(convert_numeric=True)

        if DuplicateIndexHandler == 'sum':
            print "  aggregate"
            self.data = self.data.groupby(self.data.index).sum()

        mult = float(ins['unitmult'])
        if mult != 1.0:
            self.data = self.data * mult
        
        if ins['unitparse'] == "^-1":
            print "  Doing inverse"
            self.data = 1 / self.data

        self.freqparse = ins['freqparse']
        
        if '-reset' in ins['freqparse']:      
            
            
            print "  Reseting index using " + ins['freqparse']

            datepoint = self.data.index[0]
            
            # Note IsIntance() won't work here.  We want to be more explicit
            # than the subclass level.
            if type(datepoint) == dt.date:
                strt,end = self.data.index[0], self.data.index[-1]
            elif type(datepoint) == dt.datetime:
                strt,end = self.data.index[0].date(), self.data.index[-1].date()
            elif type(datepoint) == pd.tslib.Timestamp:
                #TODO Confirm .date() is comprehensive for this datatype
                strt,end = self.data.index[0].date(), self.data.index[-1].date()
                dayonlyindex = [pd.to_datetime(d).date() for d in self.data.index]
                self.data = pd.Series(data=self.data.values,index=dayonlyindex)
            else:
                #TODO Make more cases, if things get caught here with ".date()" not working.
                strt,end = self.data.index[0].date(), self.data.index[-1].date()                
                
            f = ins['freqparse'].split("-")[0]
            self.desired_indx = pd.date_range(start=strt,end=end,freq=f)
            self.data = self.data.reindex(self.desired_indx)
        elif 'noreset' == ins['freqparse']:
            print "  Not resetting index, warning this is dangerous for several reasons."
            dayonlyindex = [pd.to_datetime(d).date() for d in self.data.index]
            self.data = pd.Series(data=self.data.values,index=dayonlyindex)
                
        print self.data.tail(3)

def SafeAsFreq(df,freq,method=None):
    
    # This was written, to work on all indicies.  Even, non-monotonic ones.
    # Really, Trump should convert all indexes to monotonic indexes, handle
    # those problems, then do the frequency conversion.
    
    # TODO Seperate Monotonic assumption from SafeAsFreq
    
    # More : Instead of trying to figure out the difference between mistmatched
    # frequencies and poorly indexed data all in one go, considering
    # reindexing to a new index with the same frequency, and looking for
    # bad data that way, then, with the new, elegedly clean index,
    # convert to a new frequency.
       
    startdate = min(df.index)
    enddate = max(df.index)
    
    exp_indx = pd.date_range(start=startdate,end=enddate,freq=freq)
    old_indx = df.index
       
    def CheckNewIndex(new,old,cp,handle='raise'):
        
        new_tmp = [pd.to_datetime(d).date() for d in new]
        old_tmp = [pd.to_datetime(d).date() for d in old]
        
        new_s = set(new_tmp)        
        old_s = set(old_tmp)
        
        DataLoss = False
        
        msg = ""
        
        if not old_s.issubset(new_s):
            bad = old_s.difference(new_s)
            bad = list(bad)
            bad.sort()
            
            if len(bad) > 10:
                bad = ",\n".join([str(d) for d in bad[:5] + ["..." + str((len(bad) - 10)) + " others ..."] + bad[-5:]])
            else:
                bad = ",\n".join([str(d) for d in bad])                
            msg = "CheckNewIndex Check Point : {0}".format(cp)
            msg += "\nFrequency Change to {0} causes dataloss".format(freq)
            msg += "\nData associated with {0} is the problem.".format(bad)
            if handle == 'raise':
                raise errex.DataLoss(msg)
            elif handle == 'warn':
                DataLoss = True
            elif handle == 'ignore':
                DataLoss = False
        
        return msg, DataLoss
        
    problem1a, problem1b, problem2a, problem2b = [False] * 4
    
    OldHadAFreq = hasattr(df.index,'freq')
    
    if OldHadAFreq:
        if df.index.freq.freqstr == freq:
            # Code should never get here, this is only to detect
            # problems with bugs in Pandas.
            msg, problem1a = CheckNewIndex(exp_indx,old_indx,"SET TEST",handle='raise')
        else:
            # This is just an attempt to check, that the check below, is comprhensive.
            msg, problem1b = CheckNewIndex(exp_indx,old_indx,"SET TEST",handle='warn')
            
        newfreq_df = df.asfreq(freq=freq,method=method)
    
        if hasattr(df.index,'freq'):
            if df.index.freq.freqstr == freq:
                msg, problem2a = CheckNewIndex(newfreq_df.index,df.index,"<h3>POST CHANGE EQUAL FREQ</h3>",handle='warn')
            else:
                msg, problem2b = CheckNewIndex(newfreq_df.index,df.index,"<h3>POST CHANGE NON EQUAL FREQ</h3>",handle='warn')        
            
            
            if problem1b != problem2b or problem1b != problem2b:
                raise errex.UnexpectedCase("An unexpected scenario occured, and needs attention. {0},{1},{2},{3}".format((problem1a,problem1b,problem2a,problem2b)))
        
        #While if might feel non-intuitive to return problem2a, we do, because the error it raises downstream (DataLoss), should be silenced explicitly.
        return newfreq_df, msg, problem2a or problem2b
    else:
        newfreq_df = df.asfreq(freq=freq,method=method)
        msg, problem = CheckNewIndex(newfreq_df.index,df.index,"<h3>NON FREQ TO FREQ</h3>",handle='warn')
        return newfreq_df, msg, problem
        
    
    
        
def FillOverRide(df,symbol):
    
    # TODO : Clean up FillOverRide.  It is now overkill, because we pulled 
    # the frequency conversion upstream.
    
    db = SmartDB('Trump')
    q = "SELECT datetime,MasterOR,FailSafeOR FROM (SELECT * FROM _humanoverride AS ORide WHERE ORide.symbol='"+symbol+"' AND ORDate = (SELECT MAX(ORDate) FROM _humanoverride WHERE _humanoverride.symbol = ORide.symbol AND _humanoverride.datetime = ORide.datetime)) AS foo;"
    #q = "SELECT datetime,MasterOR,FailSafeOR FROM _humanoverride WHERE symbol = '" + symbol+ "';"
    cr = db.con.cursor()
    cr.execute(q)
    results = cr.fetchall()
    
    q = "SELECT frequency FROM _symbolinfo WHERE symbol = '" + symbol + "';"
    cr.execute(q)
    freq = cr.fetchall()
    cr.close()
    db.disconnect()
    
    OverRidesExist = len(results) > 0
    
    if OverRidesExist:
        data = [(row[1],row[2]) for row in results]
        indx = [row[0] for row in results]
        data = pd.DataFrame(data=data,index=indx)

        startdate = min(list(df.index) + indx)
        enddate = max(list(df.index) + indx)
    else:
        startdate = min(df.index)
        enddate = max(df.index)
    
    desired_indx = pd.date_range(start=startdate,end=enddate,freq=freq[0][0])
    
    cur_indx_s = set(df.index)
    new_indx_s = set(desired_indx)
    
    if not cur_indx_s.issubset(new_indx_s):
        msg = "Reindexing will cause dataloss for : {0}\n".format(symbol)
        bad_dates = new_indx_s.difference(cur_indx_s)
        bad_dates = ",\n".join([str(d) for d in bad_dates])
        msg += "Data associated with {0} would be dropped. The data is supposed to be frequency {1}".format(bad_dates,freq[0][0])
        raise Exception(msg)

    df = df.reindex(desired_indx)

    if OverRidesExist:
        data.reindex(desired_indx)
        df['vp_000'] = data[0]
        df['vp_100'] = data[1]
    else:        
        df['vp_000'] = pd.np.NaN #Fills the entire column
        df['vp_100'] = pd.np.NaN #Fills the entire column
    
    def OverRide(dff):
        cols = ['vp_100','final','vp_000']
        def p(x):
            ret = pd.np.NaN
            for c in cols:
                if pd.notnull(x[c]):
                    ret = x[c]
            return ret
        dff['final'] = dff.apply(p,axis=1,reduce=True)
        return dff
    
    df = OverRide(df)
    return df

class SymbolCacher(object):
    def __init__(self,DoInit=True):
        self.DestDB = SmartDB('Trump')
        
        #self._IntructionColumns = self.DestDB.get_attnames('_instructions')
        #self._SymbolInfo = self.DestDB.get_attnames('_symbolinfo')
        if DoInit:
            self.InitializeAll()
            
        self.pickle_storage = sysenv.getVariablePath("TRUMP_DIR") + "Problems\\"
        
    def InitializeAll(self):
        q = "SELECT * FROM _instructions ORDER BY symbol,priority;"
        InstructionRows = self.DestDB.getRowsDicts(q)
        q = "SELECT * FROM _symbolinfo ORDER BY symbol;"
        SymbolRows = self.DestDB.getRowsDicts(q)
        
        # Dictionary where the key is a symbol, the value is a Symbol's Row
        self.Symbols = {}
        
        # Dictionary of dictionaries, where the two keys are security
        # and priority.
        self.Instructions = {}
        
        # Category is a dictionary of lists
        self.Categories = {}
       
        for Symbol in SymbolRows:
            self.Symbols[Symbol['symbol']] = Symbol
            self.Instructions[Symbol['symbol']] = {}
            self.Categories[Symbol['symbol']] = []

        for Ins in InstructionRows:
            self.Instructions[Ins['symbol']][Ins['priority']] = Ins
            
        for Symbol in SymbolRows:    
            self.Categories[Symbol['symbol']].append(Symbol['category'].lower())
        
    def ReInitializeSpecific(self,s):
        s = s.lower()
        
        q = "SELECT * FROM _instructions WHERE symbol = '" + s + "' ORDER BY priority ;"
        InstructionRows = self.DestDB.getRowsDicts(q)
   
        q = "SELECT * FROM _symbolinfo WHERE symbol = '" + s + "';"
        SymbolRow  = self.DestDB.getRowsDicts(q)
        
        if not hasattr(self,'Instructions'):
            self.Instructions = {}
        if not hasattr(self,'Categories'):
            self.Categories = {}
        if not hasattr(self,'Symbols'):
            self.Symbols = {}
        
        # Dictionary where the key is a symbol, the value is a Symbol's Row
        self.Symbols[s] = SymbolRow[0]
        
        # Dictionary of dictionaries, where the two keys are security
        # and priority.
        self.Instructions[s] = {}
        for Ins in InstructionRows:
            self.Instructions[s][Ins['priority']] = Ins

        # Category is a dictionary of lists
        self.Categories[s] = []
        self.Categories[s] = [SymbolRow[0]['category'].lower()]

    def Process(self,s=None,ReInit=False):
        """
        Set ReInit to refresh the instruction list stored in this
        object prior to processing.  You need to do this,
        if the relevant sections of the instruction list has changed
        since the this SymbolCacher object was instantiated.
        """
        
        CautionMsg = "<h2>What do I do?</h2><br>This error might be ignorable,\
                      or require immediate attention.  That will depend on the\
                      business logic using this feed or security." 

        if s is None:
            if ReInit:
                self.InitializeAll()
            SymbolsToProcess = [sym for sym in self.Symbols.iterkeys()]
        else:
            if ReInit:
                self.ReInitializeSpecific(s)
            SymbolsToProcess = [s.lower()]
        
        for Symbol in SymbolsToProcess:
            
            SymHdr = Symbol
            
            print "Doing Symbol : " + Symbol + " ReInit = " + str(ReInit)
                       
            symvalid = self.Symbols[Symbol]['validity']
            symfreq = self.Symbols[Symbol]['frequency']
            symigex = set(symvalid['ignored_exceptions'])  
            
            SymbolsData = {}
            
            if len(self.Instructions[Symbol]) > 0:
                
                InsHdrs = []
                
                for priority in self.Instructions[Symbol]:
                    
                    Ins = self.Instructions[Symbol][priority]
                    
                    insvalid = Ins['validity']
                    insigex = set(insvalid['ignored_exceptions'])
                    
                    print "\n"                    
                    print "Processing Priority : " + str(Ins['priority'])

                    InsHdrs.append(str(Ins))
                    
                    #With this tryblock, it's now possible for AllData to 
                    #be "empty".  We want to allow that, to pick up the 
                    #Override cases however for now, we'll just 
                    #let it flow and get caught in the AllData tryblock
                    #if it can't handle that case.  It'd be too time consuming
                    #to set it up.
                    try:
                        try:
                            ConvertedFeed = None
                            SymbolsData[vp(Ins['priority'])] = None
                            Feed = EquitableData()
                            Feed.FetchData(Ins)
                            UnconvertedFeed = Feed.data
                            ConvertedFeed, FreqConvMsg, FreqConvProblem = SafeAsFreq(UnconvertedFeed,symfreq)
                            SymbolsData[vp(Ins['priority'])] = ConvertedFeed
                            
                            if FreqConvProblem:
                                raise errex.DataLoss("There was Data Loss during frequency conversion of \n{0}.\n  It may or may not be a material problem.\nThis is what SafeAsFreq() had to say: \n\n {1}.  The fact that you're getting this message, means it's likely not critical, and Trump proceeded by dropping the poorly indexed data.".format((vp(Ins['priority'])),FreqConvMsg))
                                
                        except Exception as e:
                            exc_type,_,_ = sys.exc_info()
                            if exc_type in errex.members:
                                if errex.members[exc_type] in insigex:
                                    print "Ignoring Exception : " + e.english
                                    print e.message
                                else:# TODO Include Priority in this error message.
                                    print "Step 1. I can't ignore this problem. " + Symbol
                                    raise
                            else:# TODO Include Priority in this error message.
                                print "Step 1. I don't recognize this problem. " + Symbol
                                raise
                    except:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        
                        formatted_lines = traceback.format_exc().splitlines()
    
                        sub = "ERROR: Trump could not get feed for " + SymHdr
                        FeedErrorMsg = "<h2>" + sub + "</h2><br>"
                        FeedErrorMsg += "The instructions to the bad feed are:<br>" + InsHdrs[-1] + "<br>"

                        if len(InsHdrs) > 1:
                            FeedErrorMsg += "<br>FWIW, these were the previous instructions:<br>"
                            FeedErrorMsg += "<br>".join(InsHdrs[:-1])
                        
                        try:
                            pickle_fileprefix = h.sha1(InsHdrs[-1]).hexdigest()[:6] + " " + dt.datetime.now().strftime("%Y-%m-%d %H-%M%S")
                            
                            fd = open(self.pickle_storage + pickle_fileprefix + "-data.p",'ab+')
                            p.dump(Feed.data,fd)
                            fd.close()
                            
                            fi = open(self.pickle_storage + pickle_fileprefix + "-index.p",'ab+')
                            p.dump(Feed.desired_indx,fi)
                            fi.close()
                            
                            PickledMsg = "<h2>Pickle?</h2>Pickled copies of the in process data and desired_index can be accessed with the following python:<br>"
                            PickledMsg += """<br>import pickle as p<br>data = p.load(open('""" + self.pickle_storage.replace("\\","\\\\") + pickle_fileprefix.replace("\\","\\\\") + """-data.p','rb'))<br><br>desire_index = p.load(open('""" + self.pickle_storage.replace("\\","\\\\") + pickle_fileprefix.replace("\\","\\\\") + """-index.p','rb'))"""
                    
                            FeedErrorMsg += PickledMsg                            
                        except:
                            FeedErrorMsg += "<h2>Pickle?</h2><br>I failed to pickle properly, but I didn't let that stop me."

                        FeedErrorMsg += CautionMsg
                                                    
                        FeedErrorMsg += "<h2>Trace:</h2>"
                        FeedErrorMsg += "<br>".join(formatted_lines)
                        
                        mail.core('trump',sub,aMsg=FeedErrorMsg,aHtml=True)   
                
                try:
                    try:
                        AllData = None
                        
                        feedvp = SymbolsData.keys()
                        for avp in feedvp:
                            if SymbolsData[avp] is None:
                                del SymbolsData[avp]
                        
                        if len(SymbolsData) == 0:
                            raise errex.NoData
                        
                        AllDataOne = None
                        AllDataTwo = None
                        AllDataThree = None
                        
                        AllDataOne = pd.DataFrame(SymbolsData)
                        AllDataTwo = TrumpMethods['Priority'](AllDataOne)
                        AllDataThree = FillOverRide(AllDataTwo,Symbol)
                        AllData = AllDataThree
                    except Exception as e:
                        exc_type,_,_ = sys.exc_info()
                        if exc_type in errex.members:
                            if errex.members[exc_type] in symigex:
                                print "Ignoring Exception : " + e.english
                                print e.message
                            else:
                                print "Step 2. I can't ignore this problem. " + Symbol
                                raise
                        else:
                            print "Step 2. I don't recognize this problem. " + Symbol
                            raise
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    
                    formatted_lines = traceback.format_exc().splitlines()

                    sub = "ERROR: Trump could not complete the aggregation of " + SymHdr
                    FeedErrorMsg = "<h2>" + sub + "</h2><br>"
                    FeedErrorMsg += "The instructions to all of the feeds are:<br>"

                    if len(InsHdrs) > 1:
                          FeedErrorMsg += "<br><br>".join(InsHdrs)
                    
                    FeedErrorMsg += "<br>"

                    try:                    
                        pickle_fileprefix = h.sha1("agg-this-string-is-just-a-seed".join(InsHdrs)).hexdigest()[:6] + " " + dt.datetime.now().strftime("%Y-%m-%d %H-%M%S")
                    
                        fd = open(self.pickle_storage + pickle_fileprefix + "-SymbolsData.p",'ab+')
                        p.dump(SymbolsData,fd)
                        fd.close()
                        
                        fi = open(self.pickle_storage + pickle_fileprefix + "-AllDataOne.p",'ab+')
                        p.dump(AllDataOne,fi)
                        fi.close()

                        fa = open(self.pickle_storage + pickle_fileprefix + "-AllDataTwo.p",'ab+')
                        p.dump(AllDataTwo,fa)
                        fa.close()

                        fb = open(self.pickle_storage + pickle_fileprefix + "-AllDataThree.p",'ab+')
                        p.dump(AllDataThree,fb)
                        fb.close()                        

                        PickledMsg = "<h2>Pickle?</h2><br>Pickled copies of the SymbolsData and AllData can be accessed with the following python:"
                        PickledMsg += '<br><br>'.join(["""<br>import pickle as p""",
                                                       """SymbolsData = p.load(open('""" + self.pickle_storage.replace("\\","\\\\") + pickle_fileprefix.replace("\\","\\\\") + """-SymbolsData.p','rb'))""",
                                                       """AllDataOne = p.load(open('""" + self.pickle_storage.replace("\\","\\\\") + pickle_fileprefix.replace("\\","\\\\") + """-AllDataOne.p','rb'))""",
                                                       """AllDataTwo = p.load(open('""" + self.pickle_storage.replace("\\","\\\\") + pickle_fileprefix.replace("\\","\\\\") + """-AllDataTwo.p','rb'))""",
                                                       """AllDataThree = p.load(open('""" + self.pickle_storage.replace("\\","\\\\") + pickle_fileprefix.replace("\\","\\\\") + """-AllDataThree.p','rb'))"""])
                    
                        FeedErrorMsg += PickledMsg
                    except:
                        FeedErrorMsg += "<h2>Pickle?</h2>I failed to pickle properly, but I didn't let that stop me."
                                      
                    FeedErrorMsg += CautionMsg
                    
                    FeedErrorMsg += "<h2>Trace:</h2>"
                    FeedErrorMsg += "<br>".join(formatted_lines)
                    
                    mail.core('trump',sub,aMsg=FeedErrorMsg,aHtml=True)   
                
                try:
                    try:
                        FinishedQryBuilding = False
                        
                        if AllData is None:
                            raise errex.NoData("All Data is None.")
                        
                        InsertValues = []
                        for c,row in AllData.iterrows():
                            rowtoinsert = ["'" + Symbol + "'"] + ["'" + str(c) + "'" ] + [str(x) for x in row.values]
                            InsertValues.append(",".join(['Null' if x in ['nan','None'] else x for x in rowtoinsert]))
                        
                        InsertValues = "(" + "),(".join(InsertValues) + ")"
    
                        TablesToDo = ["_" + cat for cat in self.Categories[Symbol]] + [Symbol]
                        
                        InsertQrys = ["INSERT INTO " + t + " (symbol,datetime," + ",".join(AllData.columns) + ") VALUES " + InsertValues + ";" for t in TablesToDo]
                        DelQrys = ["DELETE FROM " + t + " WHERE symbol = '" + Symbol + "';" for t in TablesToDo]
                        
                        FinishedQryBuilding = True
                    except Exception as e: 
                        exc_type,_,_ = sys.exc_info()
                        if exc_type in errex.members:
                            if errex.members[exc_type] in symigex:
                                print "Ignoring Exception : " + e.english
                                print e.message
                            else:
                                print "Step 3. I can't ignore this problem. " + Symbol
                                raise
                        else:
                            print "Step 3. I don't recognize this problem. " + Symbol
                            raise
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    
                    formatted_lines = traceback.format_exc().splitlines()

                    sub = "ERROR: Trump could not create queries for " + SymHdr
                    FeedErrorMsg = "<h2>" + sub + "</h2><br>"
                    FeedErrorMsg += "The instructions to all of the feeds are:<br>"

                    if len(InsHdrs) > 1:
                          FeedErrorMsg += "<br><br>".join(InsHdrs)
                    
                    FeedErrorMsg += "<br>"
                    
                    try:                    
                        pickle_fileprefix = h.sha1("qry-this-string-is-just-a-seed".join(InsHdrs)).hexdigest()[:6] + " " + dt.datetime.now().strftime("%Y-%m-%d %H-%M%S")
                        pickle_storage = sysenv.getVariablePath("TRUMP_DIR") + "Problems\\"
                                           
                        fi = open(pickle_storage + pickle_fileprefix + "-AllData.p",'ab+')
                        p.dump(AllData,fi)
                        fi.close()

                        PickledMsg = "<h2>Pickle?</h2><br>Pickled copies of the AllData can be accessed with the following python:"
                        PickledMsg += """<br>import pickle as p<br><br>AllData = p.load(open('""" + self.pickle_storage.replace("\\","\\\\") + pickle_fileprefix.replace("\\","\\\\") + """-AllData.p','rb'))"""
                    
                        FeedErrorMsg += PickledMsg
                    except:
                        FeedErrorMsg += "<h2>Pickle?</h2>I failed to pickle properly, but I didn't let that stop me."

                    FeedErrorMsg += CautionMsg
                    
                    FeedErrorMsg += "<h2>Trace:</h2>"
                    FeedErrorMsg += "<br>".join(formatted_lines)
                    
                    mail.core('trump',sub,aMsg=FeedErrorMsg,aHtml=True)   
                
               
                cr = self.DestDB.con.cursor()
                try:
                    try:
                        q = ""   
                        if not FinishedQryBuilding:
                            raise errex.NoData("Queries were not finished building properly.")
                        #Begin is implied by psycopg2
                        for q in DelQrys:
                            cr.execute(q)
                        for q in InsertQrys:
                            cr.execute(q)
                        q = "Not a query: Couldn't Commit properly" + q
                    except Exception as e: 
                        exc_type,_,_ = sys.exc_info()
                        if exc_type in errex.members:
                            if errex.members[exc_type] in symigex:
                                print "Ignoring Exception : " + e.english
                                print e.message
                            else:
                                print "Step 5. I can't ignore this problem. " + Symbol
                                raise
                        else:
                            print "Step 5. I don't recognize this problem. " + Symbol
                            raise                        
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    
                    formatted_lines = traceback.format_exc().splitlines()
                    
                    self.DestDB.con.rollback()

                    sub = "ERROR: Trump could not execute queries for " + SymHdr
                    FeedErrorMsg = "<h2>" + sub + "</h2>"

                    FeedErrorMsg += "This is the query which failed:<br>"
                    FeedErrorMsg += q
                    
                    FeedErrorMsg += "The instructions to all of the feeds are:<br>"

                    if len(InsHdrs) > 1:
                          FeedErrorMsg += "<br><br>".join(InsHdrs)
                    
                    FeedErrorMsg += "<br>"
                    

                    FeedErrorMsg += CautionMsg
                    
                    FeedErrorMsg += "<h2>Trace:</h2>"
                    FeedErrorMsg += "<br>".join(formatted_lines)
                    
                    mail.core('trump',sub,aMsg=FeedErrorMsg,aHtml=True)
                    
                else:
                    self.DestDB.con.commit()
                cr.close()
            else:
                print "Skipping."


if __name__ == '__main__':
    sc = SymbolCacher()
    sc.Process('price_46430t109_ca')
