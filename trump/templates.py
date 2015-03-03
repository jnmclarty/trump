
from equitable.db.psyw import DBpicker

default = DBpicker('LOCALHOST')

###############################################################################

class accntTemplate(object):
    def __init__(self,username,userpass=None):
        self.name = username
        if userpass:
            self.pswd = userpass
        else:
            self.pswd = username            

Users = {}
Users['QuantsRead'] = accntTemplate('QuantsRead')
Users['Quants'] = accntTemplate('Quants')

#Special, default for this project
Users['Trump'] = accntTemplate(default.read_write,default.read_write_pass)

###############################################################################
    
class dbTemplate(object):
    def __init__(self,db,host=None,user=None):
        self.name = db
        
        if host is None:
            self.host = default.host 
        else:
            self.host = host
        
        if user is None:
            self.user = Users['Trump'].name
            self.pswd = Users['Trump'].pswd
        else:
            self.user = user.name
            self.pswd = user.pswd

Databases = {}
Databases['General'] = dbTemplate('General')
Databases['Bond'] = dbTemplate('Bond')
Databases['Equity'] = dbTemplate('Equity')
Databases['Pref'] = dbTemplate('Pref')
Databases['Portfolio'] = dbTemplate('Portfolio')

#Sepecial, default for this project
Databases['Trump'] = dbTemplate('Trump')

#Blank
Databases['NA'] = dbTemplate('NA')

###############################################################################
class Check(object):
    def __init__(self,name,output=None,params=None,checkpoints=None):
        self.name = name or 'DataExists'
        self.output = output or {'ALL' : 'PLER'}
        self.params = params or {'kwargs' : {None : None}, 'args' : [None]}
        self.checkpoints = checkpoints or ['Cache','Comprehensive'] #['Cache','MyRandomJob','DayOfMonth','DayOfWeek','Quick','Comprehensive','Debug']  #All Checks will 
    def asdict(self):
        return {'name' : self.name,
                'output' : self.output,
                'params' : self.params,
                'checkpoints' : self.checkpoints}
                
class Validity(object):
    def __init__(self,ignored_exceptions=None,checks=None):
        self.ignored_exceptions = ignored_exceptions or []
        self.checks = checks or []
    def AddCheck(self,name,output=None,params=None,checkpoints=None):
        self.checks.append(Check(name,output,params,checkpoints))
    def AddIgnoredException(self,name):
        self.ignored_exceptions.append(name)
    def Apply(self,FeedList):
        tmp = []
        for Feed in FeedList:
            Feed.validity = self
            tmp.append(Feed)
        return tmp
    def __call__(self):
        return {'ignored_exceptions' : self.ignored_exceptions,
                'checks' : [c.asdict() for c in self.checks]}

class insTemplate(object):
    """
    The defaults to any subclass template created with this class template
    are tricky.  This object, sets up a standard & recommended convention,
    that was designed at the inception of Trump.
    
    The four ways parameters get set, in a normal python function are as
    follows:
    
    1. Explicit + Mandatory (arg1, arg2, etc...)
    2. Explicit + Optional (kwarg1='default', kwarg2='default', etc...)
    3. Implicit + Defined (self.var = 'default')
    
    This template, adds a fourth, sneaky, magic, way:
    
    4. Implicit + Undefined (Using self.SetUnset())
    
    Confusion can set in, because #2 and #3, trump #4.  #1 is unlikely to confuse.
    
    We are all familiar with #1 and #2.  #3 is just, a variable that gets
    set at runtime, using logic, even if it's set up to always be a constant.
    This is useful, to simplify templates.  #4 is where the magic happens.
    If you try to set up a template, without taking care of all the defaults,
    this class will make sure they get declared for you, after __init__()
    What they get set to is in the SetUnset function.  This function, should 
    not be overloaded.  If you don't like the default in there, make sure to
    take care of them using #1, #2 or #3.
    """
    
    def __init__(self,instype,db,tablename,datefieldname,valuefieldname,keyfieldname=None,keyfieldvalue=None,freqparse='D-reset',sqlcrit=None,pycrit=None,validity=None):
        self.instype = instype
        self.db = db
        self.tablename = tablename
        self.datefieldname = datefieldname
        self.valuefieldname = valuefieldname
        self.keyfieldname = keyfieldname
        self.keyfieldvalue = keyfieldvalue
        self.freqparse = freqparse
        self.sqlcrit = sqlcrit
        
        if validity is None:
            self.validity = Validity()
        
        if type(pycrit) is list:
            self.pycrit = pycrit
        else:
            self.pycrit = [pycrit]            
        self.SetUnset()       
    def SetUnset(self):
        if not hasattr(self,'instype'):
            self.instype = 'DB'
        if not hasattr(self,'db'):
            self.db = Databases['NA']      
        if not hasattr(self,'elid'):
            self.elid = None
        if not hasattr(self,'tablename'):
            self.tablename = None
        if not hasattr(self,'datefieldname'):
            self.datefieldname = None
        if not hasattr(self,'valuefieldname'):
            self.valuefieldname = None
        if not hasattr(self,'keyfieldname'):
            self.keyfieldname = None
        if not hasattr(self,'keyfieldvalue'):
            self.keyfieldvalue = None
        if not hasattr(self,'freqparse'):
            self.freqparse = 'D-reset'
        if not hasattr(self,'sqlcrit'):
            self.sqlcrit = None
        if not hasattr(self,'pycrit'):
            self.pycrit = [None] * 4
        if not hasattr(self,'validity'):
            self.validity = Validity()
            
        self.pycrit = self.pycrit + [None] * (4 - len(self.pycrit))
        
    def GetParameters(self):
        if self.freqparse not in ['B-reset','D-reset','noreset']:
            raise Exception("Frequency Parse Option is Unknown")
        return [self.instype,self.elid,self.db.name,self.tablename,self.valuefieldname,self.datefieldname,self.keyfieldname,self.keyfieldvalue,self.freqparse,self.sqlcrit] + self.pycrit

class BOC_FX(insTemplate):
    def __init__(self,keyfieldvalue=None,sqlcrit=None):
        self.db = Databases['General']
        self.tablename = 'bankofcanada_fxrates'
        self.datefieldname = 'date'
        self.valuefieldname = 'value'
        self.keyfieldname = 'name'
        self.keyfieldvalue = keyfieldvalue
        self.sqlcrit = sqlcrit
        self.freqparse = 'B-reset'
        self.SetUnset()
        
class Econ(insTemplate):
    def __init__(self,keyfieldvalue=None,freqparse='D-reset'):
        self.initecon(keyfieldvalue,freqparse)
    def initecon(self,keyfieldvalue,freqparse):
        self.db = Databases['General']
        self.tablename = 'econ'
        self.datefieldname = 'date'
        self.valuefieldname = 'value'
        self.keyfieldname = 'name'
        self.keyfieldvalue = keyfieldvalue
        self.freqparse = freqparse
        self.SetUnset()
        
class EconB(Econ):
    def __init__(self,keyfieldvalue=None):
        self.initecon(keyfieldvalue,'B-reset')

class CCDJ(insTemplate):
    def __init__(self,keyfieldvalue,sql="date <= '2007-06-12'",valuefieldname='tri'):
        self.db = Databases['General']
        self.tablename = 'ccdj_prefindices_levels'
        self.datefieldname = 'date'
        self.valuefieldname = valuefieldname
        self.keyfieldname = 'indexname'
        self.keyfieldvalue = keyfieldvalue
        self.freqparse = 'B-reset'
        self.sqlcrit = sql
        self.SetUnset()
        
class TMXBond(insTemplate):
    '''
    Designed for bondindices_dex and bondindices_dex_monthly
    '''
    def __init__(self,tablename,keyfieldvalue=None,valuefieldname='tri'):
        self.db = Databases['General']
        self.tablename = tablename 
        self.datefieldname = 'date'
        self.valuefieldname = valuefieldname
        self.keyfieldname = 'name'
        self.keyfieldvalue = keyfieldvalue
        self.freqparse = 'B-reset'
        self.SetUnset()
        
class PamFxRates(insTemplate):
    '''
    Pulls fx rates from pam_fxrates table. Required argument is fxshortname string
    '''    
    def __init__(self,keyfieldvalue):
        self.db = Databases['Portfolio']
        self.tablename = 'pam_fxrates' 
        self.datefieldname = 'fxdate'
        self.valuefieldname = 'fxrate'
        self.keyfieldname = 'fxshortname'
        self.keyfieldvalue = keyfieldvalue
        self.freqparse = 'B-reset'
        self.SetUnset()
        
class BBFetch(insTemplate):
    def __init__(self,elid,valuefieldname,datefieldname=None,freqparse='D-reset'):
        self.instype = 'BBFetch'
        self.elid = elid
        self.valuefieldname = valuefieldname
        self.datefieldname = datefieldname
        self.freqparse = freqparse
        
        self.validity = Validity()
        self.validity.AddCheck('BBFetch')
        self.validity.AddIgnoredException('NoData')
        self.SetUnset()
        
class BBFetchBulk(insTemplate):
    def __init__(self,elid,valuefieldname,datefieldname=None):
        raise NotImplemented
        self.db = Databases['NA']
        self.instype = 'BBFetchBulk'
        
        self.elid = elid
        self.valuefieldname = valuefieldname
        self.datefieldname = datefieldname
        self.SetUnset()
        
class BL_Dividends(insTemplate):
    def __init__(self,secid,datefieldname,valuefieldname='amount'):
        self.db = Databases['Equity']
        self.tablename = 'equity_dividends'

        self.valuefieldname = valuefieldname
        self.datefieldname = datefieldname

        self.keyfieldname = 'secid'
        self.keyfieldvalue = secid

        self.pycrit = ["""DuplicateIndexHandler='sum'"""]
        self.SetUnset()        

class x_ts(insTemplate):
    def __init__(self,adb,table,secid,datefieldname='date',valuefieldname='px_last'):
        self.db = Databases[adb]
        self.tablename = table

        self.valuefieldname = valuefieldname
        self.datefieldname = datefieldname

        self.keyfieldname = 'name'
        self.keyfieldvalue = secid
        
        self.SetUnset()
        
class equity_ts(x_ts):
    def __init__(self,secid,datefieldname='date',valuefieldname='px_last'):
        super(equity_ts,self).__init__('Equity','equity_ts',secid,datefieldname,valuefieldname)        

class pref_ts(x_ts):
    def __init__(self,secid,datefieldname='date',valuefieldname='px_last'):
        super(pref_ts,self).__init__('Pref','pref_ts',secid,datefieldname,valuefieldname)    

class bond_ts(x_ts):
    def __init__(self,secid,datefieldname='date',valuefieldname='price_bid'):
        super(bond_ts,self).__init__('Bond','bond_ts',secid,datefieldname,valuefieldname)
        
class BBFetchBulkDivs(insTemplate):
    def __init__(self,elid,datefieldname,keyfieldvalue='amount'):
        self.instype = 'BBFetchBulk'
        self.elid = elid
        self.valuefieldname = 'DVD_HIST_ALL_WITH_AMT_STATUS'
        self.keyfieldname = 'dividend_type'
        self.keyfieldvalue = keyfieldvalue
        self.datefieldname = datefieldname
        
        KeepList = ["Capital Gains","Misc","Return of Capital","2nd Interim",
                    "Accumulation","Long Term Cap Gain","1st Interim",
                    "4th Interim","Special Cash","3rd Interim",
                    "Short Term Cap Gain","Income"]

        self.pycrit = ["""indexfilter={'dividend_type' : """ + str(KeepList) + """}""",
                       """DuplicateIndexHandler='sum'"""]

        self.validity = Validity()
        self.validity.AddCheck('BBFetch')
        self.SetUnset()        