import argparse 

parser = argparse.ArgumentParser(description='Add Securities')
#TODO Force should implement delete, as right now, if a feed instruction 
# disappears it won't be removed, and will linger as the nth+1 instruction
parser.add_argument('-f','--Forced', dest='force', action='store_true')
parser.add_argument('-g','--Groups', dest='groups', nargs='*')
parser.add_argument('-j','--Job', dest='jobname', default='UNSPECIFIED')

parser.set_defaults(force=False,groups='ALL')
args = parser.parse_args()



try:
    from equitable.infrastructure import sendemail
            
    from equitable.curholdings.conversions import SecIDtoBloombergFetchInput
    
    import datetime as dt
    
    print args.force
    
    if args.groups != 'ALL':
        print "Doing groups : " + ", ".join(args.groups)
    else:
        print "Doing all groups except examples"
            
    #pgw is used for reading source information which is converted to instructions
    from equitable.db import pgw
    
    #psyw is the new standard.
    from equitable.db.psyw import SmartDB
    
    from equitable.curholdings.interface import SecPos
    
    from equitable.trumpapi import process as prc
    from equitable.trumpapi import funcs as tfn
    import equitable.chronos.conversions as ec
    
    from trump.templates import Validity, Databases,insTemplate, \
                                Econ, EconB, BOC_FX, TMXBond, PamFxRates, CCDJ,\
                                BL_Dividends, BBFetch, BBFetchBulkDivs, equity_ts, \
                                pref_ts, bond_ts
    
    from equitable.errors.existence import NoELID
    
    from setup import CreateTable
    
    def AddGroupCheck(g):
        if args.groups == 'ALL':
            return g != 'examples'
        else:
            return g in args.groups
    
    ###############################################################################
    
    CouldntAdd = {}
    
    def CreateSymbolTableIfNeeded(name='_AllSymbols',NumOfDataCols=1):
        tdb = SmartDB('Trump')
        assert NumOfDataCols >= 1
        cr = tdb.con.cursor()
        q = "SELECT count(table_name) FROM information_schema.tables WHERE table_name = '" + name.lower() + "';"
        cr.execute(q)
        NumOfTablesWhichExist = cr.fetchall()[0][0]
        #print NumOfTablesWhichExist
    
        columnsStart = [('Symbol','text'),
                        ('Datetime','timestamp without time zone'),
                        ('Date','time'),
                        ('Final','double precision')]
    
        #print "NumOfTablesWhichExist = " + str(NumOfTablesWhichExist)
        if NumOfTablesWhichExist < 1:
            #print "So, create the table"
            VPs = [('vp_000','double precision'),
                   ('vp_100','double precision')]
            CreateTable(name,columnsStart + VPs,['Symbol','Datetime'])
    
        q = "SELECT count(table_name) FROM information_schema.tables WHERE table_name = '" + name.lower() + "';"
        #print q
        cr.execute(q)
        NumOfTablesWhichExist = cr.fetchall()#[0][0]
        
        #print NumOfTablesWhichExist
        #print "NumOfTablesWhichExist,after adding = " + str(NumOfTablesWhichExist)
        
        #At this point, we can assume the table exists, and has at least two VPs, but possibly more.  Can't assume the order.
        RequiredNumOfVPs = NumOfDataCols + 2
    
        VPsWhichExist = "SELECT column_name FROM information_schema.columns WHERE column_name LIKE 'vp_%' AND table_name = '" + name.lower() + "' ORDER BY ordinal_position;;"
        cr.execute(VPsWhichExist)
        results = cr.fetchall()
        VPsWhichExist = [row[0] for row in results]
        
        #print name+" VPs:"+str(len(VPsWhichExist))+" Required:"+str(RequiredNumOfVPs)
        if len(VPsWhichExist) < RequiredNumOfVPs:
            NumOfvpColsWhichExist = len(VPsWhichExist)
            
            #print "Number of VP Cols Which Exist = " + str(NumOfvpColsWhichExist)
            
            VPsToAdd =["vp_" + tfn.PadTwo(i) for i in range(NumOfvpColsWhichExist-1,NumOfDataCols+1)]
            
            #print VPsToAdd
            AllVPs = VPsWhichExist + VPsToAdd
            
            
            AllVPs.sort()
    
            CreateTable(name + "_new",columnsStart + zip(AllVPs,["double precision"] * len(AllVPs)),['Symbol','Datetime'])
            
            qry = "INSERT INTO " + name + "_new SELECT " + "".join([c[0] + "," for c in columnsStart]) + ",".join(VPsWhichExist) + " FROM " + name + ";"
            cr.execute(qry)
            tdb.con.commit()
    
            cr.execute("DROP TABLE " + name + ";")
            cr.execute("ALTER TABLE " + name + "_new RENAME TO " + name + ";")
            tdb.con.commit()
        
        cr.execute("ALTER TABLE " + name + " OWNER TO quants;")
        cr.execute("GRANT ALL ON TABLE " + name + " TO quants;")
        cr.execute('GRANT ALL ON TABLE ' + name + ' TO "quants-admin";')    
        cr.execute("GRANT SELECT ON TABLE " + name + " TO quantsread;")
        cr.close()
        tdb.con.commit()
    
    
    class Info(object):
        def __init__(self,Category,TrumpMethod,DesiredUnits,Description,Frequency,validity=None):
            self.Category = Category
            self.TrumpMethod = TrumpMethod
            self.Units = DesiredUnits
            self.Description = Description
            self.Frequency = Frequency
            self.Validity = validity or Validity()
        def GetParameters(self):
            return [self.Category,self.TrumpMethod,self.Units,self.Description,self.Frequency,self.Validity()]
    
    class Sec(object):
        def __init__(self,Source,Priority=-1,UnitMult=1,Unit='',UnitParse='',validity=None):
            self.priority = Priority
            self.mult = UnitMult
            self.unit = Unit
            self.parse = UnitParse
            self.source = Source
            self.validity = validity or Source.validity
        def setPriority(self,p):
            self.priority = p
    
    class SecurityInstruction(object):
        def __init__(self,Symbol,Securities,Info):
            self.name = Symbol.lower()
            self.Securities = Securities
            self.Info = Info
            UsedPriorities = [s.priority for s in Securities if s.priority != -1]
            AvailablePriorities = [i for i in range(99,0,-1) if i not in UsedPriorities]
            for s in self.Securities:
                if s.priority == -1:
                    p = AvailablePriorities.pop()
                    s.setPriority(p)
            self.vpCntEx100 = max([s.priority for s in Securities])
            self.Cacher = prc.SymbolCacher(DoInit=False)
            
            
        def InsertInstruction(self,Overwrite=False):
            tdb = SmartDB('Trump')
            
            cr = tdb.con.cursor()
    
            DoCheck = True
            
            DidAny = False
    
            cr.execute("SELECT COUNT(symbol) FROM _instructions WHERE symbol='" + self.name + "';")
            cnt = cr.fetchone()
            #Force an Overwrite if the number of instructions has changed.  ie, 
            #in the event of an add or delete.
            Overwrite = Overwrite or (cnt[0] != len(self.Securities))
            
            if Overwrite:
                q = "DELETE FROM _instructions WHERE symbol='" + self.name + "';"
                cr.execute(q)
                tdb.con.commit()
                DoCheck = False
    
            for s in self.Securities:
                Do = True
                if DoCheck:
                    cr.execute("SELECT COUNT(symbol) FROM _instructions WHERE symbol='" + self.name + "' and priority = " + str(s.priority) + ";")
                    cnt = cr.fetchone()
                    if cnt[0] > 0:
                        Do = False
                    
                if Do:
                    pars = [self.name,s.priority,str(s.mult),s.unit,s.parse,s.validity()] + s.source.GetParameters()
                    ss = ",".join(["%s"] * len(pars))
                    q = "INSERT INTO _instructions VALUES (" + ss + ");"
                    cr.execute(q,tuple(pars))
                    tdb.con.commit()
                    DidAny = True
            tdb.con.close()
            
            return DidAny
            
        def InsertSymbolInfo(self,Overwrite=False):
            tdb = SmartDB('Trump')
            cr = tdb.con.cursor()
            
            Did = False
            
            Do = True
            if Overwrite:
                q = "DELETE FROM _symbolinfo WHERE symbol='" + self.name + "';"
                cr.execute(q)
                tdb.con.commit()
            else:
                cr.execute("SELECT COUNT(symbol) FROM _symbolinfo WHERE symbol='" + self.name + "';")
                cnt = cr.fetchone()
                if cnt[0] > 0:
                    Do = False
                    
            if Do:
                pars = [self.name] + self.Info.GetParameters()
                ss = ",".join(["%s"] * len(pars))
                q = "INSERT INTO _symbolinfo VALUES (" + ss + ");"
                cr.execute(q,tuple(pars))
                tdb.con.commit()
                Did = True
            
            tdb.con.close()
            return Did
                
        def Setup(self):
            CreateSymbolTableIfNeeded('_' + self.Info.Category,self.vpCntEx100)
            CreateSymbolTableIfNeeded(self.name,self.vpCntEx100)
            
            InsInserted = self.InsertInstruction(Overwrite=args.force)
            SymInserted = self.InsertSymbolInfo(Overwrite=args.force)
            
            if InsInserted or SymInserted:
                self.Cacher.Process(self.name,ReInit=True)
            else:
                print "Skipping caching of " + self.name
           
    
    ##Menu Of Examples/Options
    #
    ##Bloomberg In ECON
    #Sec(EconB("FX.BL.BRLCAD"),Unit='BRL per CAD')
    #Sec(EconB("FX.BL.CA"),Unit='CAD per USD')
    #Sec(EconB("FX.BL.IDRCAD"),UnitMult=1000,Unit='IDR per CAD')
    #Sec(EconB("FX.BL.INRCAD"),Unit='INR per CAD')
    #Sec(EconB("FX.BL.JP"),Unit='JPY per USD')
    #Sec(EconB("FX.BL.TRLCAD"),UnitMult=1000000,Unit='TRL per CAD')
    #Sec(EconB("FX.BL.ZARCAD"),Unit='ZAR per CAD')
    #Sec(EconB("FX.BLINVERT.CA"),Unit='USD per CAD')
    #
    ##B OF C In ECON
    #Sec(EconB("FX.BOFC.CLOSING.EU"),Unit='CAD per EUR')
    #Sec(EconB("FX.BOFC.CLOSING.US"),Unit='CAD per USD')
    #Sec(EconB("FX.BOFC.NOON.EU"),Unit='CAD per EUR')
    #Sec(EconB("FX.BOFC.NOON.US"),Unit='CAD per USD')
    
    if AddGroupCheck('examples'):
        Category = "examples"
        CouldntAdd[Category] = []
        v = Validity(ignored_exceptions=['NoData','DataLoss'])
        #Example of Doing a One-Off
        CADUSD_Securities = [Sec(EconB("FX.BL.CA"),Unit='CAD per USD',UnitParse='^-1'), 
                             Sec(EconB("FX.BOFC.CLOSING.US"),Unit='CAD per USD'),
                             Sec(EconB("FX.BOFC.NOON.US"),Unit='CAD per USD',Priority=1),
                             Sec(EconB("FX.BOFC.NOON.US.TEST"),Unit='CAD per USD',Priority=2),
                             Sec(BOC_FX('U.S. dollar',"fxtype='close'")),
                             Sec(EconB('FX.BLINVERT.CA'),Priority=5),
                             Sec(EconB('FX.CA'),Unit='CAD per USD'),
                             Sec(BBFetch(14,'PX_LAST'),Unit='CAD per USD',UnitParse='^-1')]
    
        CADUSD_Securities = v.Apply(CADUSD_Securities)
    
        #Example of Using Bloomberg Dividend Templates
        ETF46428D108_Info = Info(Category, 'Priority', 'CAD per unit', 'Canadian Dollar', 'B')
        ETF46428D108_Securities = [Sec(BL_Dividends(1,'exdate'),validity=v),
                                   Sec(BBFetchBulkDivs(1061,'exdate'),validity=v)] 
        SecurityInstruction('TEST_DONOTUSE_iShares_TSX_60_Div_Payable',ETF46428D108_Securities,ETF46428D108_Info).Setup()
    
        
        v = Validity(ignored_exceptions=['NoData'])
    
        #B OF C Rates
        BOCRates = {}
        GeneralDB = pgw.dbConnect('General')
        BOCPairs = GeneralDB.query("SELECT DISTINCT name,fxtype FROM bankofcanada_fxrates")
        for currency in BOCPairs.getresult():
            BOCRates[currency] = Sec(BOC_FX(currency[0],'fxtype=' + currency[1]),Unit = 'CAD per X',validity=v)
    
        #Example of Doing a One-Off
        TheSecurities = [Sec(EconB("FX.BL.JP"),Unit='JPY per USD',validity=v)]
        TheInfo = Info(Category,'Priority','JPY USD','Yen for US Dollar','B')
        SecurityInstruction('TEST_DONOTUSE_JPYUSD',TheSecurities,TheInfo).Setup()
        
        #Another Example
        CADEUR_Securities = [Sec(EconB("FX.BOFC.CLOSING.EU"),Unit='CAD per EUR',validity=v),
                             Sec(EconB("FX.BOFC.NOON.EU"),Unit='CAD per EUR',validity=v)]                
        CADEUR_Info = Info(Category,'Priority','CAD per EUR','Canadian Dollar for Euros','B')
        SecurityInstruction('TEST_DONOTUSE_CADEUR',CADEUR_Securities,CADEUR_Info).Setup()
       
        CADUSD_Info = Info(Category,'Priority','CAD per USD','Canadian Dollar for US Dollar','B')
        SecurityInstruction('TEST_DONOTUSE_CADUSD',CADUSD_Securities,CADUSD_Info).Setup()
        
        GeneralDB.close()
    
    if AddGroupCheck('prefprices'):
        Category = "prefprices"
        CouldntAdd[Category] = []
        # This category adds prices for equities in EquityDB, and their
        # matching bloomberg data if available.
    
        from adders.bloombergfetch import ExCodetoCountryMap, ExCodetoCurMap, CountrytoExCodeGuessMap, CountrytoCurGuessMap
        import equitable.bpsdl as bf
        
        #2011-04-15 is basically, "Since inception" where currency is available
        SecIDs = SecPos('Pref').get_important_holdings(argDate=dt.date(2011,4,16))
            
        PrefDB = SmartDB('Pref')
    
        BB = bf.Getter.BBapi(clean=False)  
        
        for SecID in SecIDs:
             
            v = Validity()
            v.AddIgnoredException("DataLoss")
        
            qry = """SELECT secid.secid AS secid, 
                       split_part(secid.name,'.',2) AS name, 
                       COALESCE(bl_info.bloomberg_cusip, bloomberg_sedol) AS id,
                       bl_info.id_exch_symbol AS ticker,
                       split_part(secid.name,'.',1) AS country,
                       bl_info.bloomberg_cusip AS cusip,
                       bl_info.bloomberg_isin AS isin,
                       bl_info.bloomberg_sedol AS sedol,                          
                       CASE WHEN bl_info.bloomberg_cusip IS NOT NULL THEN 'CUSIP' ELSE CASE WHEN bl_info.bloomberg_sedol IS NOT NULL THEN 'SEDOL' ELSE Null END END AS tickertype,
                       bl_info.long_desc AS long_desc, 
                       bl_info.called AS called,
                       bl_info.maturity_type AS maturity_type
                     FROM 
                       public.bl_info, 
                       public.secid
                     WHERE 
                       secid.secid = bl_info.secid AND
                       secid.name = '{}'
                     ORDER BY secid;"""
                     
            prefs = PrefDB.getRowsDicts(qry.format(SecID))
            
            if len(prefs) > 1:
                raise Exception("Multiple SecID found, where expected one {}".format(SecID))
            elif len(prefs) == 1:
                pref = prefs[0]
                SecIDinfo = True
            else:
                SecIDinfo = False
    
            if pref['called'].upper() == 'Y':
                #TODO improve this logic, add/remove from bloomberg?
                # Set the ANID to 'bad' somehow, in bloombergfetch. (Fetch Never? Remove from Group?)
                v.AddIgnoredException("NoData")
                
            secs = []
            desc = []
            
            if SecIDinfo:
                secs.append(Sec(pref_ts(SecID),validity=v))
                desc = [pref['ticker'],"CALLED = {}".format(pref['called']),"mtype = {}".format(pref['maturity_type']),pref['long_desc']]
                ccforcur = SecID[:2]
                if pref['tickertype'] in ('CUSIP','ISIN'):
                    ExpBBid,ExpBBcc,ExpBBdef = SecIDtoBloombergFetchInput(SecID,"Pref")
                    ccforcur = ExpBBcc
                elif pref['tickertype'] in ('SEDOL'):
                    ExpBBid,ExpBBcc,ExpBBdef = pref['id'],pref['country'],pref['name']
                    ccforcur = ExpBBcc
                else:
                    raise Exception("Unknown tickertype in Equity DB")
            else:
                #This shouldn't be a case, but it's either do this, or handle
                # a false SeCIDinfo and be blind to the bad securities lack of existence.
                desc = ['NoSecIDinfo']
                ExpBBid,ExpBBcc,ExpBBdef = SecIDtoBloombergFetchInput(SecID,"Equity")
                ccforcur = ExpBBcc
                
            try:
                BFSecurity = BB.GetSecurity([ExpBBid,ExpBBcc,ExpBBdef])
                BBSec = True
            except NoELID:
                desc = ["NoELID"] + desc
                BBSec = False
    
            if BBSec:
                ActBBid,ActBBcc,ActBBdef,elid = BFSecurity.anid,BFSecurity.anidcc,BFSecurity.aniddef,BFSecurity.ELID
                secs.append(Sec(BBFetch(elid,"PX_LAST"),validity=v))
                About = BFSecurity.About()
                MetaExCode = BFSecurity.GetDataMostRecentFetchValue('EXCH_CODE')
                desc = ["ELID" + str(elid),ActBBid,ActBBcc,ActBBdef,MetaExCode,About['Name'],About['Security Type'],About['Security Info'],"price"] + desc
                ccforcur = ActBBcc
    
            if len(secs) >= 1:
                TheInfo = Info(Category, 'Priority', ExCodetoCurMap[ccforcur], "_".join([str(d) for d in desc]) , 'B', validity=v)
                newsymbol = "price_{0}{1}".format(*SecID.split("."))
                SecurityInstruction(newsymbol,secs,TheInfo).Setup()  
            else:
                CouldntAdd[Category].append(SecID)    
    
    if AddGroupCheck('bondprices'):
    
        Category = "bondprices"
        CouldntAdd[Category] = []
        
        from adders.bloombergfetch import ExCodetoCountryMap, ExCodetoCurMap, CountrytoExCodeGuessMap, CountrytoCurGuessMap
        import equitable.bpsdl as bf
        
        #2011-04-15 is basically, "Since inception" where currency is available
        SecIDs = SecPos('Bond').get_important_holdings(argDate=dt.date(2011,4,16))
            
        BondDB = SmartDB('Bond')
    
        BB = bf.Getter.BBapi(clean=False)  
        
        for SecID in SecIDs:
             
            v = Validity()
            v.AddIgnoredException("NoData")
            v.AddIgnoredException("DataLoss")
        
            qry = """SELECT secid.secid AS secid, 
                       secid.name AS secidname,
                       split_part(secid.name,'.',1) AS country,
                       split_part(secid.name,'.',2) AS name, 
                       bl_info.bloomberg_cusip AS cusip,
                       bl_info.bloomberg_isin AS isin,
                       bl_info.bloomberg_sedol AS sedol,                       
                       CASE WHEN bl_info.bloomberg_cusip IS NOT NULL THEN 'CUSIP' ELSE CASE WHEN bl_info.bloomberg_sedol IS NOT NULL THEN 'SEDOL' ELSE Null END END AS tickertype,
                       CASE WHEN bl_info.id_exch_symbol IS NOT NULL THEN bl_info.id_exch_symbol ELSE 'NoTicker' END AS ticker,
                       bl_info.long_desc AS long_desc, 
                       bl_info.short_desc AS short_desc,
                       bl_info.called AS called
                     FROM 
                       public.bl_info, 
                       public.secid
                     WHERE 
                       secid.secid = bl_info.secid AND
                       secid.name = '{}'                   
                     ORDER BY country, isin, secidname;"""
                     
            bonds = BondDB.getRowsDicts(qry.format(SecID))
            
            if len(bonds) > 1:
                raise Exception("Multiple SecID found, where expected one {}".format(SecID))
            elif len(bonds) == 1:
                bond = bonds[0]
                SecIDinfo = True
            else:
                SecIDinfo = False
                
            secs = []
            desc = []
            
            if SecIDinfo:
                secs.append(Sec(bond_ts(SecID,valuefieldname='price_bid'),validity=v))
                desc = [bond['ticker'],bond['long_desc']]
                ccforcur = SecID[:2]
                if bond['tickertype'] in ('CUSIP','ISIN'):
                    ExpBBid,ExpBBcc,ExpBBdef = SecIDtoBloombergFetchInput(SecID,"Bond")
                    if ExpBBcc:
                        ccforcur = ExpBBcc
    #            elif bond['tickertype'] in ('SEDOL'):
    #                ExpBBid,ExpBBcc,ExpBBdef = bond['id'],bond['country'],bond['name']
    #                ccforcur = ExpBBcc
                else:
                    raise Exception("Unknown tickertype in Equity DB")
            else:
                #This shouldn't be a case, but it's either do this, or handle
                # a false SeCIDinfo and be blind to the bad securities lack of existence.
                desc = ['NoSecIDinfo']
                ExpBBid,ExpBBcc,ExpBBdef = SecIDtoBloombergFetchInput(SecID,"Bond")
                if ExpBBcc:
                    ccforcur = ExpBBcc
                
            try:
                allpossible = [ExpBBid,ExpBBcc,ExpBBdef]
                if not ExpBBcc:
                    allpossible[1] = "NoCtryCode"
                print allpossible
                BFSecurity = BB.GetSecurity(allpossible)
                BBSec = True
            except NoELID:
                desc = ["NoELID"] + desc
                BBSec = False
    
            if BBSec:
                ActBBid,ActBBcc,ActBBdef,elid = BFSecurity.anid,BFSecurity.anidcc,BFSecurity.aniddef,BFSecurity.ELID
                secs.append(Sec(BBFetch(elid,"PX_BID"),validity=v))
                About = BFSecurity.About()
                MetaExCode = BFSecurity.GetDataMostRecentFetchValue('EXCH_CODE')
                desc = ["ELID" + str(elid),ActBBid,ActBBcc,ActBBdef,MetaExCode,About['Name'],About['Security Type'],About['Security Info'],"price"] + desc
                if ActBBcc != "NoCtryCode":
                    ccforcur = ActBBcc            
            
            if len(secs) >= 1:
                TheInfo = Info(Category, 'Priority', ExCodetoCurMap[ccforcur], "_".join([str(d) for d in desc]) , 'B', validity=v)
                newsymbol = "price_{0}{1}".format(*SecID.split("."))
                SecurityInstruction(newsymbol,secs,TheInfo).Setup()
            else:
                CouldntAdd[Category].append(SecID)
        
    if AddGroupCheck('equitydiv'):
        #Adds payable and exdate dividends for all the equities in 
        #the equity DB, and all the equities in the Equity Important Holdings
        #ever named in bloomberg.
            
        Category = "equitydiv"
        CouldntAdd[Category] = []
        # This category adds prices for equities in EquityDB, and their
        # matching bloomberg data if available.
    
        from adders.bloombergfetch import ExCodetoCountryMap, ExCodetoCurMap, CountrytoExCodeGuessMap, CountrytoCurGuessMap
        import equitable.bpsdl as bf
        
        #2011-04-15 is basically, "Since inception" where currency is available
        SecIDs = SecPos('Equity').get_important_holdings(argDate=dt.date(2011,4,16))
            
        EquityDB = SmartDB('Equity')
    
        BB = bf.Getter.BBapi(clean=False)  
        
        #TODO this was really, REALLY hacky.  Can be sped up significantly.
        for divtype in ['payable','exdate']:        
            for SecID in SecIDs:
                 
                v = Validity()
                v.AddIgnoredException("NoData")
                v.AddIgnoredException("DataLoss")
            
                qry = """SELECT secid.secid AS secid, 
                           split_part(secid.name,'.',2) AS name, 
                           COALESCE(bl_info.bloomberg_cusip, bloomberg_sedol) AS id,
                           split_part(bl_info.ticker_and_exch_code,' ',2) AS exch_code,
                           split_part(secid.name,'.',1) AS country,
                           bl_info.bloomberg_cusip AS cusip,
                           bl_info.bloomberg_isin AS isin,
                           bl_info.bloomberg_sedol AS sedol,  
                           CASE WHEN bl_info.bloomberg_cusip IS NOT NULL THEN 'CUSIP' ELSE CASE WHEN bl_info.bloomberg_sedol IS NOT NULL THEN 'SEDOL' ELSE Null END END AS tickertype,
                           bl_info.long_desc AS long_desc, 
                           split_part(bl_info.ticker_and_exch_code,' ',1) AS ticker
                         FROM 
                           public.bl_info, 
                           public.secid
                         WHERE 
                           secid.secid = bl_info.secid AND
                           secid.name = '{}'
                         ORDER BY secid;"""
                         
                equities = EquityDB.getRowsDicts(qry.format(SecID))
                
                if len(equities) > 1:
                    raise Exception("Multiple SecID found, where expected one {}".format(SecID))
                elif len(equities) == 1:
                    eqty = equities[0]
                    SecIDinfo = True
                else:
                    SecIDinfo = False
                    
                secs = []
                desc = []
                
                if SecIDinfo:
                    secs.append(Sec(BL_Dividends(eqty['secid'],divtype),validity=v))
                    desc = [eqty['ticker'],eqty['long_desc']]
                    ccforcur = SecID[:2]
                    if eqty['tickertype'] in ('CUSIP','ISIN','SEDOL'):
                        ExpBBid,ExpBBcc,ExpBBdef = SecIDtoBloombergFetchInput(SecID,"Equity")
                        ccforcur = ExpBBcc
                    else:
                        raise Exception("Unknown tickertype in Equity DB")
                else:
                    #This shouldn't be a case, but it's either do this, or handle
                    # a false SeCIDinfo and be blind to the bad securities lack of existence.
                    desc = ['NoSecIDinfo']
                    ExpBBid,ExpBBcc,ExpBBdef = SecIDtoBloombergFetchInput(SecID,"Equity")
                    ccforcur = ExpBBcc
                    
                try:
                    BFSecurity = BB.GetSecurity([ExpBBid,ExpBBcc,ExpBBdef])
                    BBSec = True
                except NoELID:
                    desc = ["NoELID"] + desc
                    BBSec = False
                
                if BBSec:
                    ActBBid,ActBBcc,ActBBdef,elid = BFSecurity.anid,BFSecurity.anidcc,BFSecurity.aniddef,BFSecurity.ELID
                    secs.append(Sec(BBFetchBulkDivs(elid,divtype),validity=v))
                    About = BFSecurity.About()
                    MetaExCode = BFSecurity.GetDataMostRecentFetchValue('EXCH_CODE')
                    desc = ["ELID" + str(elid),ActBBid,ActBBcc,ActBBdef,MetaExCode,About['Name'],About['Security Type'],About['Security Info'],"price"] + desc
                    ccforcur = ActBBcc
                
                if len(secs) >= 1:
                    TheInfo = Info(Category, 'Priority', ExCodetoCurMap[ccforcur], "_".join([str(d) for d in desc]) , 'D', validity=v)
                    newsymbol = "div_{0}_{1}{2}".format(divtype,*SecID.split("."))
                    SecurityInstruction(newsymbol,secs,TheInfo).Setup()
                else:
                    CouldntAdd[Category].append(SecID)
    
    if AddGroupCheck('equityprices'):
        Category = "equityprices"
        CouldntAdd[Category] = []
        # This category adds prices for equities in EquityDB, and their
        # matching bloomberg data if available.
    
        from adders.bloombergfetch import ExCodetoCountryMap, ExCodetoCurMap, CountrytoExCodeGuessMap, CountrytoCurGuessMap
        import equitable.bpsdl as bf
        
        #2011-04-15 is basically, "Since inception" where currency is available
        SecIDs = SecPos('Equity').get_important_holdings(argDate=dt.date(2011,4,16))
            
        EquityDB = SmartDB('Equity')
    
        BB = bf.Getter.BBapi(clean=False)  
        
        for SecID in SecIDs:
             
            v = Validity()
            v.AddIgnoredException("NoData")
            v.AddIgnoredException("DataLoss")
        
            qry = """SELECT secid.secid AS secid, 
                       split_part(secid.name,'.',2) AS name, 
                       COALESCE(bl_info.bloomberg_cusip, bloomberg_sedol) AS id,
                       split_part(bl_info.ticker_and_exch_code,' ',2) AS exch_code,
                       split_part(secid.name,'.',1) AS country,
                       CASE WHEN bl_info.bloomberg_cusip IS NOT NULL THEN 'CUSIP' ELSE CASE WHEN bl_info.bloomberg_sedol IS NOT NULL THEN 'SEDOL' ELSE Null END END AS tickertype,
                       bl_info.long_desc AS long_desc, 
                       split_part(bl_info.ticker_and_exch_code,' ',1) AS ticker
                     FROM 
                       public.bl_info, 
                       public.secid
                     WHERE 
                       secid.secid = bl_info.secid AND
                       secid.name = '{}'
                     ORDER BY secid;"""
                     
            equities = EquityDB.getRowsDicts(qry.format(SecID))
            
            if len(equities) > 1:
                raise Exception("Multiple SecID found, where expected one {}".format(SecID))
            elif len(equities) == 1:
                eqty = equities[0]
                SecIDinfo = True
            else:
                SecIDinfo = False
                
            secs = []
            desc = []
            
            if SecIDinfo:
                secs.append(Sec(equity_ts(SecID),validity=v))
                desc = [eqty['ticker'],eqty['long_desc']]
                ccforcur = SecID[:2]
                if eqty['tickertype'] in ('CUSIP','ISIN','SEDOL'):
                    ExpBBid,ExpBBcc,ExpBBdef = SecIDtoBloombergFetchInput(SecID,"Equity")
                    ccforcur = ExpBBcc
                else:
                    raise Exception("Unknown tickertype in Equity DB")
            else:
                #This shouldn't be a case, but it's either do this, or handle
                # a false SeCIDinfo and be blind to the bad securities lack of existence.
                desc = ['NoSecIDinfo']
                ExpBBid,ExpBBcc,ExpBBdef = SecIDtoBloombergFetchInput(SecID,"Equity")
                ccforcur = ExpBBcc            
            try:
                BFSecurity = BB.GetSecurity([ExpBBid,ExpBBcc,ExpBBdef])
                BBSec = True
            except NoELID:
                desc = ["NoELID"] + desc
                BBSec = False
    
            if BBSec:
                ActBBid,ActBBcc,ActBBdef,elid = BFSecurity.anid,BFSecurity.anidcc,BFSecurity.aniddef,BFSecurity.ELID
                secs.append(Sec(BBFetch(elid,"PX_LAST"),validity=v))
                About = BFSecurity.About()
                MetaExCode = BFSecurity.GetDataMostRecentFetchValue('EXCH_CODE')
                desc = ["ELID" + str(elid),ActBBid,ActBBcc,ActBBdef,MetaExCode,About['Name'],About['Security Type'],About['Security Info'],"price"] + desc
                ccforcur = ActBBcc
    
            if len(secs) >= 1:
                TheInfo = Info(Category, 'Priority', ExCodetoCurMap[ccforcur], "_".join([str(d) for d in desc]) , 'B', validity=v)
                newsymbol = "price_{0}{1}".format(*SecID.split("."))
                SecurityInstruction(newsymbol,secs,TheInfo).Setup()  
            else:
                CouldntAdd[Category].append(SecID)    
    
    if AddGroupCheck('forex'):
        Category = "forex_rates" #TODO Rename this
        CouldntAdd[Category] = [] #TODO finish this for forex
        GeneralDB = pgw.dbConnect('General')
        
        TheSecurities = [Sec(BOC_FX('U.S. dollar',"fxtype='close'"),Priority=1),
                         Sec(EconB("FX.BOFC.CLOSING.US"),Unit='CAD per USD', Priority=2),
                         Sec(PamFxRates('USD'),Unit='CAD per USD')]
        TheInfo = Info(Category,'Priority','CAD per USD','Canadian Dollar for US Dollar only BofC closing values','B')
        SecurityInstruction('CADUSD_CLOSING_BOFC',TheSecurities,TheInfo).Setup()
        
        TheSecurities = \
            [Sec(BOC_FX('U.S. dollar',"fxtype='close'"),Unit='CAD per USD', Priority=1),
             Sec(EconB("FX.BOFC.CLOSING.US"),Unit='CAD per USD', Priority=2),
             Sec(PamFxRates('USD'),Unit='CAD per USD'),
             Sec(EconB("FX.BL.CA"),Unit='USD per CAD',UnitParse='^-1'),
             Sec(EconB('FX.BLINVERT.CA'),Unit='CAD per USD'),
             Sec(EconB('FX.CA'),Unit='CAD per USD'),
             Sec(EconB('FX.RBC.CA'),Unit='CAD per USD')]                             
        TheInfo = Info(Category,'Priority','CAD per USD','Canadian Dollar for US Dollar closing values','B')
        SecurityInstruction('CADUSD_CLOSING',TheSecurities,TheInfo).Setup()
        
        TheSecurities = \
            [Sec(BOC_FX('U.S. dollar',"fxtype='noon'"),Unit='CAD per USD', Priority=1),
             Sec(EconB("FX.BOFC.NOON.US"),Unit='CAD per USD', Priority=2),
             Sec(EconB('FX.NOON.CA'),Unit='CAD per USD'),
             Sec(EconB("FX.BL.CA"),Unit='USD per CAD',UnitParse='^-1'),
             Sec(EconB('FX.BLINVERT.CA'),Unit='CAD per USD'),
             Sec(EconB('FX.CA'),Unit='CAD per USD'),
             Sec(EconB('FX.RBC.CA'),Unit='CAD per USD')]                             
        TheInfo = Info(Category,'Priority','CAD per USD','Canadian Dollar for US Dollar noon values','B')
        SecurityInstruction('CADUSD_NOON',TheSecurities,TheInfo).Setup()
        
        TheSecurities = \
            [Sec(BOC_FX("European Euro","fxtype='close'"),Priority=1),
             Sec(EconB("FX.BOFC.CLOSING.EU"),Unit='CAD per EUR', Priority=2),
             Sec(PamFxRates('EUR'),Unit='CAD per EUR')]                      
        TheInfo = Info(Category,'Priority','CAD per EUR','Canadian Dollar for EURO only BofC closing values','B')
        SecurityInstruction('CADEUR_CLOSING_BOFC',TheSecurities,TheInfo).Setup()
        
        TheSecurities = [Sec(PamFxRates('EUR'),Unit='CAD per BMD')]
        TheInfo = Info(Category,'Priority','CAD per BMD','Canadian Dollar for Bermuda Dollar','B')
        SecurityInstruction('CADBMD_CLOSING_BOFC',TheSecurities,TheInfo).Setup()
        GeneralDB.close()
    
    if AddGroupCheck('dex'):
        # Add Dex Constituents
        # By Derek
        # Status : Working 12/22/2014
    
        Category = 'bond_indices'
        CouldntAdd[Category] = [] #TODO finish this for dex
        GeneralDB = pgw.dbConnect('General')
    
        DexMonthlyNames = GeneralDB.query("SELECT DISTINCT name FROM bondindices_dex_monthly").getresult()
        DexBNames = GeneralDB.query("SELECT DISTINCT name FROM bondindices_dex").getresult()
        EconScmVarTuples = GeneralDB.query("SELECT name FROM econ_info WHERE substring(name from 1 for 3)='SCM'").getresult()
        EconScmVars = [x[0] for x in EconScmVarTuples]
        DexScmVarDict = {'tri':'TRI', 'averageyield':'YIELD', 'priceindex':'PRICE','modifiedduration':'DURATION',
                         'macaulayduration':'MACDUR','averageterm':'TERM','averagecoupon':'COUPON',
                         'convexity':'CONVEX','sectorweight':'WEIGHT'}
    
        for indexName in DexMonthlyNames:
            Dex_Securities = []
            var='tri'
            if indexName in DexMonthlyNames:
                Dex_Securities.append(Sec(TMXBond('bondindices_dex_monthly',indexName[0],var)))
            if indexName in DexBNames:
                Dex_Securities.append(Sec(TMXBond('bondindices_dex',indexName[0],var)))
            econScmName = 'SCM.'+indexName[0].upper().replace('UNIVERSE','UNIV')+'.'+DexScmVarDict[var]
            print econScmName
            if econScmName in EconScmVars:
                 Dex_Securities.append(Sec(EconB(econScmName)))
            Dex_Info = Info(Category,'Priority',var.upper(),"FTSE TMX Canada "+indexName[0].upper().replace('.',' ')+" Bond Index "+var.upper(),'B')
            SecurityInstruction(indexName[0].upper().replace('.','_')+"_"+var.upper(),Dex_Securities,Dex_Info).Setup()
        
        GeneralDB.close()
    
    if AddGroupCheck('fewrates'): #TODO Rename this
        # Add Canadian inflation rate
        # By Ryan & Jeff
        # Status : Working 12/22/2014
    
        Category = "inflation_rates"  #TODO Rename this
        CouldntAdd[Category] = [] #TODO finish this for fewrates
        TheSecurities = [Sec(insTemplate('DB',Databases['General'],tablename="ref_cpi_canada_inflation",datefieldname="date",valuefieldname="ref_cpi",freqparse='B-reset'))]                
        TheInfo = Info(Category,'Priority','Index 100 @ 2002/06','Canadian Consumer Price Index Reference. Inflation. Published Monthly and Interpolated Daily','B')
        SecurityInstruction('ca_cpi_bofc_ref',TheSecurities,TheInfo).Setup()
    
        # Add libor, cdor, intrest rates
        # By Ryan & Jeff
        # Status : Working 12/22/2014
         
        gdb = SmartDB("General")
        lrates = gdb.getRowsDicts("SELECT name,long_desc FROM econ_info WHERE name LIKE '%LIBOR%' ORDER BY name ;")
        trmp_lrates = ["_".join(row['name'].replace("MONTH","M").split(".")) for row in lrates]
        
        crates = gdb.getRowsDicts("SELECT name,long_desc FROM econ_info WHERE name LIKE '%CDOR%' AND long_desc != '' ORDER BY name;")
        trmp_crates = ["_".join(row['name'].replace("MO","M").split(".")[:2]) for row in crates]
        
        rates = lrates + crates
        trmp_rates = trmp_lrates + trmp_crates
        Securities = zip(rates,trmp_rates)
    
        Category = "interest_rates"   #TODO Rename this  
        CouldntAdd[Category] = []
        for curSec,trmpname in Securities:     
    		TheSecurities = [Sec(Econ(curSec['name'],freqparse='B-reset'))]
    		TheInfo = Info(Category,'Priority','No Units',curSec['long_desc'],'B')
    		SecurityInstruction(trmpname,TheSecurities,TheInfo).Setup()
    
    if AddGroupCheck('futures'):
        Category = 'futures'
        CouldntAdd[Category] = []  #TODO finish this for futures
        # Adds futures which exist in the 3rd Open Source table in Bloomberg.
        # Creates price_* and open_interest_* from PX_SETTLE and OPEN_INT
        
        # Status : Working
        # By Jeff
        # Status : Working 12/22/2014
           
        # TODO : Change these all to B, after safefreq improvement.
           
        BloombergDB = SmartDB('Bloomberg')
        qry = """SELECT DISTINCT elid, ticker, name, security_des 
                        FROM bpsdl_fc3 WHERE security_typ2 = 'Future';"""
        Futures = BloombergDB.getRowsDicts(qry)
        for Fut in Futures:
            v = Validity()
            if ec.deltaYear(Fut['name'].split()[3]) >= 1.0:
                v.AddIgnoredException("NoData")
            print "Doing : " + str(Fut['elid'])
            for fld in ['PX_SETTLE','OPEN_INT']:
                Fut_Feeds = [Sec(BBFetch(Fut['elid'],fld))]
                if fld == 'PX_SETTLE':
                    trmp_sym_name = "price_" + Fut['ticker']
                    desc = " price at settlement"
                elif fld == 'OPEN_INT':
                    trmp_sym_name = "open_interest_" + Fut['ticker']
                    desc = " open interest at settlement"
                fulldesc = [str(Fut['elid']),Fut['security_des'],Fut['name'],desc]
                Fut_Info = Info(Category, 'Priority', 'USD per CAD', "_".join(fulldesc), 'D', validity=v)
                SecurityInstruction(trmp_sym_name,Fut_Feeds,Fut_Info).Setup()
    
    
    if AddGroupCheck('oneoffs'): #TODO Rename this
        Category = 'oneoffs' #TODO Rename this
        CouldntAdd[Category] = []  #TODO finish this for one-offs
        v = Validity()
        v.AddIgnoredException("DataLoss")
        
        tFeeds = [Sec(Econ('TSX.PREF.TRI'),validity=v),Sec(CCDJ('universecad'),validity=v,UnitMult=9.6702461248359925502348246810518)]
        tDesc = "S&P/TSX Preferred Total Return Index with Deep History by Desjardin"
        tInfo = Info(Category, 'Priority', 'CAD', tDesc, 'B')
        SecurityInstruction("tsx_pref_tri",tFeeds,tInfo).Setup()
            
    if AddGroupCheck('legacyecon'): #TODO Rename this
        Category = "legacy_economics" #TODO Rename this
        CouldntAdd[Category] = []  #TODO finish this for legacy_econ
        
        # Adds all econ symbols which aren't used already.  It does this 
        # by looking at econ, and used names in Trump's symbol table
        # it should always be the last group be to be added.
    
        # Status : Working, but some frequency cases might not be handled properly.
        
        from adders.legacyecon import EconBloombergList, freqmap, ConfirmFreq
        
        #Get a two-way lookup of bloomberg symbols in econ from static files.
        eb = EconBloombergList()
        
        #Get a list of all econ symbols in econ_info
        GeneralDB = pgw.dbConnect('General')
        qry = "SELECT DISTINCT name,long_desc,short_desc,description,freq FROM econ_info;"
        econ_info = GeneralDB.query(qry).dictresult()
        GeneralDB.close()
        
        v = Validity()
        v.AddIgnoredException('NoData')
        
        #Get list of econ table symbols, used in trump, excluding legacy
        TrumpDB = pgw.dbConnect('Trump')
        qry = """SELECT 
                  _symbolinfo.symbol, 
                  _instructions.keyfieldvalue
                FROM 
                  public._symbolinfo, 
                  public._instructions
                WHERE 
                  _symbolinfo.symbol = _instructions.symbol AND
                  _symbolinfo.category != 'legacyecon' AND 
                  _instructions.tablename = 'econ' ORDER BY keyfieldvalue;"""
        used = TrumpDB.query(qry).getresult()
        used = set([row[0] for row in used]) #For faster searching
        
        #loop through all the symbols in econ_info
        for sym in econ_info:
            if sym['name'] in used:
                pass #do this for faster searching ("not in" would be O(n), where 
                # in is < O(n))
                #TODO delete any symbols that have been used, from existing legacy symbols.
            else: #it's not in used...
                descoptions = [sym['short_desc'],sym['long_desc'],sym['description']]
                descoptions = [d for d in descoptions if d not in ('',None)]
                if len(descoptions) > 2:
                    if descoptions[0] in descoptions[1]:
                        del descoptions[0]
                desc = "DO NOT USE " + "|".join(descoptions)
            
                #TODO find a better way to set the units on the legacy econ symbols
                #TODO handle descriptions better in legacy econ symbols
                
                TheFeeds = [Sec(Econ(sym['name'],freqparse='noreset'))]
                
                
                print "Trying : " + sym['name']
                
                if eb.econexists(sym['name']):
                    print "ELID : "
                    elid = eb.elidfromecon(sym['name'])
                    print elid
                    BB,Yellow,Field = eb.bbSymbol(sym['name'])
                    #TODO A Missing elid in legacyecon shouldn't fail silently, on prod.
                    if elid is not None:
                        TheFeeds.append(Sec(BBFetch(elid,Field)))
                        print "Including the Bloomberg Source: " + str((BB,Yellow,elid,Field))
                    else:
                        print "Excluding the Bloomberg Source: " + str((BB,Yellow,Field))
                
                freq = ConfirmFreq(sym['name'],freqmap[sym['freq']])
                if freq is not None:
                    if freq != freqmap[sym['freq']]:
                        print "{0} was mapped to {1}".format(sym['freq'],freq)
                    TheInfo = Info(Category,'Priority','TODO',desc,freq,validity=v)
                    trump_sym_name = sym['name'].replace(".","_").replace("%","_per_")
                    SecurityInstruction("le_{}".format(trump_sym_name),TheFeeds,TheInfo).Setup()
                else:
                    print "{0} was not added to Trump.".format(sym['name'])
    
        TrumpDB = pgw.dbConnect('Trump')
        qry = "SELECT COUNT(category) FROM _symbolinfo WHERE category = 'legacyecon';"
        legacy_cnt = TrumpDB.query(qry).getresult()[0][0]
        
        #TODO Get rid of this legacy econ check and e-mail
        if legacy_cnt + len(used) != len(econ_info):
            msg = """The number of econ symbols deployed properly from General.Econ is {0}.\n
                   The number of symbols in Trump's legacyecon category is {1}.\n
                   The total number of General.Econ symbols is {2}.
                   Consider running setup.py to delete the extra symbols that have
                   been deployed properly.  This check, and message and associated logic
                   can be removed from the code base, after we have successfully implmented
                   and tested the deletion of symbol.  For now, if it's bothering you,
                   you can run Trump/setup.py to clean things up."""
            msg = msg.format(legacy_cnt,len(used),len(econ_info))
                   
            sendemail.core('trump',"TRUMP INFO: Legacy Econ Symbols Need Cleaning",aMsg=msg) 
    
    for k,v in CouldntAdd.iteritems():
        if len(v) >= 0:
            sendemail.core('trump',"TRUMP WARNING: Couldn't Add All Securities Excpected",aMsg=str(CouldntAdd)) 
            break
    
    raise Exception("test")

    if args.jobname != 'UNSPECIFIED':
        jobUpdate(args.jobname,'FINISHED')
except:
    msg = "This script was ran with the following arguments : {}\n".format(str(args))
    try:
        print Category
    except NameError:
        Category = "unknown"
    
    #TODO process the jobs in the order the arguments are passed, then
    # provide that list in the output of this error.
    
    msg = msg + "There was a problem during symbol addition. Based on the trace error below, one can determine which category was being worked on reliably.  The category determines downstream process implications. The last category DECLARED last, was {}.  Just because it was the last one declared, doesn't necessarily prove which category was being workg on, but it's likely a very good guess.".format(Category)
    sendemail.error("Failure during Symbol Addition",msg)
    if args.jobname != 'UNSPECIFIED':
        jobUpdate(args.jobname,'ERROR')    
    
        