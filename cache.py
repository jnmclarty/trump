import argparse 

parser = argparse.ArgumentParser(description='Recache one, some, or all securities')
parser.add_argument('-sc','--symbol_crit', default=False, help='SQL criteria used to identify which symbol to recache.  Unset does all securities.')
parser.add_argument('-cc','--category_crit', default=False, help='SQL criteria used to identify which symbol to recache.  Unset does all securities.')
parser.add_argument('-nlcc','--not_like_category',  action='store_true', default=False, help='Inverses the SQL criteria')
parser.add_argument('-nlsc','--not_like_symbol', action='store_true', default=False, help='Inverses the SQL criteria')

args = parser.parse_args()

from equitable.infrastructure import jobs

from equitable.trumpapi.process import SymbolCacher

from equitable.infrastructure import sendemail

from equitable.db.psyw import SmartDB

print "Running : {}".format(args)

try:
    job = 'TRUMP_CACHE'
        
    if args.symbol_crit == False and args.category_crit == False:
        SymbolCacher().Process()
        jobs.jobStatusUpdate(job,'FINISHED')
    else:
        def makesql(pfx,crit,inv,like=True):
            n = ""
            if inv:
                n = "NOT"
            if crit:
                if like:
                    sql = "{} {} LIKE '{}'".format(pfx,n,crit)
                else:
                    sql = "{} {} IN ('{}')".format(pfx,n,crit)
            return sql
            
        qry = []

        if args.symbol_crit:
            symbol_sql = makesql("symbol", args.symbol_crit,args.not_like_symbol)
            qry.append(symbol_sql)

        if args.category_crit:
            cats = "','".join([c.strip(" ") for c in args.category_crit.split(",")])
            category_sql = makesql("category", cats,args.not_like_category,like=False)
            qry.append(category_sql)          
        
        qry = " AND ".join(qry)
        if len(qry) > 0:
            qry = "WHERE " + qry
        qry = "SELECT DISTINCT symbol FROM _symbolinfo {} ORDER BY symbol;".format(qry)
        sdb = SmartDB('Trump')
        cn = sdb.con
        
        cr = cn.cursor()
        cr.execute(qry)
        symbols = cr.fetchall()
        symbols = [s[0] for s in symbols]
        cr.close()
        sdb.disconnect()
    
        sc = SymbolCacher(DoInit=False)
            
        if len(symbols) > 0:
            for symbol in symbols:
                sc.Process(symbol,ReInit=True)
                
            jobs.jobStatusUpdate(job,'FINISHED')
        else:
            sendemail.error("No Trump Symbols found for the criteria : " + str(args.symbol_crit))
            jobs.jobStatusUpdate(job,'ERROR')
except:
    sendemail.error("Error in Trump.recache: Unable to complete caching")
    jobs.jobStatusUpdate(job,'ERROR')