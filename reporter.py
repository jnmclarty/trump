
from itertools import combinations
import pandas as pd

#prefprices bondprices equityprices equitydiv


import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-dl', '--dolist', nargs='+', type=str)
args = parser.parse_args()

tdb = SmartDB("Trump",pointer='PROD')

qry = {}

for g in args.dolist:
    qry = "SELECT inst.*,info.description FROM _symbolinfo info FULL OUTER JOIN (SELECT symbol,COUNT(symbol) cnt FROM _instructions GROUP BY symbol) inst ON info.symbol = inst.symbol WHERE cnt >= 0 AND info.category = '{}' ORDER BY inst.symbol;".format(g)
    
    print qry

    syms,feedcnts,desc = tdb.getColsLists(qry)
    
    for x in zip(syms,feedcnts,desc):
        print x
        
    counts = dict(zip(syms,feedcnts))
    descs = dict(zip(syms,desc))
    
    def vptonum(x):
        x = x.replace("vp_","")
        return int(x)
        
    results = {}
    dfs = {}
    
    eqthresh = 0.01
    
    eql = "equ" + str(int(eqthresh*100))
    
    def almostequal(s1,s2,p=eqthresh):
        n = (s1.div(s2) - 1.0).abs().dropna() <= p
        return n
    
    for symbol in syms:
        print "*" * 20
        print symbol
        if counts[symbol] > 1:
            vps = ['vp_00' + str(x) for x in range(1,counts[symbol]+1)]
            vps = ",".join(vps)
            qry = "SELECT datetime,{} FROM {};".format(vps,symbol)
            df = tdb.getDataFrame(qry)
            
            results[symbol] = {}
            
            dfs[symbol] = df
            
            # Count shows the cases of number of data points per row.
            # So, a result of 0, 1, 4 means there are rows with no data, rows with one data point, and rows with two.
            
            results[symbol]['cnt'] = df.count(axis=1,numeric_only=True).unique().tolist()
            results[symbol]['cnt'].sort()
            results[symbol]['cnt'] = [str(x) for x in results[symbol]['cnt']]
            results[symbol]['cnt'] = ", ".join(results[symbol]['cnt'])
        
            # Standard Dev shows the number of unique standard deviations which exists
            # Across a row.  So, 2 likely means there are blanks, and a std deviation of 0.
            # Anything higher than 2 means there are rows that don't exactly match.
            # print df.std(axis=1,skipna=True)
            print df.std(axis=1,skipna=True).unique()
            
            results[symbol]['std'] =  len(df.std(axis=1,skipna=True).unique())
        
            # Length counts the number of data points in each field of data.  
            # Single digits, indicate a likely problem with a single feed.
            # A mismatch where there shouldn't be a mistmatch, could be a problem.
            
            results[symbol]['len'] = df.count(numeric_only=True).tolist()
            results[symbol]['len'] = [str(x) for x in results[symbol]['len']]
            results[symbol]['len'] = ", ".join(results[symbol]['len'])
        
            # Equality looks at the number of data points that are equal between
            # both feeds.  0 = a problem.  If the number is equal to the minimum of the number
            # in count, than the feeds match.
        
            results[symbol][eql] = {}
            
            for a,b in combinations(df.columns,2):
                na,nb = vptonum(a),vptonum(b)
                check = [True for x in list(df[a].values) + list(df[b].values) if x is None]
                print check
                if check:
                    results[symbol][eql][str(na) + ":" + str(nb)] = -1
                else:
                    results[symbol][eql][str(na) + ":" + str(nb)] = len([1 for x in almostequal(df[a],df[b]) if x])
                            
                                   
            results[symbol][eql] = ", ".join([k + " " + str(v) for k,v in results[symbol][eql].iteritems()])
        elif counts[symbol] == 1:
            vps = ['vp_00' + str(x) for x in range(1,counts[symbol]+1)]
            vps = ",".join(vps)
            qry = "SELECT datetime,{} FROM {};".format(vps,symbol)
            df = tdb.getDataFrame(qry)
            
            results[symbol] = {}
            
            dfs[symbol] = df
            
            # Count shows the cases of number of data points per row.
            # So, a result of 0, 1, 4 means there are rows with no data, rows with one data point, and rows with two.
            
            results[symbol]['cnt'] = df.count(axis=1,numeric_only=True).unique().tolist()
            results[symbol]['cnt'].sort()
            results[symbol]['cnt'] = [str(x) for x in results[symbol]['cnt']]
            results[symbol]['cnt'] = ", ".join(results[symbol]['cnt'])
        
            # Standard Dev shows the number of unique standard deviations which exists
            # Across a row.  So, 2 likely means there are blanks, and a std deviation of 0.
            # Anything higher than 2 means there are rows that don't exactly match.
            # print df.std(axis=1,skipna=True)
            
            results[symbol]['std'] =  0
        
            # Length counts the number of data points in each field of data.  
            # Single digits, indicate a likely problem with a single feed.
            # A mismatch where there shouldn't be a mistmatch, could be a problem.
            
            results[symbol]['len'] = df.count(numeric_only=True).tolist()
            results[symbol]['len'] = [str(x) for x in results[symbol]['len']]
            results[symbol]['len'] = ", ".join(results[symbol]['len'])
        
            # Equality looks at the number of data points that are equal between
            # both feeds.  0 = a problem.  If the number is equal to the minimum of the number
            # in count, than the feeds match.
                                           
            results[symbol][eql] = "ONE"
        elif counts[symbol] == 0:

            results[symbol] = {}
            
            dfs[symbol] = df
            
            results[symbol]['cnt'] = 0
            
            results[symbol]['std'] = 0
                   
            results[symbol]['len'] = 0
                                           
            results[symbol][eql] = 0
    
    data = { k : [] for k in results[symbol]}
    data['desc'] = desc
    
    index = []
    for symbol in syms:
        s = results[symbol]
        print "{} cnt:{}\tstd:{}\tlen:{}\t{}:{}".format(symbol,s['cnt'],s['std'],s['len'],eql,s[eql])
        for key in s:
            data[key].append(s[key])
        index.append(symbol)
    
    report = pd.DataFrame(data=data,index=index)
    
    
    report = report.sort([eql,'len','std'])
    report.to_html('report-{}.html'.format(g))
    report.to_excel('report-{}.xls'.format(g))
    
    #vp1, vp2 = df['vp_001'], df['vp_002']
    
    
    #dfg = df.groupby(['equ']).count()['index']
    
    #df = df.reset_index()
    
        #print "  std test : {}".format(s['std_test'])
        #print "  len test : {}".format(s['len_test_str'])