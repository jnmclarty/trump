
import pandas as pd

ptr = 'PROD'

t = SmartDB('Trump',pointer=ptr)
b = SmartDB('Bloomberg',pointer=ptr)
p = SmartDB('Pref',pointer=ptr)

def pdindexsearch(df,s,source):
    mask = [True if s in x else False for x in df.index]
    new = df[mask]
    return list(new[new['whr'] == source]['vals'].values)
    
def CheckSecurity(trumpsymbol,expected_elid,expected_name):
    tr = t.getRowsDicts("SELECT * FROM _symbolinfo_w_inst WHERE symbol = '{}';".format(trumpsymbol))
    br = b.getOneDict("SELECT * FROM bpsdl_fc2 WHERE elid = {};".format(expected_elid))
    pr = p.getOneDict("SELECT * FROM pref_ts_info WHERE name = '{}';".format(expected_name))
    
    ind = []
    dat = []
    whr = []
    
    i = 0
    for row in tr:
        for k,v in row.iteritems():
            ind.append(str(k) + "_r" + str(i))
            dat.append(v)
            whr.append('tr')
        i = i + 1
    
    def unpk(d,n):
        for k,v in d.iteritems():
            ind.append(k)
            dat.append(v)    
            whr.append(n)
    
    unpk(pr,'pr')
    unpk(br,'br')
    
    res = pd.DataFrame(index=ind,data={'vals' : dat, 'whr' : whr})
    res = res.sort_index()

    res.to_html("CheckSecurity{}-{}-{}-full.html".format(trumpsymbol,expected_elid,expected_name))
    def savr(c):
        res[res['whr'] == c].to_html("CheckSecurity{}-{}-{}-{}.html".format(trumpsymbol,expected_elid,expected_name,c))
    savr('tr')
    savr('br')
    savr('pr')
    
    if expected_elid not in pdindexsearch(res,'elid','br'):
        print "Problem with ELID 1"
    if expected_elid not in pdindexsearch(res,'elid','tr'):
        print "Problem with ELID 2"
        
    if expected_name not in pdindexsearch(res,'name','pr'):
        print "Problem with name 1"
    if expected_name not in pdindexsearch(res,'keyfieldvalue','tr'):
        print "Problem with name 2"
        
    return res

#CheckSecurity('price_780087748_ca',4365,'CA.780087748') #Looks like a very illiquid security, but it's all Royal in Canada.
#CheckSecurity('price_26857q200_ca',4240,'CA.726857q200') #Looks like a very illiquid security, but it's all Royal in Canada.
#CheckSecurity('price_455871400_ca',4302,'CA.455871400') #Looks like a very illiquid security, but it's all Royal in Canada.

CheckSecurity('price_112585864_ca',4192,'CA.112585864')
