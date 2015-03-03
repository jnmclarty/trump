

tr = SmartDB('Trump','PROD')

for x in ['equityprices','equitydiv']:#,'equityprices','prefprices','equitydiv']:
    todelete = tr.getOneColumn("SELECT DISTINCT symbol FROM _symbolinfo WHERE category='{}';".format(x))

    for s in todelete:
        tr.deletetable(s)
        tr.deletefrom('_symbolinfo',"symbol = '{}'".format(s))
        tr.deletefrom('_instructions',"symbol = '{}'".format(s))
tr.disconnect()