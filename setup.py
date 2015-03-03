# -*- coding: utf-8 -*-

import psycopg2 as ps2


import datetime as dt

import subprocess

from equitable.utils import processtools as proct


dflt = DBpicker()

def InitializeDatabase():
    
    cn = ps2.connect(database='Trump')
    
    q = "SELECT table_name FROM information_schema.tables WHERE table_name = '_humanoverride';"
    cr = cn.cursor()
    cr.execute(q)
    existcheck = cr.fetchall()
           
    if len(existcheck) > 0:
        print "Normally would have backed-up _humanoverride"
        backupFile = sysenv.get('TRUMP_DIR') + "_humanoverride_dump_" + dt.datetime.now().strftime("%Y%m%d-%H%M%S") + ".sql"
        backupCmd = 'pg_dump -w --host=' + dflt.host + " --table=_humanoverride -U quants-admin --format=c --file=" + backupFile + " Trump" 
        retCode = subprocess.call(backupCmd)#, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        if retCode <> 0: raise RuntimeError(retCode)
    else:
        print "Didn't back up _humanoverride"
        backupFile = None
        
    q = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    cr = cn.cursor()
    cr.execute(q)
    existingtables = cr.fetchall()

    for i,table in enumerate(existingtables):
        print "Dropping Symbol : " + table[0]
        dropcmd = "DROP TABLE IF EXISTS " + table[0] + ";"
        cr.execute(dropcmd)
        if (i + 1) % 65 == 0:
            cn.commit()
    cn.commit()

    print "Done"    
#    "DROP TABLE IF EXISTS " + table[0] + ";"
#    drop 
#
#        cr.execute()
#        if i % 50:
#            cn.commit()
#    cn.commit()
    cn.close()
    return backupFile
    
def CreateTable(name,columns,pk,addindex=False):
    name = name.lower().replace(" ","_")
    print " Creating a new table : " + dflt.host + ".Trump"
    cn = ps2.connect(database='Trump')

    columnnames = [c[0] for c in columns]
    columntypes = [c[1] for c in columns]
    DropQry = "DROP Table IF EXISTS " + name + ";"
    
    
    cr = cn.cursor()
    cr.execute(DropQry)
    cn.commit()

    CreateQry = "CREATE TABLE " + name + " ( " + ",".join([x[0] + " " + x[1] for x in zip(columnnames,columntypes)]) + ", PRIMARY KEY (" + ",".join(pk) + "));"
    cr.execute(CreateQry)
    cn.commit()
    
    if addindex:
        CreateQry = "CREATE UNIQUE INDEX idx_lower_unique ON " + name + " (lower(symbol));"
        cn.query(CreateQry)        
    
    cr.execute("ALTER TABLE " + name + " OWNER TO quants;")
    cr.execute("GRANT ALL ON TABLE " + name + " TO quants;")
    cr.execute('GRANT ALL ON TABLE ' + name + ' TO "quants-admin";')
    cr.execute("GRANT SELECT ON TABLE " + name + " TO quantsread;")
    
    cn.commit()
    cn.close()

def RestoreHumanOverride(fname):
    print "Restoring : " + fname
    cmd = 'pg_restore -w --host=' + dflt.host + " -U quants-admin --format=c -d Trump " + fname
    print cmd
    retCode = subprocess.call(cmd)
    if retCode <> 0: raise RuntimeError(retCode)

_Instructions = [("Symbol" , 'text'),
                 ("Priority" , 'int'),
                 ("UnitMult" , 'text'),
                 ("Unit" , 'text'),
                 ("UnitParse" , 'text'),
                 ("Validity" , "json"),
                 ("InstructionType" , "text"),
                 ("ELID" , "integer"),
                 ("Database" , 'text'),
                 ("TableName" , 'text'),
                 ("ValueFieldName" , "text"),
                 ("DateFieldName" , "text"),
                 ("KeyFieldName" , "text"),
                 ("KeyFieldValue" , "text"),
                 ("FreqParse" , "text"),
                 ("SQLCrit" , "text"),
                 ("PyCrit1" , "text"),
                 ("PyCrit2" , "text"),
                 ("PyCrit3" , "text"),
                 ("PyCrit4" , "text")]

_SymbolInfo = [("Symbol" , 'text'),
               ("Category" , 'text'),
               ("TrumpMethod" , 'text'),
               ("Units" , 'text'),
               ("Description" , 'text'),
               ("Frequency" , 'text'),
               ("Validity" , "json")]

_HumanOverride = [("Symbol" , 'text'),
                  ("Datetime" , 'timestamp without time zone'),
                  ("MasterOR" , 'double precision'),
                  ("FailsafeOR" , 'double precision'),
                  ("Comment" , 'text'),
                  ("UserName" , 'text'),
                  ("ORDate",'timestamp without time zone DEFAULT CURRENT_TIMESTAMP')]

#TODO: Create this view, during setup.

_symbolinfo_w_inst_qry = """SELECT 
          _symbolinfo.symbol, 
          _symbolinfo.category,
          _instructions.priority, 
          _instructions.unitmult, 
          _instructions.unit, 
          _instructions.unitparse, 
          _instructions.validity, 
          _instructions.instructiontype, 
          _instructions.elid, 
          _instructions.database, 
          _instructions.tablename, 
          _instructions.valuefieldname, 
          _instructions.datefieldname, 
          _instructions.keyfieldname, 
          _instructions.keyfieldvalue, 
          _instructions.freqparse, 
          _instructions.sqlcrit, 
          _instructions.pycrit1, 
          _instructions.pycrit2, 
          _instructions.pycrit3, 
          _instructions.pycrit4
        FROM 
          public._instructions, 
          public._symbolinfo
        WHERE 
          _symbolinfo.symbol = _instructions.symbol #AND
          #_instructions.symbol = 'price_46430k108_ca'
        ORDER BY
          _instructions.symbol ASC, 
          _instructions.priority ASC
        """
_symbolinfo_w_inst = """CREATE OR REPLACE VIEW _symbolinfo_w_inst AS {};""".format(_symbolinfo_w_inst_qry)

print _symbolinfo_w_inst

if __name__ == '__main__':
    try:
        jobName = 'Trump_Setup'
        sendemail.devteam("Info Trump.setup.py: Setup is about to be run","Access to data in the Trump db via queries or other processes will be interupted during this process")
        BackupORfname = InitializeDatabase()
        CreateTable("_Instructions",_Instructions, pk = ["Symbol","Priority"])
        CreateTable("_SymbolInfo",_SymbolInfo, pk = ["Symbol"])
        if BackupORfname is not None:
            #Needed for any re-install.
            RestoreHumanOverride(BackupORfname)
        else:
            #Only needed for the first time.
            CreateTable("_HumanOverride",_HumanOverride, pk = ["Symbol","Datetime","ORDate"])
        proct.path_append(sysenv.get("trump_dir"))
        
        sendemail.devteam("Info Trump.setup.py: Setup is complete","Access should be restored for queries and processes")
        # Disable the job once completed since we will only run this when new additions need to be reflected
        jobs.jobStatusUpdate(jobName,'DISABLED')
    except:
        sendemail.error("Error Trump.setup.py: Unable to complete setup for Trump db")
        jobs.jobStatusUpdate(jobName,'ERROR')
