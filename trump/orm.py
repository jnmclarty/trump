# -*- coding: utf-8 -*-
"""
Created on Sat Dec 20 15:50:37 2014

@author: Jeffrey
"""

import pandas as pd

import datetime as dt

from sqlalchemy import event, Table, Column, ForeignKey, ForeignKeyConstraint
from sqlalchemy import String, Integer, Float, DateTime, MetaData
from sqlalchemy import func

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.exc import ProgrammingError

from sqlalchemy.sql.expression import insert, delete


from sqlalchemy.sql import and_

from sqlalchemy.orm.session import object_session

from sqlalchemy.orm import sessionmaker
from tools import ReprMixin, ProxyDict, isinstanceofany
from sqlalchemy import create_engine

from extensions.symbol_aggs import apply_row, choose_col
from extensions.feed_munging import munging_methods

from templates import tFeed

engine = create_engine(r'postgresql://TODO:TODO@localhost/TrumpORM')

Base = declarative_base()
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

metadata = MetaData(bind=engine)

cc = {'onupdate' : "CASCADE", 'ondelete' : "CASCADE"}
      
checkpoints = ('EXCEPTION','CHECK')
state = ('ENABLED','DISABLED','ERROR')

class SymbolManager(object):
    def __init__(self,ses=session):
        self.ses = session
    def create(self,name,description=None,freq=None,units=None,agg_method="PRIORITY_FILL"):
        sym = self.try_to_get(name)
        if sym is not None:
            print sym.name
            self.ses.delete(sym)
            self.ses.commit() 
        sym = Symbol(name,description,freq,units,agg_method)
        sym.addAlias(name)
        return sym      
    def delete(self,symbol):
        if isinstance(symbol,str):
            sym = self.get(symbol)
        elif isinstance(symbol,Symbol):
            sym = symbol
        del sym
        self.ses.commit()
    def complete(self):
        self.ses.commit()
    def exists(self,symbol):
        syms = self.ses.query(Symbol).filter(Symbol.name == symbol).all()
        if len(syms) == 0:
            return False
        else:
            return True
    def get(self,symbol):
        syms = self.try_to_get(symbol)
        if syms == None:
            raise Exception("Symbol {} does not exist".format(symbol))
        else:
            return syms[0]
    def try_to_get(self,symbol):
        syms = self.ses.query(Symbol).filter(Symbol.name == symbol).all()
        if len(syms) == 0:
            return None
        else:
            return syms[0]    

class Symbol(Base, ReprMixin):
    __tablename__ = '_symbols'
    
    name =        Column('name',String,primary_key = True)
    description = Column('description',String)
    freq =        Column('freq',String)
    units =       Column('units',String)
    agg_method =   Column('agg_method',String)
    
    tags =        relationship("SymbolTag",cascade="all, delete-orphan")
    aliases =     relationship("SymbolAlias",cascade="all, delete-orphan")
    validity =    relationship("SymbolValidity",cascade="all, delete-orphan")
    feeds =       relationship("Feed",cascade="all, delete-orphan")
    
    #overrides =   relationship("Override",cascade="save-update",passive_deletes=True)
    
    def __init__(self,name,description=None,freq=None,units=None,agg_method="PRIORITY_FILL"):
        """
        agg_method : see extensions.symbol_aggs.py
        
        """         
        self.name = name
        self.description = description
        self.freq = freq
        self.units = units
        self.agg_method = agg_method
        self.datatable = False
    def cache_feed(self,fid):
        raise NotImplemented
    def cache(self):
        
        data = []
        cols = ['final','override_feed000','failsafe_feed999']
        
        #TODO : Take out the override columns.  That behavious should be
        #       Extension agnostic.
        
        for f in self.feeds:
            f.cache()
            data.append(f.data)
            cols.append(f.data.name)
        
        
        data = pd.concat(data,axis=1)
        
        l = len(data)
        data['override_feed000'] = [None] * l
        data['failsafe_feed999'] = [None] * l

        os = object_session(self)
        
        gb = os.query(Override.dt_ind,func.max(Override.dt_log).label('max_dt_log')).group_by(Override.dt_ind).subquery()
        ords = os.query(Override).join((gb, and_(Override.dt_ind == gb.c.dt_ind, Override.dt_log == gb.c.max_dt_log))).all()
              
        for row in ords:
            data.loc[row.dt_ind,'override_feed000'] = row.value

        gb = os.query(FailSafe.dt_ind,func.max(FailSafe.dt_log).label('max_dt_log')).group_by(FailSafe.dt_ind).subquery()
        ords = os.query(FailSafe).join((gb, and_(FailSafe.dt_ind == gb.c.dt_ind, FailSafe.dt_log == gb.c.max_dt_log))).all()
              
        for row in ords:
            data.loc[row.dt_ind,'failsafe_feed999'] = row.value

        if self.agg_method in apply_row:
            data['final'] = data.apply(apply_row[self.agg_method],axis=1)
        elif self.agg_method in choose_col:
            data['final'] = choose_col[self.agg_method](data)
        
        
        delete(self.datatable).execute()
        
        #os = object_session(self)
        #os.execute(self.datatable.delete())
        #os.commit()
        #os.execute("ALTER TABLE '{0}.id' AUTO_INCREMENT = 0;".format(self.name))
        
        for index, row in data.iterrows():
            vals = {k : row[k] for k in cols}
            vals['datetime'] = index
            insert(self.datatable,vals).execute()
    
        #self.datatable.


    #Feed has to fetch
    #Feed has to reindex
    #Feed has to munge
    #Feed has to apply
    #Symbol has to choose or apply
        
                
    @property
    def describe(self):
        s = []
        s.append("Symbol = {}".format(self.name))
        if len(self.tags):
            s.append("  tagged = {}".format(", ".join(x.tag for x in self.tags)))
        if len(self.aliases):
            s.append("  aliased = {}".format(", ".join(x.alias for x in self.aliases)))
        if len(self.validity):
            s.append("  validity =")
            printed_cp = []
            for v in self.validity:
                if v.checkpoint not in printed_cp:
                    printed_cp.append(v.checkpoint)
                    s.append("    Checkpoint = {}".format(v.checkpoint))
                s.append("      {} -> {} : {}".format(v.logic,v.key,v.value))
        if len(self.feeds):
            s.append("  feeds:")
        
            for f in self.feeds:
                s.append("    {}. {} -> {}".format(f.fnum,f.ftype,f.source_str()))
                printed_cp = []
                for v in f.validity:
                    if v.checkpoint not in printed_cp:
                        printed_cp.append(v.checkpoint)
                        s.append("      Checkpoint = {}".format(v.checkpoint))
                    s.append("        {} -> {} : {}".format(v.logic,v.key,v.value))        
        return "\n".join(s)
                   
    def addTag(self,tag):
        os = object_session(self)
        tmp = SymbolTag(tag=tag,symbol=self)
        self.tags.append(tmp)
        os.add(tmp)
        os.commit()
   
    def addOverride(self,dt_ind,value,dt_log=None,user=None,comment=None):
        os = object_session(self)
        
        if not dt_log:
            dt_log = dt.datetime.now()
            
        tmp = Override(symname=self.name,dt_ind=dt_ind,value=value,dt_log=dt_log,user=user,comment=comment)
        os.add(tmp)
        os.commit()

    def addFailSafe(self,dt_ind,value,dt_log=None,user=None,comment=None):
        os = object_session(self)
        
        if not dt_log:
            dt_log = dt.datetime.now()
            
        tmp = FailSafe(symname=self.name,dt_ind=dt_ind,value=value,dt_log=dt_log,user=user,comment=comment)
        os.add(tmp)
        os.commit()
        
    def delTag(self):
        raise NotImplemented
        
    def addTags(self,tags):
        os = object_session(self)        
        tmps = [SymbolTag(tag=t,symbol=self) for t in tags]
        os.add_all(tmps)
        os.commit()
    
    @property
    def n_tags(self):
        return len(self.tags)

    def addFeed(self,obj,**kwargs):
        if 'fnum' in kwargs:
            fnum = kwargs['fnum']
            del kwargs['fnum']
        else:
            fnum = None
            
        if isinstance(obj,dict):
            raise NotImplemented
        if isinstance(obj,tFeed):
            for kw in kwargs:
                if kw == 'munging':
                    obj.addMungeTemplate(kwargs[kw])
                elif kw in munging_methods:
                    obj.addMungeTemplate(kw,kwargs[kw])
            f = Feed(self,obj.ftype,obj.sourcing,obj.munging,obj.validity,fnum)
        if isinstance(obj,Feed):
            f = obj
        self.feeds.append(f)
        session.add(f)
    def addAlias(self,obj):
        if isinstance(obj,list):
            raise NotImplemented
        elif isinstanceofany(obj,(str,unicode)):
            a = SymbolAlias(self,obj)
            self.aliases.append(a)
            session.add(a)             
    def data(self):
        t = self.datatable
        return session.query(t.c.datetime,t.c.final).all()
             
    def delFeed(self):
        raise NotImplemented

    def addFeeds(self,symbol,ftype,sourcing,munging=None,fnum=None):
        raise NotImplemented
        
    @property
    def n_feeds(self):
        return len(self.feeds)

    def addValidity(self,checkpoint,logic,kwargs):
        for key,value in kwargs.iteritems():
            self.validity.append(SymbolValidity(checkpoint=checkpoint,logic=logic,key=key,value=value,symbol=self))
        #self = session.merge(self)
        session.commit()
        
    def setDescription(self,description):
        self.description = description

    def setFreq(self,freq):
        self.freq = freq
        
    def setUnits(self,units):
        self.units = units
        
    def InitializeDataTable(self):
        self.datatable = self._DataTableFactory()
        self.datatable.create(checkfirst=True)
        
    def _DataTableFactory(self):
        feed_cols = ['feed{0:03d}'.format(i+1) for i in range(self.n_feeds)]
        feed_cols = ['override_feed000'] + feed_cols + ['failsafe_feed999']
        
        t = Table(self.name, metadata, 
                  Column('datetime', DateTime, primary_key=True),
                  Column('final', Float),
                  *(Column(feed_col, Float) for feed_col in feed_cols),
                  extend_existing=True)
        return t

class SymbolTag(Base, ReprMixin):
    __tablename__ = '_symbol_tags'
    symname = Column('symname',String,ForeignKey('_symbols.name', **cc),  primary_key = True)
    tag =  Column('tag',String, primary_key = True)
    symbol = relationship("Symbol")
    def __init__(self,symbol,tag):
        self.symbol = symbol
        self.tag = tag

class SymbolAlias(Base, ReprMixin):
    __tablename__ = '_symbol_aliases'               
    symname = Column('symname',String,ForeignKey('_symbols.name', **cc), primary_key = True)
    alias = Column('alias',String, primary_key = True)
    
    symbol = relationship("Symbol")  
    def __init__(self,symbol,alias):
        self.symbol = symbol
        self.alias = alias

class SymbolValidity(Base, ReprMixin):
    __tablename__ = "_symbol_validity"
    
    symname = Column('symname', String, ForeignKey("_symbols.name", **cc), primary_key = True)
    checkpoint = Column('checkpoint',String, primary_key = True)
    logic = Column('logic',String, primary_key = True)
    key = Column('key',String, primary_key = True)
    value = Column('value',String)

    symbol = relationship("Symbol")   
    def __init__(self,symbol,checkpoint,logic,key,value=None):
        self.symbol = symbol
        self.checkpoint = checkpoint
        self.logic = logic
        self.key = key
        self.value = value
     
@event.listens_for(Symbol, 'load')
def __receive_load(target, context):
    target.InitializeDataTable()

class Feed(Base, ReprMixin):
    __tablename__ = "_feeds"
    
    symname = Column('symname',String, ForeignKey("_symbols.name", **cc), primary_key = True)
    fnum = Column('fnum',Integer,primary_key = True)
       
    state = Column('state',String,nullable=False)
    ftype = Column('ftype',String,nullable=False)
    
    tags = relationship("FeedTag",cascade="all, delete-orphan")
    sourcing = relationship("FeedSource",lazy="dynamic",cascade="all, delete-orphan")
    meta = relationship("FeedMeta",lazy="dynamic",cascade="all, delete-orphan")
    munging = relationship("FeedMunge",lazy="dynamic",cascade="all, delete-orphan")
    validity = relationship("FeedValidity",lazy="dynamic",cascade="all, delete-orphan")
       
    symbol = relationship("Symbol")

    def __init__(self,symbol,ftype,sourcing,munging=None,validity=None,meta=None,fnum=None):
        self.ftype = ftype
        self.state = "ON"
        self.symbol = symbol
        self.data = None
        
        if fnum is None:
            existing_fnums = session.query(Feed.fnum).filter(Feed.symname == symbol.name).all()
            existing_fnums = [n[0] for n in existing_fnums]
            if len(existing_fnums) == 0:
                self.fnum = 0
            else:
                self.fnum = max(existing_fnums) + 1
        else:
            self.fnum = fnum
            
        def settypesinmeta(t,o):
            if o is not None:
                if t in o:
                    self.meta.append(FeedMeta(attr=t,value=o[t],feed=self))
                    del o[t]
            else:
                self.meta.append(FeedMeta(attr=t,value='undefined',feed=self))
        
        settypesinmeta('stype',sourcing)
        settypesinmeta('mtype',munging)
        settypesinmeta('vtype',validity)

        for key in meta:
            self.meta.append(FeedMeta(attr=key,value=meta[key],feed=self))
                
        for key in sourcing:
            self.sourcing.append(FeedSource(param=key,value=sourcing[key],feed=self))

        if munging != None:            
            for i,method in enumerate(munging):
                fm = FeedMunge(order=i,method=method,feed=self)
                for arg,value in munging[method].iteritems():
                    fm.methodargs.append(FeedMungeArg(arg,value,feedmunge=fm))
                self.munging.append(fm)

        if validity is dict:
            for checkpoint in validity:
                for logic in checkpoint:
                    for key in logic:
                        self.validity.append(FeedValidity(checkpoint,logic,key,logic[key]))
    def cache(self):
        if self.ftype == 'Quandl':
            print "Getting Quandle data for {}".format(self.symbol.name)
            import Quandl as q
            self.data = q.get(**self.sourcing_dict())
            self.data = self.data[self.sourcing_dict()['fieldname']]
            self.data.name = "feed" + str(self.fnum+1).zfill(3)
        
        for m in self.munging:
            args = {}
            for a in m.methodargs:
                args[a.arg] = a.value
            self.data = munging_methods[m.method](self.data,**args)

    @property
    def sourcing(self):
        return ProxyDict(self, 'sourcing', FeedSource, 'param')

    @property
    def meta(self):
        return ProxyDict(self, 'sourcing', FeedMeta, 'attr')
     
    @property         
    def source(self):
        return " ".join([p.key + " : " + p.value for p in self.sourcing])
               
class FeedTag(Base, ReprMixin):
    __tablename__ = '_feed_tags'
    symname = Column('symname',String,  primary_key = True)
    fnum = Column('fnum',Integer,  primary_key = True)
    
    tag =  Column('tag',String, primary_key = True)
    
    feed = relationship("Feed")

    __table_args__ = (ForeignKeyConstraint([symname,fnum],[Feed.symname,Feed.fnum], **cc),{})
         
class FeedSource(Base, ReprMixin):
    __tablename__ = "_feed_sourcing"
    
    symname = Column('symname',String, primary_key = True)
    fnum = Column('fnum',Integer, primary_key = True)
    param =  Column('param',String, primary_key = True)
    
    feed = relationship("Feed")
    
    value = Column('value',String)
    
    __table_args__ = (ForeignKeyConstraint([symname,fnum],[Feed.symname,Feed.fnum], **cc),{})
    def __init__(self,feed,param,value):
        self.feed = feed
        self.param = param
        self.value = value

class FeedMeta(Base, ReprMixin):
    __tablename__ = "_feed_meta"
    
    symname = Column('symname',String, primary_key = True)
    fnum = Column('fnum',Integer, primary_key = True)
    attr =  Column('attr',String, primary_key = True)
    
    feed = relationship("Feed")
    
    value = Column('value',String)
    
    __table_args__ = (ForeignKeyConstraint([symname,fnum],[Feed.symname,Feed.fnum], **cc),{})
    def __init__(self,feed,attr,value):
        self.feed = feed
        self.attr = attr
        self.value = value
        
class FeedMunge(Base, ReprMixin):
    __tablename__ = "_feed_munging"
    
    symname = Column('symname',String, primary_key = True)
    fnum = Column('fnum',Integer, primary_key = True)
    order =  Column('order',Integer, primary_key = True)
    method =  Column('method',String)
    
    feed = relationship("Feed")
    
    methodargs = relationship("FeedMungeArg",lazy="dynamic",cascade="all, delete-orphan")
    
    __table_args__ = (ForeignKeyConstraint([symname,fnum],[Feed.symname,Feed.fnum]),{})
    def __init__(self,order,method,feed):
        self.order = order
        self.method = method
        self.feed = feed
        
class FeedMungeArg(Base, ReprMixin):
    __tablename__ = "_feed_munging_args"
    
    symname = Column('symname',String, primary_key = True)
    fnum = Column('fnum',Integer, primary_key = True)
    order =  Column('order',Integer, primary_key = True)
    arg =  Column('arg',String, primary_key = True)    
    value = Column('value',String)

    feedmunge = relationship("FeedMunge")
    
    __table_args__ = (ForeignKeyConstraint([symname,fnum,order],[FeedMunge.symname,FeedMunge.fnum,FeedMunge.order]),{})
    def __init__(self,arg,value,feedmunge):
        self.arg = arg
        self.value = value
        self.feedmunge = feedmunge
        
class FeedValidity(Base, ReprMixin):
    __tablename__ = "_feed_validity"
    
    symname = Column('symname',String, primary_key = True)
    fnum = Column('fnum',Integer, primary_key = True)
    checkpoint = Column('checkpoint',String, primary_key = True)
    logic = Column('logic',String, primary_key = True)
    key = Column('key',String, primary_key = True)

    feed = relationship("Feed")
    
    value = Column('value',String)

    __table_args__ = (ForeignKeyConstraint([symname,fnum],[Feed.symname,Feed.fnum]),{})
    def __init__(self,feed,checkpoint,logic,key,value=None):
        self.feed = feed
        self.checkpoint = checkpoint
        self.logic = logic
        self.key = key
        self.value = value     


class Override(Base, ReprMixin):
    __tablename__ = '_overrides'

    symname = Column('symname',String, primary_key = True)
    ornum = Column('ornum',Integer,primary_key = True)
    dt_ind = Column('dt_ind',DateTime, nullable=False)

    value = Column('value',Float,nullable=False)
     
    dt_log = Column('dt_log',DateTime,nullable=False)
    user = Column('user',String,nullable=True)
    comment = Column('comment',String,nullable=True)

class FailSafe(Base, ReprMixin):
    __tablename__ = '_failsafes'

    symname = Column('symname',String, primary_key = True)
    fsnum = Column('fsnum',Integer,primary_key = True)
    dt_ind = Column('dt_ind',DateTime, nullable=False)

    value = Column('value',Float,nullable=False)
     
    dt_log = Column('dt_log',DateTime,nullable=False)
    user = Column('user',String,nullable=True)
    comment = Column('comment',String,nullable=True)
    
    
drops = """DROP TABLE IF EXISTS _symbols CASCADE;
DROP TABLE IF EXISTS _symbol_validity CASCADE;
DROP TABLE IF EXISTS _symbol_tags CASCADE;
DROP TABLE IF EXISTS _symbol_aliases CASCADE;
DROP TABLE IF EXISTS _feeds CASCADE;
DROP TABLE IF EXISTS _feed_munging CASCADE;
DROP TABLE IF EXISTS _feed_sourcing CASCADE;
DROP TABLE IF EXISTS _feed_validity CASCADE;"""

try:
    Base.metadata.create_all(engine)
    print "Done creating"
except ProgrammingError as e:
    print e.statement
    print e.message
    raise 
#conn = engine.connect()
#qry = symbols.insert().values(symbol="test",description="This is a test symbol",freq="B",units="%")
#result = conn.execute(qry)
#print result.inserted_primary_key
#print qry


# Bind the engine to the metadata of the Base class so that the
# declaratives can be accessed through a DBSession instance

# A DBSession() instance establishes all conversations with the database
# and represents a "staging zone" for all the objects loaded into the
# database session object. Any change made against the objects in the
# session won't be persisted into the database until you call
# session.commit(). If you're not happy about the changes, you can
# revert all of them back to the last commit by calling
# session.rollback()

if __name__ == '__main__':
    import time
    
    for x in range(3):
        un = str(x) + dt.now().strftime("%H%M%S")
        print un
        time.sleep(1.1)
        
        NewSymbol = Symbol(name="NewSymbol" + un,description = 'tester chester')
        NewSymbol.setFreq('D')
        
        print NewSymbol.tags
        print NewSymbol.n_tags
        
        NewSymbol.addTag("Alpha")
        NewSymbol.addTags(["Beta","Charlie","Delta"])
        
        print NewSymbol.tags
        print NewSymbol.n_tags
        
        session.commit()
        
        print NewSymbol.tags
        print NewSymbol.n_tags
    
        
        SymbolAlias1 = SymbolAlias(alias="Newish" + un,symbol=NewSymbol)
        SymbolAlias2 = SymbolAlias(alias="Newer" + un,symbol=NewSymbol)
    
        session.add_all([NewSymbol,SymbolAlias1,SymbolAlias2])
        session.commit()
        #session.merge(NewSymbol)
        
        print NewSymbol.tags
        print NewSymbol.n_tags
        
        vals = [SymbolValidity(checkpoint='Exception',logic='NoData',key='arg1',value=1,symbol=NewSymbol),
                SymbolValidity(checkpoint='Exception',logic='NoData',key='arg2',value=2,symbol=NewSymbol),
                SymbolValidity(checkpoint='Exception',logic='NoData',key='arg3',value=3,symbol=NewSymbol),
                SymbolValidity(checkpoint='Exception',logic='NoFeed',key='arg1',value=10,symbol=NewSymbol),
                SymbolValidity(checkpoint='Exception',logic='NoFeed',key='arg2',value=20,symbol=NewSymbol)]
    
        for v in vals:
            session.add(v)
        
        for f in range(4):
            un = str(x) + dt.now().strftime("%H%M%S")
            time.sleep(0.25)
            NewFeed = Feed(NewSymbol,"DB",sourcing={'db' : 'General', 'user' : 'TODO'})
    
            vals = [FeedValidity(checkpoint='Exception',logic='NoData',key='arg1',value=1,feed=NewFeed),
                    FeedValidity(checkpoint='Exception',logic='NoData',key='arg2',value=2,feed=NewFeed),
                    FeedValidity(checkpoint='Check',logic='NoData',key='arg3',value=3,feed=NewFeed),
                    FeedValidity(checkpoint='Check',logic='NoFeed',key='arg1',value=100,feed=NewFeed),
                    FeedValidity(checkpoint='Check',logic='NoFeed',key='arg2',value='jeff',feed=NewFeed)]
        
            for v in vals:
                session.add(v)
    
            session.add(NewFeed)
        
        NewSymbol.InitializeDataTable()
        
        session.commit()
            
    
    session.commit()
    
    
    aSymbol = session.query(Symbol).all()[0]