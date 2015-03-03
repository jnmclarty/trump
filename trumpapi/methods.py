# -*- coding: utf-8 -*-
import pandas as pd

def trmp_Priority(df):
    cols = df.columns
    def p(x):
        ret = pd.np.nan
        for c in cols[::-1]:
            if pd.notnull(x[c]):
                ret = x[c]
        return ret
    df['final'] = df.apply(p,axis=1,reduce=True)
    return df
    
def trmp_Mean(df):
    cols = df.columns
    def p(x):
        cnt = 0
        ret = 0.0
        for c in cols[::-1]:
            if pd.notnull(x[c]):
                cnt = cnt + 1
                ret = ret + x[c]
        if cnt != 0:
            return ret / cnt
        else:
            return pd.np.NaN
    df['final'] = df.apply(p,axis=1,reduce=True)
    return df

def trmp_Median(df):
    def p(x):
        return x.median()
    df['final'] = df.apply(p,axis=1,reduce=True)
    return df

TrumpMethods = {'Priority' : trmp_Priority, 'Mean' : trmp_Mean, 'Median' : trmp_Median}