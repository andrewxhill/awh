#!/usr/bin/env python
from __future__ import division
import pylab
from scipy import stats
from rpy import r
import matplotlib.pyplot as plt
import png
import math
import numpy as np
import couchdb
from couchdb.client import Server
from couchdb.schema import Schema, Document, TextField, IntegerField, DateTimeField, ListField, FloatField, DictField

CELL_IDS = range(64801) #64800

class Cell(Document):
    cellid = IntegerField()
    records = ListField(DictField(Schema.build(
                recid = IntegerField(),
                binomial = TextField(),
                lat = FloatField(),
                lon = FloatField(),
                cuim = IntegerField(),
                year = IntegerField(),
                providerid = IntegerField(),
                resourceid = IntegerField(),
                dataprovider = TextField()
              )))
    issues = ListField(DictField(Schema.build(
                recid = IntegerField(),
                binomial = TextField(),
                type = IntegerField(),
                cuim = IntegerField(),
                year = IntegerField(),
                providerid = IntegerField(),
                resourceid = IntegerField(),
                dataprovider = TextField()
              )))
    cuidct = IntegerField()
    taxaunique = ListField(TextField())
    cuimmean = FloatField()
    cuimstd = FloatField()
    level1 = ListField(IntegerField())
    

def ErrorXYear(sn):
    issues = {}
    records = {}
    log_records = {}
    props = {}
    years = range(1900,2010) #[0::10]
    for i in years:
        props[i] = 0
        issues[i] = 0
        records[i] = 0
    
    couch = couchdb.Server()
    db = couch[sn]
    
    for key in db.view('_all_docs'):
        row = Cell.load(db, key.id)
        #row = Cell.load(db, uuid)
        cell = int(row.cellid)  
        rw = 18-int(math.floor(cell/3600))
        for rec in row.records:
            try:
                records[rec.year] += 1
            except:
                pass
        for rec in row.issues:
            try:
                issues[rec.year] += 1
            except:
                pass
    width = 0.35
    for a,b in issues.items():
        props[a] = b / (b + records[a]) if b > 0 else 0
    for a,b in records.items():
        log_records[a] = math.log(b+1)
    """
    p1 = plt.bar(years, records.values(),width, color='r')
    p2 = plt.bar(years, issues.values(),width, color='y',
             bottom=records.values())
    plt.ylabel('Record Count')
    plt.title('Records and Issues Per Year')
    plt.legend( (p1[0], p2[0]), ('Records', 'Suspected Issues') )
    """
    gradient, intercept, r_value, p_value, std_err = stats.linregress(years,props.values())
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.plot(years, log_records.values(), 'b-')
    ax1.set_ylabel('records', color='b')
    ax1.set_xlabel('year')
    for tl in ax1.get_yticklabels():
        tl.set_color('b')
    ax2 = ax1.twinx()
    ax2.plot(years, props.values(), 'r.')
    ax2.set_ylabel('proportion suspected issues', color='r')
    ax2.plot([min(years),max(years)],[(gradient*min(years)+intercept),(gradient*max(years)+intercept)], 'r--')
    for tl in ax2.get_yticklabels():
        tl.set_color('r')
    plt.xlim((min(years), max(years)))
    plt.savefig('errors_per_year.png')
    f = open('errors_per_year.log',"w+")
    f.write("gradient: %s\nintercept: %s\nr_value: %s\np_value: %s\nstd_err: %s" % (gradient, intercept, r_value, p_value, std_err))
    f.close()
    
    

def CuimXYear(sn):
    records = {}
    cuim = {}
    props = {}
    log_records = {}
    years = range(1900,2010) #[0::10]
    for i in years:
        props[i] = 0
        cuim[i] = 0
        records[i] = 0
    
    couch = couchdb.Server()
    db = couch[sn]
    for key in db.view('_all_docs'):
        row = Cell.load(db, key.id)
        #row = Cell.load(db, uuid)
        cell = int(row.cellid)  
        rw = 18-int(math.floor(cell/3600))
        for rec in row.records:
            try:
                records[rec.year] += 1
                try:
                    if rec.cuim > 0:
                        cuim[rec.year] += 1
                except:
                    pass
            except:
                pass
                
    width = 0.35
    for a,b in cuim.items():
        props[a] = b / (b + records[a]) if b > 0 else 0
    for a,b in records.items():
        log_records[a] = math.log(b+1)
        
    gradient, intercept, r_value, p_value, std_err = stats.linregress(years,props.values())
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.plot(years, log_records.values(), 'b-')
    ax1.set_ylabel('records', color='b')
    ax1.set_xlabel('year')
    for tl in ax1.get_yticklabels():
        tl.set_color('b')
    ax2 = ax1.twinx()
    ax2.plot(years, props.values(), 'r.')
    ax2.set_ylabel('proportion having cuim', color='r')
    ax2.plot([min(years),max(years)],[(gradient*min(years)+intercept),(gradient*max(years)+intercept)], 'r--')
    for tl in ax2.get_yticklabels():
        tl.set_color('r')
    plt.xlim((min(years), max(years)))
    plt.savefig('cuim_per_year.png')
    f = open('cuim_per_year.log',"w+")
    f.write("gradient: %s\nintercept: %s\nr_value: %s\np_value: %s\nstd_err: %s" % (gradient, intercept, r_value, p_value, std_err))
    f.close()
    
    

def YearData(sn):
    issues = {} #tracks the 'issue' count per year
    records = {} #tracks the total records count per year
    log_records = {} #later calculated from records() 
    iss_props = {} #later calculated from records() and issues()
    cuim = {} #the coordinate uncertainty count per year
    cuim_props = {} #later calculated from records() and cuim()
    cuim_mean = {}
    years = range(1900,2010) #[0::10]
    
    #problematic resources identified in errors_by_provider.py
    probs = [8960,225,581,2465,2839,2688,8159]
    
    for i in years:
        iss_props[i] = 0
        cuim_props[i] = 0
        cuim_mean[i] = 0
        issues[i] = 0
        records[i] = 0
        cuim[i] = 0
    
    couch = couchdb.Server()
    db = couch[sn]
    for key in db.view('_all_docs'):
        row = Cell.load(db, key.id)
        #row = Cell.load(db, uuid)
        cell = int(row.cellid)  
        for rec in row.records:
            if int(rec.resourceid) not in probs:
                try:
                    records[rec.year] += 1
                    try:
                        if rec.cuim > 0.0:
                            cuim[rec.year] += 1
                            cuim_mean[rec.year] += rec.cuim
                    except:
                        pass
                except:
                    pass
        for rec in row.issues:
            if int(rec.resourceid) not in probs:
                try:
                    issues[rec.year] += 1
                except:
                    pass
    width = 0.35
    for a,b in issues.items():
        prop = b / (b + records[a]) if b > 0 else 0
        iss_props[a] = prop
        if prop > 0.04:
            print a,prop,records[a]
        
    for a,b in cuim.items():
        cuim_props[a] = b / (b + records[a]) if b > 0 else 0
    for a,b in cuim_mean.items():
        cuim_mean[a] = math.log( 1 + b / records[a]) if b > 0 else 0
    for a,b in records.items():
        log_records[a] = math.log(b+1)
        
        
    f = open('by_year_summary.log',"w+")
    
    #add records chart
    fig = plt.figure()
    ax1 = fig.add_subplot(411)
    ax1.plot(years, log_records.values(), 'k-')
    #ax1.set_ylabel('total records', color='k')
    ax1.xaxis.set_ticklabels([])
    plt.xlim((min(years), max(years)))
    #plt.xticks([])
    
    #add issues chart
    gradient, intercept, r_value, p_value, std_err = stats.linregress(years,iss_props.values())
    ax2 = fig.add_subplot(412)
    ax2.plot(years, iss_props.values(), 'k.')
    #ax2.set_ylabel('proportion suspected issues', color='k')
    ax2.plot([min(years),max(years)],[(gradient*min(years)+intercept),(gradient*max(years)+intercept)], 'k--')
    #ax2.yaxis.set_label_position('right')
    ax2.yaxis.set_ticks_position('right')
    ax2.xaxis.set_ticklabels([])
    plt.xlim((min(years), max(years)))
    f.write("PROP ISSUES\ngradient: %s\nintercept: %s\nr_value: %s\np_value: %s\nstd_err: %s\n" % (gradient, intercept, r_value, p_value, std_err))
    
        
    gradient, intercept, r_value, p_value, std_err = stats.linregress(years,cuim_props.values())
    ax3 = fig.add_subplot(413)
    ax3.plot(years, cuim_props.values(), 'k.')
    #ax3.set_ylabel('proportion having cuim', color='k')
    ax3.plot([min(years),max(years)],[(gradient*min(years)+intercept),(gradient*max(years)+intercept)], 'k--')
    ax3.xaxis.set_ticklabels([])
    plt.xlim((min(years), max(years)))
    f.write("PROP CUIM\ngradient: %s\nintercept: %s\nr_value: %s\np_value: %s\nstd_err: %s\n" % (gradient, intercept, r_value, p_value, std_err))
    
        
    gradient, intercept, r_value, p_value, std_err = stats.linregress(years,cuim_mean.values())
    ax4 = fig.add_subplot(414)
    ax4.plot(years, cuim_mean.values(), 'k.')
    #ax4.set_ylabel('mean cuim (log)', color='k')
    ax4.plot([min(years),max(years)],[(gradient*min(years)+intercept),(gradient*max(years)+intercept)], 'k--')
    #ax4.yaxis.set_label_position('right')
    ax4.yaxis.set_ticks_position('right')
    plt.xlim((min(years), max(years)))
    f.write("LOG MEAN CUIM\ngradient: %s\nintercept: %s\nr_value: %s\np_value: %s\nstd_err: %s\n" % (gradient, intercept, r_value, p_value, std_err))
    
    ax4.set_xlabel('year')
    #plt.show()
    
    plt.savefig('by_year_summary.png')
    f.close()
    
    
if __name__ == "__main__":  
    setname = "gbif_mammals"
    #ErrorXYear(setname)
    #CuimXYear(setname)
    YearData(setname)
    
