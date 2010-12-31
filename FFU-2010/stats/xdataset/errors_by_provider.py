#!/usr/bin/env python
from __future__ import division
import pylab
from scipy import stats
from rpy import r
import matplotlib.pyplot as plt
import png
import math
import numpy as np
import operator
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
    

def ProviderIssues(sn):
    datasets = {} #region[1] = {cuim<deg = [], recs = []}
    providers, resources, labels = {}, {}, {} #region[1] = {cuim<deg = [], recs = []}
    rtotals, ptotals = {'issues': 0, 'records': 0}, {'issues': 0, 'records': 0}
    couch = couchdb.Server()
    db = couch[sn]
    for key in db.view('_all_docs'):
        row = Cell.load(db, key.id)
        #row = Cell.load(db, uuid)
        cell = int(row.cellid)  
        
        """
        for rec in row.records:
        for rec in row.issues:
        """
            
        for rec in row.records:
            #resource logs
            try:
                assert resources[rec.resourceid]
            except:
                resources[rec.resourceid] = {'issues': 0, 'records': 0}
            resources[rec.resourceid]['records']+=1
            rtotals['records']+=1
            #provider logs
            try:
                assert providers[rec.providerid]
            except:
                providers[rec.providerid] = {'issues': 0, 'records': 0}
            providers[rec.providerid]['records']+=1
            ptotals['records']+=1
            
        for rec in row.issues:
            #resource logs
            try:
                assert resources[rec.resourceid]
            except:
                resources[rec.resourceid] = {'issues': 0, 'records': 0}
            resources[rec.resourceid]['issues']+=1
            rtotals['issues']+=1
            resources[rec.resourceid]['records']+=1
            rtotals['records']+=1
            #provider logs
            try:
                assert providers[rec.providerid]
            except:
                providers[rec.providerid] = {'issues': 0, 'records': 0}
            providers[rec.providerid]['issues']+=1
            ptotals['issues']+=1
            providers[rec.providerid]['records']+=1
            ptotals['records']+=1
           
           
     
    f = open('errors_by_dataset_props.log',"w+")
    fig = plt.figure(figsize=(9,13))
    ax = fig.add_subplot(211)
    #ax2 = ax.twinx()
    x, y, props = [], [], []
    for a,b in providers.items():
        rec = math.log(b['records']) if b['records'] > 1 else 0
        iss = math.log(b['issues']) if b['issues'] > 1 else 0
        x.append(rec)
        #y.append(b['issues'])
        y.append(iss)
        prop = float(b['issues']) / float(b['records']) if b['records'] > 0 else 0
        props.append(prop)
    ax.scatter(x,props)
    ax.set_xlabel('Records (log)')
    ax.set_ylabel('Percent geospatial issues (log)')
    
    gradient, intercept, r_value, p_value, std_err = stats.linregress(x,props)
    ax.plot([min(x),max(x)],[(gradient*min(x)+intercept),(gradient*max(x)+intercept)], 'k--')
    #ax.yaxis.set_ticklabels([])
    ax.set_title('Providers')
    f.write("PROP PROVIDER ERRORS\ngradient: %s\nintercept: %s\nr_value: %s\np_value: %s\nstd_err: %s\ncount: %s\n" % (gradient, intercept, r_value, p_value, std_err, len(x)))
    
    ax3 = fig.add_subplot(212)
    #ax4 = ax3.twinx()
    x, y, props, bad = [], [], [], {}
    for a,b in resources.items():
        rec = math.log(b['records']) if b['records'] > 1 else 0
        iss = math.log(b['issues']) if b['issues'] > 1 else 0
        x.append(rec)
        #y.append(b['issues'])
        y.append(iss)
        prop = float(b['issues']) / float(b['records']) if b['records'] > 0 else 0
        if prop > 0.05:
            bad[a] = b['issues']
            print a,prop,b['records']
        props.append(prop)
    ax3.scatter(x,props)
    ax3.set_xlabel('Records (log)')
    ax3.set_ylabel('Percent geospatial issues (log)')
    #plt.ylim([0,max(y)])
    gradient, intercept, r_value, p_value, std_err = stats.linregress(x,props)
    f.write("\nPROP RESOURCE ERRORS\ngradient: %s\nintercept: %s\nr_value: %s\np_value: %s\nstd_err: %s\ncount: %s\n" % (gradient, intercept, r_value, p_value, std_err, len(x)))
    ax3.plot([min(x),max(x)],[(gradient*min(x)+intercept),(gradient*max(x)+intercept)], 'k--')
    #ax3.yaxis.set_ticklabels([])
    ax3.set_title('Resources')
    #ax4.set_title('Records versus suspected issues')
    f.write("PROBLEMATIC RESOURCES\n")
    for a,b in bad.items():
        f.write("%s: %s\n" % (a,b))
    f.write("TOTAL RESOURCE RECORDS: %s\n" % ptotals['records'])
    f.write("TOTAL RESOURCE ISSUES: %s\n" % rtotals['issues'])
    f.write("TOTAL PROVIDER RECORDS: %s\n" % ptotals['records'])
    f.write("TOTAL PROVIDER ISSUES: %s\n" % rtotals['issues'])
    #plt.title('Records versus suspected geospatial issues')
    plt.savefig('errors_by_dataset_props.png')
    
if __name__ == "__main__":  
    setname = "gbif_amphibia"
    setname = "gbif_mammals"
    ProviderIssues(setname)
    
