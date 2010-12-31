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
    

def CuimXRegion(sn):
    regions = {} #region[1] = {cuim<deg = [], recs = []}
    couch = couchdb.Server()
    db = couch[sn]
    probResources = [8960,225,581,2465,2839,2688,8159]
    probYears = [1997]
    for key in db.view('_all_docs'):
        row = Cell.load(db, key.id)
        #row = Cell.load(db, uuid)
        cell = int(row.cellid)  
        rw = 18-int(math.floor(cell/3600))
        for level1 in row.level1:
            try:
                assert regions[int(level1)]
            except:
                regions[int(level1)] = {'issues': 0, 'records': 0}
            recs = 0
            cuims = 0
            for rec in row.records:
                if rec.resourceid not in probResources and rec.year != 1997 and 1900 < rec.year:
                    regions[int(level1)]['records'] += 1
            for rec in row.issues:
                if rec.resourceid not in probResources and rec.year != 1997 and 1900 < rec.year:
                    regions[int(level1)]['records'] += 1
                    regions[int(level1)]['issues'] += 1
    labels = {
        1:"Europe",
        2:"Africa",
        3:"Asia-Temperate",
        4:"Asia-Tropical",
        5:"Australasia",
        6:"Pacific",
        7:"Northern America",
        8:"Southern America",
        9:"Antarctic"}
        
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.28) #,left=0.08)
    ax1 = fig.add_subplot(111)
    ax2 = ax1.twinx()
    
    ordered, data, ticks, x, y = {}, {}, [], [], []
    ct = 1
    #totali = 0
    for a,b in regions.items():
        if a < 9:
            #totali += b['records']
            data[b['issues']] = a
            
    ordered = data.keys()
    ordered.sort()
    ct = 0
    for i in ordered:
        x.append(ct)
        ct+=1
        y.append(i)
        ticks.append(labels[data[i]])
    ax1.scatter(x,y,c='k',marker='o')
    #ax1.xaxis.set_ticklabels([])
    ax1.xaxis.set_ticks(x)
    ax1.xaxis.set_ticklabels(ticks, rotation=45)
    ax1.set_yscale('log')
    ax1.set_ylabel('Suspected geospatial issues (log)')
    
    y2 = []
    for i in ordered:
        a = data[i]
        prop = regions[a]['issues'] / regions[a]['records']
        print prop
        y2.append(prop)
        
    ax2.scatter(x,y2,c='k',marker='v') #,markersize=10)
    ax2.set_ylabel('Percent geospatial issues')
    
    #plt.xlim((0.9,len(x)+0.1))
    plt.title("Distribution of geospatial issues per TDWG Level 1 region")
    plt.savefig('xrecords_issues_region.png')
    #f.close()
    #plt.show()
"""
1;"Europe"
2;"Africa"
3;"Asia-Temperate"
4;"Asia-Tropical"
5;"Australasia"
6;"Pacific"
7;"Northern America"
8;"Southern America"
9;"Antarctic"
"""
    
if __name__ == "__main__":  
    setname = "gbif_amphibia"
    setname = "gbif_mammals"
    #ErrorXYear(setname)
    #CuimXYear(setname)
    CuimXRegion(setname)
    
