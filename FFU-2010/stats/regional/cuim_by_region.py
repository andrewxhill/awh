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
                regions[int(level1)] = {'cuims': [], 'recs': []}
            recs = 0
            cuims = 0
            for rec in row.records:
                if rec.resourceid not in probResources and rec.year != 1997 and 1900 < rec.year:
                    try:
                        assert rec.cuim > 0.0
                        #assert rec.cuim < 200000
                        recs += 1
                        regions[int(level1)]['cuims'].append(rec.cuim)
                        #cuims += rec.cuim
                    except:
                        pass
            
            #cuims = cuims/recs if cuims > 0 else 0
            #regions[int(level1)]['cuims'].append(cuims)
            #regions[int(level1)]['recs'].append(recs)
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
    symbs = ['s','o','^','d','h','+','v']
    colors = ['b','r','g','y','k','m','c']
    data = []
    ordered = {}
    f = open('xrecords_cuim_region.log',"w+")
    for i,r in regions.items():
        if i < 9:
            mn = np.array(r['cuims']).mean()
            ordered[mn] = i
            print i
    ok = ordered.keys()
    ok.sort()
    ticks = []
    for m in ok:
        i = ordered[m]
        f.write('region: %s\nmean: %srecords: %s\n' % (labels[i],m,len(regions[i]['cuims'])))
        data.append(regions[i]['cuims'])
        ticks.append(labels[i])
    
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.28,left=0.08)
    
    ax1 = fig.add_subplot(111)
    ax1.boxplot(data, positions=range(1,len(data)+1))
    ax1.xaxis.set_ticklabels(ticks, rotation=45)
    ax1.set_yscale('log')
    """
    ct = 0
    for label in ax1.xaxis.get_ticklabels():
        ax1.text(ct+1, 40-(40*0.88), int(ok[ct]),
            horizontalalignment='center', size='x-small')
        ct+=1
    """
    
    
    
    ax1.set_ylabel('coordinate undertainty (log)')
    plt.title("Distribution of coordinate uncertainty per TDWG Level 1 region")
    plt.savefig('xrecords_cuim_region.png')
    f.close()
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
    
