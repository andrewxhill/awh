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
                year = IntegerField()
              )))
    issues = ListField(DictField(Schema.build(
                recid = IntegerField(),
                binomial = TextField(),
                type = IntegerField(),
                cuim = IntegerField(),
                year = IntegerField()
              )))
    cuidct = IntegerField()
    taxaunique = ListField(TextField())
    cuimmean = FloatField()
    cuimstd = FloatField()
    level1 = ListField(IntegerField())
    

def ErrorXLat(sn):
    latitudes = {}
    couch = couchdb.Server()
    db = couch[sn]
    for key in db.view('_all_docs'):
        row = Cell.load(db, key.id)
        #row = Cell.load(db, uuid)
        cell = int(row.cellid)  
        rw = 180-int(math.floor(cell/360))
        try:
            assert latitudes[rw]
        except:
            latitudes[rw] = []
        for rec in row.records:
            try:
                if rec.cuim > 0.0:
                    latidues[rw].append(rec.cuim)
        
        for reg in row.level1:
            
            if int(reg) > -1:
                try:
                    assert regionStats[reg]
                except:
                    regionStats[reg] = {'issues':0, 'good': 0, 'species':{}}
                regionStats[reg]['issues'] += len(row.issues)
                regionStats[reg]['good'] += len(row.records)
                for spec in row.records:
                    regionStats[reg]['species'][spec['binomial']] = 1
    x = []
    y = []
    labels = []
    for i,a in regionStats.items():
        if i != 9:
            specs = len(a['species'])
            if a['issues']==a['good']==0:
                print i,0, specs
            else:
                val = a['issues']/(a['issues']+a['good'])
                print i, val, specs
                x.append(math.log(specs))
                y.append(val)
                pylab.text(math.log(specs),val,'reg %s' %i,color="k")
                #labels.append('reg %s' %i)
    pylab.scatter(x,y, marker='o', c='b')
    pylab.show()
                
                
def Main(sn):
    latArray = {}
    cuimArray = {}
    cellsArray = {}
    recsArray = {}
    regionStats = {}
    couch = couchdb.Server()
    db = couch[sn]
    for key in db.view('_all_docs'):
        row = Cell.load(db, key.id)
        #row = Cell.load(db, uuid)
        cell = int(row.cellid)  
        rw = 18-int(math.floor(cell/3600))
        for reg in row.level1:
            
            if int(reg) > -1:
                try:
                    assert regionStats[reg]
                except:
                    regionStats[reg] = {'issues':0, 'good': 0, 'cuim': 0, 'species':{}}
                #regionStats[reg]['issues'] += len(row.issues)
                for spec in row.records:
                    regionStats[reg]['species'][spec['binomial']] = 1
                    if spec['cuim'] is not None:
                        regionStats[reg]['cuim'] += spec['cuim']
                        regionStats[reg]['good'] += 1
                
    x = []
    y = []
    labels = []
    for i,a in regionStats.items():
        if i != 9:
            specs = len(a['species'])
            if a['cuim']==a['good']==0:
                print i,0, specs
            else:
                val = a['cuim']/(a['good'])
                print i, val, specs
                x.append(math.log(specs))
                y.append(val)
                pylab.text(math.log(specs),val,'reg %s' %i,color="k")
                #labels.append('reg %s' %i)
    pylab.scatter(x,y, marker='o', c='b')
    pylab.show()
        
        
            
    """
        #Count unique cells/latitude
        try:
            cellsArray[rw] += 1
        except:
            cellsArray[rw] = 1
            
            
        #Count number of records/latitude
        try:
            recsArray[rw] += len(row.value[2])
        except:
            recsArray[rw] = len(row.value[2])
            
            
        #Measure species/latitude
        try:
            assert latArray[rw]
        except:
            latArray[rw] = {}
        for s in row.value[2]:
            try:
                latArray[rw][s['binomial']] += 1
            except:
                latArray[rw][s['binomial']] = 1
        
        
        #Measure coor-unc-in-met/latitude
        try:
            assert cuimArray[rw]
        except:
            cuimArray[rw] = []
        cuimArray[rw].append(row.value[0])
    """
    
        
    """
    for lat,cuimA in cuimArray.items():
        recs = cellsArray[lat]
        cuim = math.log(np.array(cuimA).mean())
        #cuim = math.sqrt(np.array(cuimA).mean())
        scatter(10*(9-lat),cuim,s=recs, marker='o', c='b')
    """
    
    """
    lats = []
    cuims = []
    specs = []
    recs = []
    cells = []
    
    
    for lat,cuim in cuimArray.items():
        lats.append(10*(9-lat))
        cuims.append(math.log(np.array(cuim).mean()))
        specs.append(len(latArray[lat]))
        recs.append(math.sqrt(recsArray[lat]))
        cells.append(cellsArray[lat])
    """
    
    
    """
    plt.scatter(cuims,specs)
    gradient, intercept, r_value, p_value, std_err = stats.linregress(cuims,specs)
    print "Gradient and intercept", gradient, intercept
    print "R-squared", r_value**2
    print "p-value", p_value
    plt.plot([min(cuims),max(cuims)], [min(cuims)*gradient,max(cuims)*gradient])
    
    tmpx = cuims
    tmpx.pop(-1)
    tmpx.pop(0)
    tmpy = specs
    tmpy.pop(-1)
    tmpy.pop(0)
    coefs = np.lib.polyfit(tmpx, tmpy, 1) #4
    fit_y = np.lib.polyval(coefs, tmpx) #5
    plt.plot(tmpx, fit_y, 'b-') #6
    """
    """
    
    fig = plt.figure()
    ax1 = fig.add_subplot(211)
    ax1.scatter(cuims, specs, s=cells, marker='o', c='b') #, usevlines=True, maxlags=9, normed=True, lw=2)
    ax1.grid(True)
    ax1.axhline(0, color='black', lw=2)

    ax2 = fig.add_subplot(212, sharex=ax1)
    ax2.scatter(lats, specs, s=cells, marker='o', c='r') #, usevlines=True, normed=True, maxlags=9, lw=2)
    ax2.grid(True)
    ax2.axhline(0, color='black', lw=2)

    plt.show()
    
    """

if __name__ == "__main__":  
    setname = "gbif_amphibia"
    Main(setname)
    
