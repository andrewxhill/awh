#!/usr/bin/env python
from __future__ import division
from pylab import *
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


def Main(sn):
    latArray = {}
    cuimArray = {}
    cellsArray = {}
    recsArray = {}
    couch = couchdb.Server()
    db = couch[sn]
    map_all = '''function(doc) {
            if (doc.cuimmean > 0.0)
                emit(doc.cellid, [doc.cuimmean, doc.cuimstd, doc.records, doc.taxaunique]);
        }'''
    """<Row id='489f57b77d3bbb84f4b45c0e97399d9a', 
        key=57808, 
        value=[35245, 
               0, 
               [
                {'lat': 70.109189999999998, 
                 'recid': 199455455, 
                 'binomial': 'rana temporaria', 
                 'lon': 28.84985, 
                 'cuim': 35245}, 
                ], 
                []]>"""
    for row in db.query(map_all):
        #row = Cell.load(db, uuid)
        cell = int(row.key)  
        rw = 18-int(math.floor(cell/3600))
        
        
        
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
    for lat,cuimA in cuimArray.items():
        recs = cellsArray[lat]
        cuim = math.log(np.array(cuimA).mean())
        #cuim = math.sqrt(np.array(cuimA).mean())
        scatter(10*(9-lat),cuim,s=recs, marker='o', c='b')
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
    
    

if __name__ == "__main__":  
    setname = "gbif_amphibia"
    Main(setname)
    
