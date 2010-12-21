#!/usr/bin/env python
from __future__ import division
from pylab import *
import png
import math
import numpy as np
import couchdb
from couchdb.client import Server
from couchdb.schema import Schema, Document, TextField, IntegerField, DateTimeField, ListField, FloatField, DictField

CELL_IDS = range(64801) #64800


def Main(sn):
    imgArray = np.zeros((180,360,4), dtype=int)
    f = open("%s_%s.png" % (sn,sn), "w+")
    w = png.Writer(360,180, planes=4, greyscale=False, alpha=True, bitdepth=8)
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
               [{'lat': 70.109189999999998, 
                 'recid': 199455455, 
                 'binomial': 'rana temporaria', 
                 'lon': 28.84985, 
                 'cuim': 35245}, 
                 {'lat': 70.109189999999998, 
                  'recid': 199043592, 
                  'binomial': 'rana temporaria', 
                  'lon': 28.84985, 
                  'cuim': 35245}, 
                 {'lat': 70.109189999999998, 
                  'recid': 199043591, 
                  'binomial': 'rana temporaria', 
                  'lon': 28.84985, 'cuim': 35245}, 
                 {'lat': 70.109189999999998, 
                  'recid': 199455397, 
                  'binomial': 'rana temporaria', 
                  'lon': 28.84985, 
                  'cuim': 35245}
                ], 
                []]>"""
    MaxMeanCuim = 2101015.0
    StdCuim = 425773.62807
    MaxNumRecords = 30271.0
    MaxUniTaxa = 0
    for row in db.query(map_all):
        #row = Cell.load(db, uuid)
        cell = int(row.key)
        col = cell%360        
        rw = 179-int(math.floor(cell/360))
        #val = (255.0 * math.log(row.value[0]+1)/math.log(MaxMeanCuim+1))
        #val = (255.0 * math.log(row.value[1]+1)/math.log(StdCuim+1))
        val = (255.0 * math.log(len(row.value[2])+1)/math.log(MaxNumRecords+1))
        imgArray[rw][col][0] = int(val)
        imgArray[rw][col][2] = 255-int(val)
        imgArray[rw][col][3] = 188
        
    w.write(f,np.reshape(imgArray, (-1,360*4)))
    f.close()
    

if __name__ == "__main__":  
    setname = "gbif_amphibia"
    Main(setname)
    
