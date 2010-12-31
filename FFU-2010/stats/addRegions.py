#!/usr/bin/env python
from __future__ import division
from pylab import *
from pg import DB
from myconns import evo
import math
import numpy as np
import couchdb
from couchdb.client import Server
from couchdb.schema import Schema, Document, TextField, IntegerField, DateTimeField, ListField, FloatField, DictField

CELL_IDS = range(64801) #64800
pconn = DB(**evo.colorado.edu())

def Main(sn):
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
        
    couch = couchdb.Server()
    db = couch[sn]
    #select all from sn by cellid
    #for i in CELL_IDS:
    #results = db.query(map_all)
    ct = 0
    for row in db.view('_all_docs'): #,wrapper=Cell):
        #for cell in db.query(map_all,cellid=50471):
        #cell = Cell.load(db, Cell.cellid=50471)
        cell = Cell.load(db, row.id)
        
        regions = []
        q = "SELECT level1_id FROM cell_level1_map WHERE cell_id = %s" % (cell.cellid)
        cur = pconn.query(q)
        res = cur.getresult()
        for r in res:
            if int(r[0]) > -1:
                regions.append(int(r[0]))
        cell.level1 = regions
        cell.store(db)
        ct+=1
        if ct%1000 == 0:
            print 'SAVED %s' % ct
        
    

if __name__ == "__main__":  
    setname = "gbif_amphibia"
    Main(setname)
