#!/usr/bin/env python
from __future__ import division
from pylab import *
from pg import DB
import math
import numpy as np
import couchdb
from couchdb.client import Server
from couchdb.schema import Schema, Document, TextField, IntegerField, DateTimeField, ListField, FloatField, DictField

CELL_IDS = range(64801) #64800
pconn = DB('gbif_eorm','evo.colorado.edu',5432,user='postgres',passwd='tful282')

def IsUnique(sn, id, taxa):
    q = "SELECT count(*) FROM %s WHERE cell_id != %s AND geospatial_issue='0' AND binomial != '%s' and latitude != 0 and longitude != 0 and latitude is not null and basis_of_record != 'fossil'" % (sn,id,taxa)
    cur = pconn.query(q)
    res = cur.getresult()
    n = 0
    for r in res:
        n = int(r[0])
    if n == 0:
        return True
    else:
        return False

def Main(sn, DELETE = False):
    couch = couchdb.Server()
    if DELETE:
        try:
            couch.delete(sn)
        except:
            pass
    try:
        db = couch.create(sn)
    except:
        db = couch[sn]
    
    print 'Connected to database'
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
        
    #select all from sn by cellid
    for i in CELL_IDS:
        row = Cell(cellid=i)
        cuimmean = 0
        cuimstd = 0
        cuimtot = 0
        cuimct = 0
        records = []
        issues = []
        uniques = []
        cuims = []
        last = ""
        print "Running select on %s" % i
        q = "SELECT occurrence_id,binomial, latitude, longitude, coordinate_precision, geospatial_issue, date_part('year',date_collected) FROM %s WHERE cell_id = %s AND binomial != '' and binomial is not null and latitude != 0 and longitude != 0 and latitude is not null and basis_of_record != 'fossil' ORDER BY binomial" % (sn,i)
        cur = pconn.query(q)
        res = cur.getresult()
        #measure mean
        for r in res:
            cuim = None
            if r[4] != '':
                try:
                    cuim = float(r[4])
                    cuimct+=1.0
                    cuimtot += float(r[4])
                    cuims.append(float(r[4]))
                except:
                    pass
            if float(r[5])==0:
                records.append(
                    {'recid':int(r[0]),
                     'binomial': r[1],
                     'lat': float(r[2]),
                     'lon': float(r[3]),
                     'cuim': cuim,
                     'year': int(r[6])
                    })
            else:
                issues.append(
                    {'recid':int(r[0]),
                     'binomial': r[1],
                     'type': int(r[5]),
                     'cuim': cuim,
                     'year': int(r[6])
                    })
                    
        if cuimct != 0:
            cuimmean = cuimtot/cuimct
            std = 0
            for c in cuims:
                std += (c-cuimmean)**2
            cuimstd = sqrt(std/cuimct)
        else:
            cuimmean = None
            cuimstd = None
        
        print "Storing cell"
        #create a Cell()
        row.records=records
        row.issues=issues
        row.taxaunique=uniques
        row.cuimmean=cuimmean
        row.cuimstd=cuimstd
        row.cuimct=cuimct
        row.store(db)
        
    # read the data from the database
    print 'reading all rows from database'
    print len(db), 'rows retrieved from database'
    print 'name', 'age', 'added'
    for uuid in db:
       row = Cell.load(db, uuid)
       print row.cellid, row.cuimmean, row.cuimstd, row.records
       
       
    # query the database
    print 'querying the database'
    code = '''function(doc) { if (doc.cellid===1){ emit([doc.cellid], doc); }}'''
    results = db.query(code)
    print len(results), 'rows retrieved from database'
    print 'cellid', 'unique', 'cuimed'
    for res in results:
       print res
       




if __name__ == "__main__":  
    setname = "gbif_amphibia"
    Main(setname, DELETE=True)
