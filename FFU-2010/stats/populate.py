#!/usr/bin/env python
from __future__ import division
from pylab import *
from pg import DB
import math, time, random
import numpy as np
from ruffus import *
from myconns import evo
import couchdb
from couchdb.client import Server
from couchdb.schema import Schema, Document, TextField, IntegerField, DateTimeField, ListField, FloatField, DictField

#CELL_IDS = range(4000) #64800
SETNAME = "gbif_mammals"
DELETE = True

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

def Main(sn):
    pconn = DB(**evo.colorado.edu())
    
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
        level4 = ListField(IntegerField())
    cellid = -1
    qlimit = 100000
    qoffset = 0
    qlast = 100000
    
    while qlast == 100000:
        qlast = 0
        print "RUNNING SELECT: %s, %s" % (cellid,qoffset)
        q = "SELECT occurrence_id,binomial, latitude, longitude, coordinate_precision, geospatial_issue, date_part('year',date_collected),cell_id,data_provider_id,data_resource_id,data_provider FROM %s WHERE binomial != '' and binomial is not null and cell_id > -1 and latitude is not null and basis_of_record != 'fossil' ORDER BY cell_id,binomial ASC LIMIT %s OFFSET %s" % (sn,qlimit,qoffset)
        qoffset += 100000
        for r in pconn.query(q).getresult():
            qlast += 1
            if int(r[7]) != cellid:
                if cellid != -1:
                    print "STORING CELL: %s" % cellid
                    #create a Cell()
                    #q2 = "SELECT count(*) FROM %s WHERE binomial != '' and binomial is not null and cell_id = %s and (latitude is null or longitude is null) and basis_of_record != 'fossil'" % (sn,cellid)
                    #row.nogeog = int(pconn.query(q2).getresult()[0][0])
                    row.records=records
                    row.issues=issues
                    row.taxaunique=uniques
                    row.cuimmean=cuimmean
                    row.cuimstd=cuimstd
                    row.cuimct=cuimct
                    row.level1 = regions1
                    row.level4 = regions4
                    row.store(db)
                cellid = int(r[7])
                row = Cell(cellid=cellid)
                cuimmean = 0
                cuimstd = 0
                cuimtot = 0
                cuimct = 0
                records = []
                issues = []
                uniques = []
                cuims = []
                last = ""
                regions1 = []
                regions4 = []
                print 'GETTING REGIONS: %s' % cellid
                q1 = "SELECT level1_id FROM cell_level1_map WHERE cell_id = %s" % (cellid)
                cur1 = pconn.query(q1)
                for i in cur1.getresult():
                    if int(i[0]) > -1:
                        regions1.append(int(i[0]))
                q1 = "SELECT level4_id FROM cell_level4_map WHERE cell_id = %s" % (cellid)
                cur1 = pconn.query(q1)
                for i in cur1.getresult():
                    if int(i[0]) > -1:
                        regions4.append(int(i[0]))
                
            #measure mean
            cuim = None
            if r[4] != '':
                try:
                    cuim = float(r[4])
                    cuimct+=1.0
                    cuimtot += float(r[4])
                    cuims.append(float(r[4]))
                except:
                    pass
            if float(r[5])==0 and r[2] is not None and r[3] is not None :
                records.append(
                    {'recid':int(r[0]),
                     'binomial': r[1],
                     'lat': float(r[2]),
                     'lon': float(r[3]),
                     'cuim': cuim,
                     'providerid': r[8],
                     'resourceid': r[9],
                     'provider': r[10],
                     'year': int(r[6]) if r[6] is not None else None
                    })
            else:
                issues.append(
                    {'recid':int(r[0]),
                     'binomial': r[1],
                     'type': int(r[5]),
                     'cuim': cuim,
                     'providerid': r[8],
                     'resourceid': r[9],
                     'provider': r[10],
                     'year': int(r[6]) if r[6] is not None else None
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
            
    print "STORING CELL: %s" % cellid
    #create a Cell()
    row.records=records
    row.issues=issues
    row.taxaunique=uniques
    row.cuimmean=cuimmean
    row.cuimstd=cuimstd
    row.cuimct=cuimct
    row.level1 = regions1
    row.level4 = regions4
    row.store(db)
        
    """
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
    """




if __name__ == "__main__":  
    Main(SETNAME)
