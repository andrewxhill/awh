#!/usr/bin/env python
from __future__ import division
from pylab import *
from ruffus import *
from pg import DB
from myconns import evo
import math
import numpy as np

JOB_CELL_IDS = range(64801) #64800


params = [
            [JOB_CELL_IDS[0: int(math.floor(len(JOB_CELL_IDS)/4))]],
            [JOB_CELL_IDS[int(math.floor(len(JOB_CELL_IDS)/4)): int(math.floor(len(JOB_CELL_IDS)/2))]],
            [JOB_CELL_IDS[int(math.floor(len(JOB_CELL_IDS)/2)): len(JOB_CELL_IDS)-int(math.floor(len(JOB_CELL_IDS)/4))]],
            [JOB_CELL_IDS[len(JOB_CELL_IDS)-int(math.floor(len(JOB_CELL_IDS)/4)):]]
         ]
@parallel(params)
def Main(CELL_IDS):
    pconn = DB(**evo.colorado.edu())
    
    #select all from sn by cellid
    ins = []
    ct = 0
    lim = 50
    for i in CELL_IDS: 
        ct+=1
        #y = 90 -int(math.floor(i/360)) - 1
        y = -90+int(math.floor(i/360))
        x = i%360 - 180
        print 'SELECTING %s: %s %s' % (i, x, y)
        q = "SELECT * FROM tdwg_level4 WHERE ST_Intersects(the_geom, 'BOX(%s %s,%s %s)'::box2d );" % (x,y,x+1,y+1)
        cur = pconn.query(q)
        res = cur.getresult()
        marine = True
        for r in res:
            marine = False
            #ins = "INSERT INTO cell_id_map (cell_id,level1_id)VALUES(%s,%s)" % (i,r[0])
            ins.append({"cell_id": i,"level4_id": r[0]})
        
        if marine:
            ins.append({"cell_id": i,"level4_id": -1})
            #ins = "INSERT INTO cell_id_map (cell_id,level1_id)VALUES(%s,%s)" % (i,-1)
            #pconn.query(ins)
            
        if ct==lim:
            print 'INSERTING %s' % lim
            
            for s in ins:
                pconn.insert('cell_level4_map',s)
            
            ct = 0
            ins = []
            lim = 2* lim
            if lim > 5000:
                lim = 50
            
    
    print 'FINAL INSERT %s' % len(ins)
    for s in ins:
        pconn.insert('cell_level4_map',s)

if __name__ == "__main__":  
    #setname = "gbif_amphibia"
    pipeline_run([Main], multiprocess = 4)
    #Main()
