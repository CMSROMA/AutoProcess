import os
from pprint import pprint
from airtable import Airtable
import pandas as pd
import runDB

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--id',dest='runid')
parser.add_argument('--type',dest='runtype')
parser.add_argument('--xtal',dest='xtal')
parser.add_argument('--ov',dest='ov')
parser.add_argument('--posx',dest='posX')
parser.add_argument('--posy',dest='posY')
parser.add_argument('--tag',dest='runtag')
args = parser.parse_args()

runID=args.runid
runType=args.runtype
runXtal=args.xtal
runTag=args.runtag
runOV=float(args.ov)
runPosX=float(args.posX)
runPosY=float(args.posY)

runTypeOK=False

for t in runDB.runTypes:
    if t==runType:
        runTypeOK=True
        break
if not runTypeOK:
    print('Unknown RunType %s'%runType)
    exit(-1)

records=runDB.airtables['RUNS'].search('RunID',runID)
if (len(records)>0):
    print('Error: RUN already inserted')
    exit(-1)

if (runType=='PHYS'):
    records = runDB.airtables['Crystals'].search('ID', runXtal)
    if (len(records)!=1):
        print('Error in querying associated crystal')
        exit(-1)
    runDB.airtables['RUNS'].insert({'RunID': runID, 'Type':runType, 'Crystal':[records[0]['id']],'Processing status':'DAQ STARTED','TAG':runTag,'SetupID':2,'POSX':runPosX,'POSY':runPosY,'OV':runOV})
else:
    runDB.airtables['RUNS'].insert({'RunID': runID, 'Type':runType,'Processing status':'DAQ STARTED','TAG':runTag,'SetupID':2,'POSX':runPosX,'POSY':runPosY,'OV':runOV})
