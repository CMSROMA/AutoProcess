import os
from pprint import pprint
from airtable import Airtable
import pandas as pd

base_key = 'appQ2YoOIQFBKKIpG'
tables = ['RUNS','Crystals']

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--id',dest='runid')
parser.add_argument('--type',dest='runtype')
parser.add_argument('--xtal',dest='xtal')
parser.add_argument('--tag',dest='runtag')
args = parser.parse_args()

runID=args.runid
runType=args.runtype
runXtal=args.xtal
runTag=args.runtag

runTypes = ['SOURCE','PED','LED']
runTypeOK=False

for t in runTypes:
    if t==runType:
        runTypeOK=True
        break
if not runTypeOK:
    print('Unknown RunType %s'%runType)
    exit(-1)

airtables={}
for t in tables:
    airtables[t] = Airtable(base_key, t, api_key=os.environ['AIRTABLE_KEY'])
#print(airtable)

records=airtables['RUNS'].search('RunID',runID)
if (len(records)>0):
    print('Error: RUN already inserted')
    exit(-1)

if (runType=='SOURCE'):
    records = airtables['Crystals'].search('ID', runXtal)
    if (len(records)!=1):
        print('Error in querying associated crystal')
        exit(-1)
    airtables['RUNS'].insert({'RunID': runID, 'Type':runType, 'Crystal':[records[0]['id']],'Processing status':'DAQ STARTED','TAG':runTag,'SetupID':1})
else:
    airtables['RUNS'].insert({'RunID': runID, 'Type':runType,'Processing status':'DAQ STARTED','TAG':runTag,'SetupID':1})
