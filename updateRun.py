import os
from pprint import pprint
from airtable import Airtable
import pandas as pd

import runDB

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--id',dest='runid')
parser.add_argument('--status',dest='runstatus')
parser.add_argument('--events',dest='runevents')
args = parser.parse_args()

runID=args.runid
runStatus=args.runstatus
if(args.runevents!=None):
    runEvents=int(args.runevents)

runStatusOK=False

for t in runDB.runStatuses:
    if t==runStatus:
        runStatusOK=True
        break
if not runStatusOK:
    print('Unknown RunStatus %s'%runStatus)
    exit(-1)


record=runDB.airtables['RUNS'].match('RunID',runID)
if (len(record)==0):
    print('Error: Cannot find %s'%runID)
    exit(-1)

fields={}
if (runStatus!=None):
    fields['Processing status']=runStatus
if (args.runevents!=None):
    fields['Events']=runEvents

runDB.airtables['RUNS'].update(record['id'], fields)
