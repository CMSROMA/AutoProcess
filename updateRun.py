import os
from pprint import pprint
from airtable import Airtable
import pandas as pd

base_key = 'appQ2YoOIQFBKKIpG'
tables = ['RUNS','Crystals']

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--id',dest='runid')
parser.add_argument('--status',dest='runstatus')
parser.add_argument('--events',dest='runevents')
args = parser.parse_args()

runID=args.runid
runStatus=args.runstatus
runEvents=int(args.runevents)

runStatuses = [
    'DAQ STARTED',
    'DAQ COMPLETED',
    'RAW2ROOT STARTED',
    'RAW2ROOT COMPLETED',
    'PROCESSING STARTED',
    'PROCESSING COMPLETED',
    'VALIDATED'
]
runStatusOK=False

for t in runStatuses:
    if t==runStatus:
        runStatusOK=True
        break
if not runStatusOK:
    print('Unknown RunStatus %s'%runStatus)
    exit(-1)

airtables={}
for t in tables:
    airtables[t] = Airtable(base_key, t, api_key=os.environ['AIRTABLE_KEY'])
#print(airtable)

record=airtables['RUNS'].match('RunID',runID)
if (len(record)==0):
    print('Error: Cannot find %s'%runID)
    exit(-1)

fields = {'Processing status': runStatus, 'Events': runEvents}

airtables['RUNS'].update(record['id'], fields)
