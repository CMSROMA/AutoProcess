import os
from pprint import pprint
from airtable import Airtable
import pandas as pd

base_key = 'appQ2YoOIQFBKKIpG'
tables = ['Crystals']

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--output',dest='output')

args = parser.parse_args()

try:
    f=open(args.output,'w')
except OSError:
    print('cannot open', args.output)
    exit(-1)

airtables={}
for t in tables:
    airtables[t] = Airtable(base_key, t, api_key=os.environ['AIRTABLE_KEY'])

crystals= airtables['Crystals'].get_iter()

for c in crystals:
    for cc in c:
#        print(cc['fields']['ID'])
        f.write(cc['fields']['ID']+'\n')
f.close()
