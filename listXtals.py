import os
from pprint import pprint
from airtable import Airtable
import pandas as pd

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--output',dest='output')

args = parser.parse_args()

try:
    f=open(args.output,'w')
except OSError:
    print('cannot open', args.output)
    exit(-1)

import runDB
records= runDB.airtables['Crystals'].get_all()
crystals=[ t['fields']['ID'] for t in records ]
crystals.sort(reverse=True)

for c in crystals:
#        print(cc['fields']['ID'])
        f.write(c+'\n')
f.close()
