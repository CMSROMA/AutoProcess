import os
from airtable import Airtable

#base_key = 'appi6WW159OZerPg2'
#from 21/06/2021
base_key = 'appqU3LwkozsBCCJW'
#from 25/01/2022
base_key = 'appTtgSyFU1PmkFEU'

tables = ['RUNS','Crystals']

runStatuses = [
    'DAQ STARTED',
    'DAQ COMPLETED',
    'RAW2ROOT STARTED',
    'RAW2ROOT COMPLETED',
    'PROCESSING STARTED',
    'PROCESSING COMPLETED',
    'VALIDATED'
]

runTypes = ['PHYS','PED'] 

airtables={}
for t in tables:
    airtables[t] = Airtable(base_key, t, api_key=os.environ['AIRTABLE_KEY'])

