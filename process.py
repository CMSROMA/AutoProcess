import os
import threading
import time
import sys
import datetime
from airtable import Airtable

import logging
logging.basicConfig(format='%(asctime)s  %(filename)s  %(levelname)s: %(message)s',
                    level=logging.INFO)

base_key = 'appQ2YoOIQFBKKIpG'
tables = ['RUNS']

airtables={}
for t in tables:
    airtables[t] = Airtable(base_key, t, api_key=os.environ['AIRTABLE_KEY'])

class processThread (threading.Thread):
   def __init__(self, threadID, name, runid, runtype):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name= name
      self.runid = runid
      self.runtype = runtype.lower()
   def run(self):
      logging.info("Starting RAW2ROOT of " + self.runid)
      fields = {'Processing status': 'RAW2ROOT STARTED'}
      airtables['RUNS'].update(self.name, fields)
      #Launch processing on pccmsdaq02
      os.system('ssh pccmsdaq02 "cd /home/cmsdaq/work/LYBenchProcessing;  ./processData.sh -t %s -r %s" > /dev/null 2>&1'%(self.runtype,self.runid))
      logging.info("RAW2ROOT completed for " + self.runid)
      fields = {'Processing status': 'RAW2ROOT COMPLETED'}
      airtables['RUNS'].update(self.name, fields)
      del processingRuns[self.name]

class analysisThread (threading.Thread):
   def __init__(self, threadID, name, runid, runtype, ledref=None,ledrefid=None):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name= name
      self.runid = runid
      self.runtype = runtype.lower()
      self.ledref = ledref
      self.ledrefid = ledrefid
   def run(self):
      if (self.runtype=='ped'):
          return
      if (self.runtype=='source' and (self.ledref==None or self.ledrefid==None)):
          return
      logging.info("Started analysis of " + self.runid)
      fields = {'Processing status': 'PROCESSING STARTED'}
      if (self.runtype=='source'):
          fields.update({'LED RUNS': [self.ledrefid]})
      airtables['RUNS'].update(self.name, fields)
      #Launch analysis
      if (self.runtype=='led'):
          os.system('cd /home/cmsdaq/Analysis/LYBenchMacros; ./runSinglePEFit.sh -i %s -l -s -w > logs/singlePEFit_%s.log 2>&1'%(self.runid,self.runid))
      elif (self.runtype=='source'):
          os.system('cd /home/cmsdaq/Analysis/LYBenchMacros; ./runSourceAnalysis.sh -i %s -p %s -w > logs/sourceAnalysis_%s.log 2>&1'%(self.runid,self.ledref,self.runid))
      logging.info("Analysis completed for " + self.runid)
      fields = {'Processing status': 'PROCESSING COMPLETED','Results':'http://10.0.0.44/process/%s/'%self.runid}
      airtables['RUNS'].update(self.name, fields)
      del analysisRuns[self.name]

processingRuns={}
analysisRuns={}
counterThreads=1

while True:
    try:
        #Find the last good LED-SCAN run for today
        today=datetime.datetime.now().date()
        lastValidatedRuns=airtables['RUNS'].search('Processing status','VALIDATED')
        todayLedRuns={}
        for r in lastValidatedRuns:
            if r['fields']['Type']!='LED':
                continue
            if not 'SCAN' in r['fields']['RunID']:
                continue
            if datetime.datetime.strptime(r['fields']['Last modified'],'%Y-%m-%dT%H:%M:%S.%fZ').date() != today:
                continue
            todayLedRuns[r['fields']['RunID']]=r['id']
        if (len(todayLedRuns)>0):
            lastLed=sorted(todayLedRuns.keys(),reverse=True)[0]

        #Discover if there is any run to be processed
        runsToProcess=airtables['RUNS'].search('Processing status','DAQ COMPLETED')
        for r in runsToProcess:
            if not r['id'] in processingRuns:
                #print(r)
                processingRuns[r['id']]=processThread(counterThreads,r['id'],r['fields']['RunID'],r['fields']['Type'])
                processingRuns[r['id']].start()
                counterThreads+=1

        #Discover if there is any run to be analysed
        runsToAnalyze=airtables['RUNS'].search('Processing status','RAW2ROOT COMPLETED')
        for r in runsToAnalyze:
            if not r['id'] in analysisRuns:
                if (r['fields']['Type']=='PED'):
                    continue
                elif (r['fields']['Type']=='LED'):
                    analysisRuns[r['id']]=analysisThread(counterThreads,r['id'],r['fields']['RunID'],r['fields']['Type'])
                    analysisRuns[r['id']].start()
                    counterThreads+=1
                elif (r['fields']['Type']=='SOURCE'):
                    if (len(todayLedRuns)==0):
                        continue
                    analysisRuns[r['id']]=analysisThread(counterThreads,r['id'],r['fields']['RunID'],r['fields']['Type'],lastLed,todayLedRuns[lastLed])
                    analysisRuns[r['id']].start()
                    counterThreads+=1

        #refresh every 10s
        time.sleep(10)

    except KeyboardInterrupt:

        logging.info("Bye")
        sys.exit()
