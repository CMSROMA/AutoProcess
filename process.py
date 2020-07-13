import os
import threading
import time
import sys
import datetime
from airtable import Airtable

import logging
logging.basicConfig(format='%(asctime)s  %(filename)s  %(levelname)s: %(message)s',
                    level=logging.INFO)

import processConfig

import runDB

class processThread (threading.Thread):
   def __init__(self, threadID, name, runid, runtype,tag):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name= name
      self.runid = runid
      self.runtype = runtype.lower()
      self.tag = tag
   def run(self):
      logging.info("Starting RAW2ROOT of " + self.runid)
      fields = {'Processing status': 'RAW2ROOT STARTED'}
      runDB.airtables['RUNS'].update(self.name, fields)
      #Launch processing on pccmsdaq02
      tags=self.tag.split('_')
      runNumber=int(tags[0].replace('Run',''))
      runType='BAR' if 'BAR' in tags[2] else 'ARRAY'
      command='cd %s; python %s -r %d-%d -d %s > %s/RESULTS/%s_raw2root.log 2>&1'%(processConfig.WORKDIR,processConfig.processCommand,runNumber,runNumber,processConfig.OUTPUTDIR[runType],processConfig.OUTPUTDIR[runType],self.tag)
      exitStatus=os.WEXITSTATUS(os.system(command))
#      os.WEXITSTATUS(os.system('cd %s; python process_runs.py -r %d-%d -d %s > /dev/null 2>&1'%(runNumber,runNumber,OUTPUTDIR[runType])))
      if (exitStatus==0):
          logging.info("RAW2ROOT completed for " + self.runid)
          fields = {'Processing status': 'RAW2ROOT COMPLETED'}
          runDB.airtables['RUNS'].update(self.name, fields)
      else:
          logging.info("RAW2ROOT failed for " + self.runid)
          fields = {'Processing status': 'FAILED'}
          runDB.airtables['RUNS'].update(self.name, fields)

      del processingRuns[self.name]

class analysisThread (threading.Thread):
   def __init__(self, threadID, name, runid, runtype, tag, pedRuns=None):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name= name
      self.runid = runid
      self.runtype = runtype.lower()
      self.tag = tag
      self.pedRuns = pedRuns
   def run(self):
      if (self.runtype=='ped'):
          return
      if (self.runtype=='phys' and len(self.pedRuns)!=2):
          return
      logging.info("Started analysis of " + self.runid)
      fields = {'Processing status': 'PROCESSING STARTED'}
      if (self.runtype=='phys'):
          fields.update({'PED RUNS': [ p for p in self.pedRuns.keys() ] })
      runDB.airtables['RUNS'].update(self.name, fields)
      tags=self.tag.split('_')
      runNumber=int(tags[0].replace('Run',''))
      runType='BAR' if 'BAR' in tags[2] else 'ARRAY'
      if (runType=='BAR'):
         partNumber=int(tags[2].replace('BAR',''))
         command='cd %s; python %s --run %d --barCode %d -i %s -o %s/RESULTS > %s/RESULTS/%s_analysis.log 2>&1'%(processConfig.WORKDIR,processConfig.analysisCommand[runType],runNumber,partNumber,processConfig.OUTPUTDIR[runType],processConfig.OUTPUTDIR[runType],processConfig.OUTPUTDIR[runType],self.tag)
      elif (runType=='ARRAY'):
         partNumber=int(tags[2].replace('ARRAY',''))
         command='cd %s; python %s --run %d --arrayCode %d -i %s -o %s/RESULTS > %s/RESULTS/%s_analysis.log 2>&1'%(processConfig.WORKDIR,processConfig.analysisCommand[runType],runNumber,partNumber,processConfig.OUTPUTDIR[runType],processConfig.OUTPUTDIR[runType],processConfig.OUTPUTDIR[runType],self.tag)

      #      print(command)
      #Launch analysis
      exitStatus=os.WEXITSTATUS(os.system(command))
      if (exitStatus==0):
          logging.info("Analysis completed for " + self.runid)
          fields = {'Processing status': 'PROCESSING COMPLETED','Results':'%s%s'%(processConfig.WEBDIR[runType],self.runid)}
          runDB.airtables['RUNS'].update(self.name, fields)
          for p in pedRuns.keys():
             runDB.airtables['RUNS'].update(p, fields)
      else:
          logging.info("Analysis failed for " + self.runid)
          fields = {'Processing status': 'FAILED'}
          runDB.airtables['RUNS'].update(self.name, fields)
          for p in pedRuns.keys():
             runDB.airtables['RUNS'].update(p, fields)

      del analysisRuns[self.name]


class summaryThread (threading.Thread):
   def __init__(self, threadID, name, runid, runtype, tag, arrayRuns=None):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name= name
      self.runid = runid
      self.runtype = runtype.lower()
      self.tag = tag
      self.arrayRuns = arrayRuns
   def run(self):
      if (self.runtype=='ped'):
          return
      if (self.runtype=='phys' and len(self.arrayRuns)!=4):
          return
      logging.info("Started summary of " + self.runid)
      fields = {'Processing status': 'SUMMARY STARTED'}
      runDB.airtables['RUNS'].update(self.name, fields)
      for r in self.arrayRuns.keys():
         runDB.airtables['RUNS'].update(r, fields)         
      tags=self.tag.split('_')
      runNumber=int(tags[0].replace('Run',''))
      runType='BAR' if 'BAR' in tags[2] else 'ARRAY'
      if (runType=='BAR'):
         return
      elif (runType=='ARRAY'):
         partNumber=int(tags[2].replace('ARRAY',''))
         command='cd %s; python %s --firstRun %d --arrayCode %d -i %s -o %s/RESULTS > %s/RESULTS/%s_summary.log 2>&1'%(processConfig.WORKDIR,processConfig.summaryCommand[runType],runNumber,partNumber,processConfig.OUTPUTDIR[runType],processConfig.OUTPUTDIR[runType],processConfig.OUTPUTDIR[runType],self.tag)

      #      print(command)
      #Launch summary
      exitStatus=os.WEXITSTATUS(os.system(command))
      if (exitStatus==0):
          logging.info("Summary completed for " + self.runid)
          fields = {'Processing status': 'SUMMARY COMPLETED'}
          runDB.airtables['RUNS'].update(self.name, fields)
          for r in self.arrayRuns.keys():
             runDB.airtables['RUNS'].update(r, fields)         
      else:
          logging.info("Analysis failed for " + self.runid)
          fields = {'Processing status': 'FAILED'}
          runDB.airtables['RUNS'].update(self.name, fields)
          for r in arrayRuns.keys():
             runDB.airtables['RUNS'].update(r, fields)

      del summaryRuns[self.name]

processingRuns={}
analysisRuns={}
summaryRuns={}
counterThreads=1

logging.info("Starting processing loop")


while True:
    try:
        #Discover if there is any run to be processed
        runsToProcess=runDB.airtables['RUNS'].search('Processing status','DAQ COMPLETED')
        for r in runsToProcess:
            if not r['id'] in processingRuns:
                #print(r)
                processingRuns[r['id']]=processThread(counterThreads,r['id'],r['fields']['RunID'],r['fields']['Type'],r['fields']['TAG'])
                processingRuns[r['id']].start()
                counterThreads+=1

        #Discover if there is any PHYS run to be analysed
        runsToAnalyze=runDB.airtables['RUNS'].search('Processing status','RAW2ROOT COMPLETED')
        for r in runsToAnalyze:
            if not r['id'] in analysisRuns:
               if (r['fields']['Type']=='PED'):
                   continue
               elif (r['fields']['Type']=='PHYS'):
                  runNumber=int(r['fields']['RunID'].replace('Run',''))
                  #check subsequent runs to be PED, to be processed and sufficient close in start time
                  runPedID=['Run%s'%str(runNumber-1).zfill(6),'Run%s'%str(runNumber+1).zfill(6)]
                  runTime=datetime.datetime.strptime(r['fields']['Created'],'%Y-%m-%dT%H:%M:%S.%fZ')
                  pedRuns={}
                  for p in runPedID:
                     for rv in runsToAnalyze:
                        if (rv['fields']['RunID']==p and rv['fields']['Type']=='PED'):
                           if abs((datetime.datetime.strptime(rv['fields']['Created'],'%Y-%m-%dT%H:%M:%S.%fZ')-runTime).total_seconds()<7200):
                              pedRuns[rv['id']]=p
                           break
                  if (len(pedRuns)!=2):
                     continue
                  analysisRuns[r['id']]=analysisThread(counterThreads,r['id'],r['fields']['RunID'],r['fields']['Type'],r['fields']['TAG'],pedRuns)
                  analysisRuns[r['id']].start()
                  counterThreads+=1

        runsToSummarize=runDB.airtables['RUNS'].search('Processing status','PROCESSING COMPLETED')
        for r in runsToSummarize:
            if not r['id'] in summaryRuns:
               if (r['fields']['Type']=='PED'):
                   continue
               elif (r['fields']['Type']=='PHYS'):
                  runNumber=int(r['fields']['RunID'].replace('Run',''))
                  tags=r['fields']['TAG'].split('_')
                  
                  #identify first run of array
                  if (not 'ARRAY' in tags[2]):
                     continue
                  if (not 'POS1' in tags[3]):
                     continue
                  logging.info(r['fields']['TAG'])
                  #check subsequent PHYS runs for other positions
                  runArrays=[ 'Run%s'%str(runNumber+i*3).zfill(6) for i in range(1,6) ]
                  runsArray={}
                  for i,p in enumerate(runArrays):
                     for rv in runsToSummarize:
                        if (rv['fields']['RunID']==p and rv['fields']['Type']=='PHYS'):
                           if (rv['fields']['Crystal'] != r['fields']['Crystal']):
                              continue
                           logging.info(rv['fields']['TAG'])
                           mytags=rv['fields']['TAG'].split('_')
                           if(mytags[3] != 'POS%d'%(i+2)):
                              continue
                           runsArray[rv['id']]=p
                           break
                  if (len(runsArray)!=4):
                     continue
                  summaryRuns[r['id']]=summaryThread(counterThreads,r['id'],r['fields']['RunID'],r['fields']['Type'],r['fields']['TAG'],runsArray)
                  summaryRuns[r['id']].start()
                  counterThreads+=1

    except KeyboardInterrupt:
        logging.info("Bye")
        sys.exit()

    except Exception as e:
        logging.error(e)

    try:
        #refresh every 10s
        time.sleep(10)
    except KeyboardInterrupt:
        logging.info("Bye")
        sys.exit()
