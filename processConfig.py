WORKDIR='/home/cmsdaq/Workspace/TOFPET/Timing-TOFPET/'

OUTPUTDIR={
   'BAR':'/media/cmsdaq/ext/data/LYSOBARS',
   'ARRAY':'/media/cmsdaq/ext/data/LYSOARRAYS'
}

WEBDIR={
   'BAR':'http://localhost/BARS/index.php?match=',
   'ARRAY':'http://localhost/ARRAYS/index.php?match='
}

processCommand='process_runs.py'

analysisCommand={
   'BAR':'analysis/analyze_run_bar.py',
   'ARRAY':'analysis/analyze_run_array.py',
}

summaryCommand={
   'BAR':'',
   'ARRAY':'analysis/launch_analyze_run_array.py',
}
