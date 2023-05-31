import socket

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
ip=s.getsockname()[0]
print("IP Address:", ip)

WORKDIR='/home/cmsdaq/Workspace/TOFPET/Timing-TOFPET2/'

OUTPUTDIR={
   'BAR':'/media/cmsdaq/ext/data/LYSOBARS',
   'ARRAY':'/data/cmsdaq/LYSOMULTIARRAYNEWLAB4ARRAYS/'
}

WEBDIR={
   'BAR':'http://%s/BARS/index.php?match='%ip,
   'ARRAY':'http://%s/ARRAYS/index.php?match='%ip
}

processCommand='process_runs.py'

analysisCommand={
   'BAR':'analysis/analyze_run_bar.py',
   'ARRAY':'analysis/analyze_run_array_with_barRef.py',
}

summaryCommand={
   'BAR':'',
   'ARRAY':'analysis/launch_analyze_run_array.py',
}
