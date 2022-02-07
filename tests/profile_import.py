import io
import numpy as np
#Run "python -X importtime -c 'import ploomber' 2> tempfile_for_profiling.log" before running this script
#This script reads the file created from that command and lists packages and cumulative time taken in us in descending order
temp_file_profile="tempfile_for_profiling.log"
outfile=open("profiling_diagnostics.log","w")
file_obj = io.open(temp_file_profile,'r', encoding='utf-16-le')
lines=file_obj.readlines()
file_obj.close()
cum_time=[]
self_time=[]
package=[]
for i,s in enumerate(lines):
    if(s.startswith("import")):
            line=s.strip().split()
            if(len(line)==7):
                package.append(line[-1])
                cum_time.append(int(line[-3]))
                self_time.append(int(line[-5]))

#Sorting cumulative time array
sort_index=np.argsort(cum_time)
#Writing output- cumulative time per package in descending order
outfile.write("Package\tCumulativetime[us]\n")
for i in range(1,len(sort_index)+1):
    outfile.write("%s\t%d\n"%(package[sort_index[-i]],cum_time[sort_index[-i]]))
outfile.close()
