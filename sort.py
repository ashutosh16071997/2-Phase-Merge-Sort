import csv
import sys
import os
import math
from collections import OrderedDict
from itertools import islice
import heapq
import time
data = []
order = ""
heap = []
argumentlist = []
chunkno = 0
def sorting(argumentlist,order):
    start = time.time()
    print("##################################START EXECUTION#########################################")
    for values in argumentlist:
        if(values == "asec" or values == "ASEC" or values == "ascending" or values == "ASCENDING" or values == "asc" or values == "ASC"):
            order = "asec"
        if(values == "desc" or values == "DESC" or values == "descending" or values == "DESCENDING" or values == "DSC" or values == "dsc"):
            order = "desc"
    #print(order)
    colsize = 0
    columns = []
    columnssize = []
    f = open('metadata.txt','r')
    for line in f:
        line.replace(" ","")
        columns.append(line.split(',')[0])
        columnssize.append(int(line.split(',')[1]))
    #print(columns)
    #print(columnssize) 
    no_of_tuples = 0  
    inputfile = argumentlist[0]
    f = open(inputfile,'r')
    for line in f:
        no_of_tuples+=1
    #print(no_of_tuples)    
    tot = 0
    for values in columnssize:
        tot+=values
    tot = tot + len(columns)*2-1
    filesize = no_of_tuples*tot
    mainmemory = int(argumentlist[2])
    mainmemory = mainmemory*1024*1024 
    print("filesize:", filesize)       
    recordno = math.floor(mainmemory/tot)
    noofsplits = math.ceil(no_of_tuples/recordno)
    print("no of records in a file", recordno)
    print("no of splits", noofsplits)
    if(tot*noofsplits>((mainmemory))):
        print("Less avaialble memory")
        sys.exit(0)
    splittingfiles(inputfile,recordno,noofsplits,order,argumentlist,columnssize,columns)
    mergefiles(argumentlist,columnssize,columns,order,noofsplits)
    endtime = time.time()-start
    print(endtime)
    print("#########################EXECUTION TERMINATED#######################")
def writelast(intermediatefile,values,argumentlist,order,columns):
    fn = []
    fn = (argumentlist[4:])
    cn = []
    #print(fn)
    for val in fn:
       cn.append(columns.index(val))
    ff = lambda x:[x[i] for i in cn]   
    if order == "asec":
        print("Sorting files")
        values.sort(key = ff)
    elif order == "desc":
        values.sort(key = ff,reverse = True)
    #print(intermediatefile)    
    f11 = open(str(intermediatefile)+".txt",'w+')
    print("Writing tuple in indermediate file number",intermediatefile)
    for z in values:
        f11.write("  ".join(z))
        f11.write("\n")
    values.clear()
    f11.close()    
def openfiles(values,noofsplits):
    #print("openfiles")
    for i in range(noofsplits):
          ftt = open(str(i)+".txt")
          values.append(ftt)
    #print("wapas")      
    return values 
def openfiles1(values,columnssize,list1):
    #print("openfiles1")
    for i in range(len(values)):
        line = values[i].readline()
        if not line:
            break
        y = []
        tot = 0
        j = 0
        while(j<len(columnssize)):
            tat = columnssize[j]
            y.append(line[tot:tot+tat])
            tot = tat+2+tot
            j = j+1
        list1.append([y,i])
    #print("wapas")    
    return list1
def writefiles(list1,columns,order,argumentlist,values,columnssize):
    fn = []
    fn = argumentlist[4:]
    cn = []
    #print(fn)
    #print(columns)
    for val in fn:
        cn.append(columns.index(val))
    outputfile = argumentlist[1]
    f11 = open(outputfile,'w+')
    f11 = open(outputfile,'a+')
    print("WRITING IN OUTPUT LIFE")
    while True:
        if(len(list1) == 0):
            f11.close()
            #print("cll")
            break
        ff = lambda x:[x[0][i] for i in cn]
        if (order == "asec"):
            f11.write("  ".join(min(list1,key=ff)[0]))
            f11.write("\n")
            indices = min(list1,key=ff)[1]
            list1.remove(min(list1,key=ff))
        elif order=="desc":
            f11.write(" ".join(max(list1,key=ff)[0]))
            f11.write("\n")
            indices=max(list1,key=ff)[1]
            list1.remove(max(list1,key=ff)) 
        line2=values[indices].readline()
        if not line2:
           continue
        y=[]
        m = 0
        for i in columnssize:
            y.append(line2[m:m+i])
            m=m+2+i
        list1.append([y,indices])
    f11.close()

                   
def mergefiles(argumentlist,columnssize,columns,order,noofsplits):
    values = []
    values = openfiles(values,noofsplits)
    list1 = []
    list1 = openfiles1(values,columnssize,list1)
    #print(list1)
    outputfile = argumentlist[1]
    print("##############Writing files##############")
    writefiles(list1,columns,order,argumentlist,values,columnssize)
    print("#####################EXECUTION COMPLETED##############")
    
def splittingfiles(inputfile,recordno,noofsplits,order,argumentlist,columnsize,columns):
    print("############SPLITING INITIATED#############")
    #print(columns)
    inputfile = argumentlist[0]
    f = open(inputfile,'r')
    intermediatefile = 0
    record = 0
    values = []
    for line in f:
        j = []
        tot = 0
        for val in columnsize:
            j.append(line[tot:tot+val])
            tot = tot + val + 2
        values.append(j)
        record = record+1
        if(record == recordno):
            fn = []
            fn = (argumentlist[4:])
            cn = []
            for val in fn:
               cn.append(columns.index(val))
            ff = lambda x:[x[i] for i in cn]
            if order == "asec":
                print("Sorting files")
                values.sort(key = ff)
            elif order == "desc":
                print("Sorting files")
                values.sort(key = ff,reverse = True)
            f11 = open(str(intermediatefile)+".txt",'w+')
            print("writing intermediate file in",intermediatefile)
            for zz in values:
               f11.write("  ".join(zz))
               f11.write("\n")
            f11.close()    
            intermediatefile = intermediatefile+1
            values.clear()
            record = 0            

    if(len(values) != 0):
        #print(record)
        writelast(intermediatefile,values,argumentlist,order,columns)
    f.close()
if __name__ == '__main__':
  arguments = sys.argv[1:]
  argumentlist = arguments
  #print(argumentlist)
  sorting(argumentlist,order)