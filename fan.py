import zmq
import random
import math
import json

def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")

def generateCentroid(Dimensiones):
           
         Centroidetemp = {}
         
         NumTemp =  random.randint(1,100)
        
         for i in range(NumTemp):

            movieID = random.randint(1,Dimensiones)
            Atribute = random.randint(1,5)
            
            Centroidetemp[movieID] = Atribute
            
         return Centroidetemp

def Createcentroides(K,Dimensiones):
    
    Centroides = []
    
    for i in range(K):
        
         Centroides.append(generateCentroid(Dimensiones))
        
    return Centroides

def SendsPoints(workers,NumPoints,Centroides,tipo):
    
    Pointer = 1
    TamPoints = 500 ####
    workCount = 1

    
    while Pointer < NumPoints:
        
        Inicio = Pointer
        Fin = Pointer+TamPoints-1
        workers.send_multipart([Strencode(Inicio),Strencode(Fin),Strencode(json.dumps(Centroides)),Strencode(workCount),Strencode(tipo)])
        workCount = workCount +1
            
        Pointer = Pointer + TamPoints

def CalculateDist(dataSetI,dataSetII):
    
    sum_AB = 0
    sum_powA = 0 
    sum_powB = 0 
            
    for k in dataSetI.keys():
    		sum_AB += dataSetI[k]*dataSetII.get(k,0)
    		sum_powA += dataSetI[k]**2
    
    for k in dataSetII.keys():
    	sum_powB += dataSetII[k]**2
        
    if sum_powA == 0 or sum_powB == 0:
        print(dataSetI)
        print(dataSetII)
    
    return 1 - (sum_AB / (math.sqrt(sum_powA)*math.sqrt(sum_powB)))

def  Evaluatemovement(firstEstate,secondstate):
    
    ChangeValue = 0.01
    
    for i in range(len(firstEstate)):
        if CalculateDist(firstEstate[i],secondstate[i]) > ChangeValue:
            return True
    return False        
    

def Main():

    K = 2 ####
    MoviesCant = 17770
    Centroides = Createcentroides(K,MoviesCant)
    print(Centroides)    
    
    context = zmq.Context()
    
    # socket with workers
    workers = context.socket(zmq.PUSH)
    workers.bind("tcp://*:5557")
    
    # socket with sink
    sinkSend = context.socket(zmq.PUSH)
    sinkSend.connect("tcp://localhost:5558")
    
    sinkRecive = context.socket(zmq.PULL)
    sinkRecive.connect("tcp://localhost:5556")
    
    print("Press enter when workers are ready...")
    _ = input()
    print("sending tasks to workers")
    
    NumPoints =  470758
    
    sinkSend.send_multipart([Strencode(NumPoints)])
    
    EndsKmeans = False

    Count = 0
    
    while not EndsKmeans:
        
        sinkSend.send_multipart([b'0',Strencode(json.dumps(Centroides))])
        SendsPoints(workers,NumPoints,Centroides,0)    
        Newcentroids = sinkRecive.recv_multipart()
        Newcentroids = json.loads(Bdecode(Newcentroids[0]))
        
        for j in range(len(Newcentroids)):
                if len(Newcentroids[j]) == 0 :
                    Newcentroids[j] = generateCentroid()
        Count = Count  + 1
        
        if Evaluatemovement(Centroides,Newcentroids):
            Centroides = Newcentroids

	    if Count == 100:
		 
          	sinkSend.send_multipart([b'1',Strencode(json.dumps(Centroides))])
            	SendsPoints(workers,NumPoints,Centroides,1) 
            
            	print("K-MeansEnd")
            	EndsKmeans = True


        else:
            Centroides = Newcentroids
            sinkSend.send_multipart([b'1',Strencode(json.dumps(Centroides))])
            SendsPoints(workers,NumPoints,Centroides,1) 
            
            print("K-MeansEnd")
            EndsKmeans = True
            
    
    

Main()


