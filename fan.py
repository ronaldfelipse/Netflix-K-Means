import zmq
import random
import math
import json

def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")


def Createcentroides(K,Dimensiones):
    
    Centroides = []
    
    for i in range(K):
        
         Centroidetemp = []
        
         for i in range(Dimensiones):

            Atribute = random.randrange(6)
            
         
            Centroidetemp.append(Atribute)
         Centroides.append(Centroidetemp)
        
    return Centroides

def SendsPoints(workers,NumPoints,Centroides):
    
    Pointer = 1
    TamPoints = 2 ####
    workCount = 1

    
    while Pointer < NumPoints:
        
        Inicio = Pointer
        Fin = Pointer+TamPoints-1
        workers.send_multipart([Strencode(Inicio),Strencode(Fin),Strencode(json.dumps(Centroides)),Strencode(workCount)])
        workCount = workCount +1
            
        Pointer = Pointer + TamPoints

def CalculateDist(Point1,Point2):
    
    Sum = 0
    
    for i in range(len(Point1)):
        Sum = Sum + ( Point1[i] - Point2[i] ) ** 2
        
    return math.sqrt(Sum)

def  Evaluatemovement(firstEstate,secondstate):
    
    ChangeValue = 5
    
    for i in range(len(firstEstate)):
        if CalculateDist(firstEstate[i],secondstate[i]) > ChangeValue:
            return True
    return False        
    

def Main():

    K = 1 ####
    MoviesCant = 17770
    Centroides = Createcentroides(K,MoviesCant)
        
    
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
    
    NumPoints = 12
    
    sinkSend.send_multipart([Strencode(NumPoints)])
    
    EndsKmeans = False
    
    while not EndsKmeans:
        
        sinkSend.send_multipart([Strencode(json.dumps(Centroides))])
        SendsPoints(workers,NumPoints,Centroides)    
        Newcentroids = sinkRecive.recv_multipart()
        Newcentroids = json.loads(Bdecode(Newcentroids[0]))
        
        if Evaluatemovement(Centroides,Newcentroids):
            Centroides = Newcentroids
        else:
            print("Final Centroides: ")
            print(Centroides)
            EndsKmeans = True
            
    
    

Main()


