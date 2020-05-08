import zmq
import json
import math

def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")

def getPoint(Point):
    
    FileToOpen =     "DataSet/"+str(Point)+".txt"
    
    f = open(FileToOpen, "r")
    Iduser = f.readline()
    Datos = f.readline().split("|")
    
    Point = {}
    
    for Dato in Datos:
        datosTemp = Dato.split(",")
        Point[datosTemp[0]] = datosTemp[1]
    
    
    f.close()
    return Point
            

def CalculateDist(dataSetI,dataSetII):
    
    sum_AB = 0
    sum_powA = 0 
    sum_powB = 0 
            
    for k in dataSetI.keys():
    		sum_AB += int(dataSetI[k])*int(dataSetII.get(k,0))
    		sum_powA += int(dataSetI[k])**2
    
    for k in dataSetII.keys():
    	sum_powB += int(dataSetII[k])**2
        
    if sum_powA == 0 or sum_powB == 0:
        print(dataSetI)
        print(dataSetII)
    
    return 1 - (sum_AB / (math.sqrt(sum_powA)*math.sqrt(sum_powB)))
  
    

def CalculateCentroide(Point,Centroids,dicc,tipo):
    
    DistMenor = 0
    CentroID = 0
   
    dataSetI = getPoint(Point)
    
    for i in range(len(Centroids)):
        if i == 0 :
             DistMenor = CalculateDist(Centroids[i],dataSetI)
        else:
             DistTemp = CalculateDist(Centroids[i],dataSetI)
             if DistTemp < DistMenor :
                 CentroID = i
                 DistMenor = DistTemp
                 
    if tipo == 0:
        
        dicc[CentroID]["Cant"] = dicc[CentroID]["Cant"] + 1
        dicc[CentroID]["Sumatoria"] = SumDataSet(dicc[CentroID]["Sumatoria"],dataSetI)
         
    else:
        
        temp = dicc[CentroID]
        temp.append(Point)
        dicc[CentroID] = temp
        
        
def SumDataSet(Vect1,Vect2):
    
    VecTSum = {}
    
    for k in Vect1.keys():
        if k in VecTSum:
            VecTSum[k] = int(VecTSum[k]) + int(Vect1[k])
        else:
            VecTSum[k] = int(Vect1[k])
    
    for j in Vect2.keys():
        if j in VecTSum:
            VecTSum[j] = int(VecTSum[j]) + int(Vect2[j])
        else:
            VecTSum[j] = int(Vect2[j])

    return VecTSum

def Main():

    context = zmq.Context()
    
    work = context.socket(zmq.PULL)
    work.connect("tcp://localhost:5557")
    
    # Socket to send messages to
    sink = context.socket(zmq.PUSH)
    sink.connect("tcp://localhost:5559")
    
    # Process tasks forever
    while True:
        s = work.recv_multipart()
        
        Init = Bdecode(s[0])
        Fin = Bdecode(s[1])
        Centrois = Bdecode(s[2])
        WorkId = Bdecode(s[3])
        tipo = Bdecode(s[4])
        
        print("Procesing work : "+WorkId)
        
        Centrois = json.loads(Centrois)
        
        dicc = {}

        if int(tipo) == 0 :
    
            for j in range(len(Centrois)):
                dicc[j] = {}
                dicc[j]["Sumatoria"] = {}
                dicc[j]["Cant"] = 0
                 
        else :
            
            for j in range(len(Centrois)):
                dicc[j] = []
            
        
        for k in range(int(Fin)-int(Init)+1):
           CalculateCentroide(k+int(Init),Centrois,dicc,int(tipo))
            

        # Send results to sink
        sink.send_multipart([Strencode(json.dumps(dicc))])

Main()