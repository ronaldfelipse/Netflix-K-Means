import zmq
import json

def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")

def SumVect(Vect1,Vect2):
    
    VecTSum = []
    
    for i in range(len(Vect1)):
        Val = Vect1[i] + Vect2[i]
        VecTSum.append(Val)
        
    return VecTSum

context = zmq.Context()

fanRecive = context.socket(zmq.PULL)
fanRecive.bind("tcp://*:5558")

fanSend = context.socket(zmq.PUSH)
fanSend.bind("tcp://*:5556")

Workers = context.socket(zmq.PULL)
Workers.bind("tcp://*:5559")

# Wait for start of batch
s = fanRecive.recv_multipart()
Points = Bdecode(s[0])
print("Points : "+Points)

while True:
    
    centroids = fanRecive.recv_multipart()
    centroids = json.loads(Bdecode(centroids[0]))
    print("-----------------------")
    print("NewIte")

    
    PointsProcesed = 0
    dicc = {}
    BaseVector = []
    
    for i in range(len(centroids[0])):
        BaseVector.append(0)
        
    for j in range(len(centroids)):
        dicc[j] = {}
        dicc[j]["Sumatoria"] = BaseVector
        dicc[j]["Cant"] = 0
    
    while PointsProcesed < int(Points):
        
            dataWork = Workers.recv_multipart()
            dataWork = json.loads(Bdecode(dataWork[0]))
            for z in range(len(centroids)):
            
                dicc[z]["Cant"] = dicc[z]["Cant"]  + dataWork[str(z)]["Cant"]
                PointsProcesed = PointsProcesed + dataWork[str(z)]["Cant"]
                dicc[z]["Sumatoria"] = SumVect(dicc[z]["Sumatoria"],dataWork[str(z)]["Sumatoria"])
    
    
    
    NewCentroids = []
    
    for k in range(len(centroids)):
        
        NewValCentro = []
        
        for M in range(len(dicc[k]["Sumatoria"])):
            ValTemp = dicc[k]["Sumatoria"][M]
            ValTemp = ValTemp / dicc[k]["Cant"]
            NewValCentro.append(ValTemp)
            
        NewCentroids.append(NewValCentro)
     
    print("-----------------------")
    
    fanSend.send_multipart([Strencode(json.dumps(NewCentroids))])
               
        

    
