import zmq
import json

def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")

def SumVect(Vect1,Vect2):
    
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
    
    Data = fanRecive.recv_multipart()
    typeWork = Bdecode(Data[0])
    centroids = json.loads(Bdecode(Data[1]))
    
    if int(typeWork) == 0 :
        
        
        print("-----------------------")
        print("New Iteration")
    
        
        PointsProcesed = 0
        dicc = {}
        BaseVector = []
       
            
        for j in range(len(centroids)):
            dicc[j] = {}
            dicc[j]["Sumatoria"] = {}
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
            
            NewValCentro = {}
            
            for j in  dicc[k]["Sumatoria"].keys() :
                ValTemp = int(dicc[k]["Sumatoria"][j])
                ValTemp = ValTemp / int(dicc[k]["Cant"])
                ValTemp = int(ValTemp)
                if ValTemp != 0 :
                    NewValCentro[j] = ValTemp
                
            NewCentroids.append(NewValCentro)
         
        print("-----------------------")
        
        fanSend.send_multipart([Strencode(json.dumps(NewCentroids))])
        
    else : 
        
         dicc = {}
         Inerence = 0
         
         for j in range(len(centroids)):
                dicc[j] = []
                
         PointsProcesed = 0
                
         while PointsProcesed < int(Points):
                dataWork = Workers.recv_multipart()
                dataWork = json.loads(Bdecode(dataWork[0]))
       
                for z in range(len(centroids)):
                    PointsProcesed += len(dataWork[str(z)]["Points"])
                    
                    Inerence = Inerence + float(dataWork[str(z)]["SumDist"])
                    
                    for x in range( len(dataWork[str(z)]["Points"] )):
                   
                        dicc[z].append(dataWork[str(z)]["Points"][x])
          
         #print(dicc)
                        
         print("Inerence : "+str(Inerence))
                
         for z in range(len(centroids)):
                print("************")
                print("Centroid: "+str(z))
                print("PointCant: "+str(len(dicc[z])))
                print("************")
        

    
