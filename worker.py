import zmq
import json
import scipy.spatial.distance 

def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")

def getPoint(Point,CantMovies):
    
    BaseVector = []
    for i in range(CantMovies):
            BaseVector.append(0)
    
    
    f = open("DataSet/Datos.txt", "r")
    n=0
    for linea in f:
        n+=1    
        if  n == Point:
            Datos = linea.split("|")
           
            Datos.remove(Datos[0])
           
            for i in range(len(Datos)):
                Value = Datos[i].split(",")
                BaseVector[int(Value[0])] = int(Value[1])
        break
    f.close()
    return BaseVector
            

def CalculateDist(dataSetI,dataSetII):
    
    return  scipy.spatial.distance.cosine(dataSetI, dataSetII)
    

def CalculateCentroide(Point,Centroids,CantMovies,dicc):
    
    DistMenor = 0
    CentroID = 0
   
    dataSetI = getPoint(Point,CantMovies)
    
    for i in range(len(Centroids)):
        if i == 0 :
             DistMenor = CalculateDist(dataSetI,Centroids[i])
        else:
             DistTemp = CalculateDist(dataSetI,Centroids[i])
             if DistTemp < DistMenor :
                 CentroID = i
                 DistMenor = DistTemp
            
    dicc[CentroID]["Cant"] = dicc[CentroID]["Cant"] + 1
    dicc[CentroID]["Sumatoria"] = SumVect(dicc[CentroID]["Sumatoria"],dataSetI)
         
def SumVect(Vect1,Vect2):
    
    VecTSum = []
    
    for i in range(len(Vect1)):
        Val = Vect1[i] + Vect2[i]
        VecTSum.append(Val)
        
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
        
        print("Procesing work : "+WorkId)
        
        Centrois = json.loads(Centrois)
        
        dicc = {}
        
        BaseVector = []
        for i in range(len(Centrois[0])):
            BaseVector.append(0)
            
        for j in range(len(Centrois)):
            dicc[j] = {}
            dicc[j]["Sumatoria"] = BaseVector
            dicc[j]["Cant"] = 0
            
        for k in range(int(Fin)-int(Init)+1):
            CalculateCentroide(k+1,Centrois,len(Centrois[0]),dicc)
            

        # Send results to sink
        sink.send_multipart([Strencode(json.dumps(dicc))])

Main()