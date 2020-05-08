# -*- coding: utf-8 -*-
"""
Created on Wed May  6 11:23:04 2020

@author: usuario
"""


data = {}

f = open("combined_data_1.txt", "r")

LasMovieID = 0 

count = 0 
count2 = 0

for x in f:
        
      if ":" in x :
          LasMovieID = x.split(":")[0]
          continue
      
      Assig = x.split(",")
      
      arrayTemp = []
      
      if Assig[0] in data : 
          arrayTemp = data[Assig[0]]
 
      value = Assig[1]
      newValue = LasMovieID+","+value
      
      arrayTemp.append(newValue)
      
      data[Assig[0]] = arrayTemp

      count = count + 1
      count2 = count2 + 1
     
    
      if count2 == 5000 :
           count2 = 0
           print(count)
  
f.close()


count = 0
count2 = 0

for User in data:
    
    count = count + 1
    count2 = count2 + 1
   
  
    if count2 == 5000 :
         count2 = 0
         print(count)
    
    namefile = "DataSet/"+str(count)+".txt"
    f = open(namefile, "w")
    
    listToStr = '|'.join([str(elem) for elem in data[User]]) 
    newLine = str(User)+"\n"
    f.write(newLine)
    newLine = str(listToStr)+"\n"
    f.write(newLine)
    f.close()
    

   