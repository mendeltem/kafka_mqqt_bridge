#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 14:50:27 2020

@author: panda
"""

# Returns the new average 
# after including x 
def getAvg(prev_avg, x, n): 
    return ((prev_avg * 
             n + x) / 
            (n + 1)); 
  
# Prints average of  
# a stream of numbers 
#def streamAvg(arr, n): 
#    avg = 0; 
#    for i in range(n): 
#        avg = getAvg(avg, arr[i], i); 
#        print("Average of ", i + 1, 
#              " numbers is ", avg); 
  
# Driver Code 
arr = [10, 20, 30, 
       40, 50, 60]; 
       
avg = 0;        
          
       
i = 0
       

for i in range(len(arr)): 

    avg = getAvg(avg, arr[i], i); 
    print("Average of ", i + 1," numbers is ", avg); 




smedian = streamMedian()


smedian.insert(arr[0])

smedian.getMean()

# This code is contributed 
# by mits 