#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 24 16:07:32 2018

@author: tpchen
"""

# open socket
###============================###
# may open two socket using two threads (with differetn port)
# to simulate multi-client using one port. 
###============================###

import sys
import socket
from socket import *
import time
import matplotlib.pyplot as plt
def help():
    s = """
    client.py - client program for integer stream
    USAGE:
        client.py -h
        client.py <host> <port>
    OPTIONS:
        -h get this help page
        <host> host IP address or host name(currently, only support localhost)
        <port> port number
    EXAMPLES:
        client.py -h
        clien.py localhost 44419
    CONTACT:
        Tzu-Ping Chen, 669/377-9163 tchen2@scu.edu
    """
    print(s)
    raise SystemExit(1)

class Client():
    def __init__(self):
        self.port=0
        self.host=0
        self.power = False
        self.total_time = 0
        self.avg_time_num =[]
        self.avg_time_list = []
        self.tok = (b'\x02').decode()
        self.SEND_PROTO = '8'
        self.GET_PROTO= '10'
        self.REQUEST_LEADER_PROTO = '9'
        self.ENABLE_HOST_PROTO = '11'
        
    def changeval(self,char,val):
        # sent char's value to server using socket
        self.sent_to_server(self.SEND_PROTO,char,val)
        print("sent successful\n")
        return
    def changehost(self,hostlist):                
        for i in hostlist:
            sok = socket(AF_INET,SOCK_STREAM)
            #print(i)
            sok.connect((i,self.port))
            p = self.REQUEST_LEADER_PROTO +'\n'
            ps = str.encode(p)
            sok.send(ps)
            
            done = False
            recv_list = []
            while not done:
                d = sok.recv(1024)
                if d == b'':
                    done = True
                else:
                    recv_list.append(d)
            sok.close()
            #print(recv_list)
            if len(recv_list) == 1:
                if recv_list[0].decode() == '1\n':
                    return i
            else:
                continue
    def RecieveManual(self,recved,proto):
        
        # recv nothing
        if recved == []:
            print("\nI got nothing :(\n")
        
        # only recieved header, means server's dead
        if len(recved) == 1 and recved[0] == self.tok:
            print("ERROR : Server didn't send anything to client")
        if len(recved) == 1 and recved[0] != self.tok and proto == self.GET_PROTO:
            tempp = recved[0].decode()
            tempp = tempp.replace(self.tok,'')
            print("Recieved ", tempp)
        # case : [b'\xac\xed\x00\x05', b'\x02' , b'500' ]
        if len(recved) == 3 and proto == self.GET_PROTO:
            print('Recieved : ', recved[2].decode())
        # case : [b'blahblah' , b'\x02500']
        if len(recved) == 2 and proto == self.GET_PROTO:
            x = recved[1].decode()
            x = x.replace(self.tok,'')
            print('Recieved : ', x )
        
        if proto == self.REQUEST_LEADER_PROTO:
            word = []
            IPadds=[]
        # if recieved multiple address, means it's not host, change host to the boomandoo
            for i in recved:
                word.append(i.decode())
            if len(word) == 1:
                if word[0] == "1\n":
                    print('Link host to :',self.host)
                    return  
            #print(word)
            for key in word:
                keys = key.split('\n')
                for parse in keys:
                    if parse :
                        IPadds.append(parse)
            #print(IPadds)
            self.host = self.changehost(IPadds)
            
            print('Link host to : ',self.host)
        if len(recved) == 2:
            if recved[1].decode() == 'OK':
                print("Data Commited!") 
        #print(recved)
        return 
    
    def testcasechange(self,file):
        # read file n line by line and see if it's good
        try:
            f = open(file,'r')
        except IOError:
            print("Error: file not exist :(")
            return
        cnt = 0
        self.total_time = 0
         
        for line in f:
            cnt+=1
            print(cnt)
            temp = line.split(" ")
            temp[1]= temp[1].replace('\n\n','\n')
            # sent char's value to server using socket
            time1 = time.time()
            self.sent_to_server(self.SEND_PROTO,temp[0],temp[1])
            time2 = time.time()
            self.total_time += time2-time1
        self.total_time = self.total_time/cnt
        self.avg_time_num.append(cnt)
        self.avg_time_list.append(self.total_time)
        # performance : avg_latency
        print(self.total_time)
        
        return
    
    def sent_to_server(self,p,c,v):
        print('changing value : ',c,v) #(1.p(str) 2.c (str) 3.v(str))
        # send : 1. protocol(int 8) 2.char(str) 3.val(number)
        sok = socket(AF_INET,SOCK_STREAM)
        sok.connect((self.host,self.port))
        #print("connected...")
        p+= '\n'
        c+= '\n'
        v+= '\n'
        
        ps = str.encode(p)
        cs = str.encode(c)
        vs =  str.encode(v)
        
        sok.send(ps)
        sok.send(cs)
        sok.send(vs)
        
        done = False
        recv_list = []
        while not done:
            d = sok.recv(1024)
            if d == b'':
                done = True
            else:
                recv_list.append(d)
                #print(d)
        # recieve on "yes"
        #print(data)
        
        #print(recv_list[2].decode())
        
        # processing the recieved package:
        # [b'\xac\xed\x00\x05',b'\x02',b'No']
        sok.close()
        self.RecieveManual(recv_list,self.SEND_PROTO)
        
      
        
        return
    
    def change_route(self,c,v):
        # add this protocol after the leader election part complete :DDDDDDDDD

        '''
        self.host = recv_pakage[0].decode()
        self.port = int(recv_pakage[1].decode())
        '''
        a,b = self.host,self.port
        print('Client re-route to leader at :\n IP_Address : %s \n Port : %s'%(a,b))
        print('data resending...\n\n')
        
        self.sent_to_server(self.SEND_PROTO,c,v)
        return
    
    def get_value(self,p,c):
        sok = socket(AF_INET,SOCK_STREAM)
        sok.connect((self.host,self.port))
        
        p+= '\n'
        c+= '\n'
        #v+= '\n'
        ps = str.encode(p)
        cs = str.encode(c)
        #vs =  str.encode(v)
        sok.send(ps)
        sok.send(cs)
        #sok.send(vs)
        
        done = False
        recv_list = []
        while not done:
            d = sok.recv(1024)
            if d == b'':
                done = True
            else:
                recv_list.append(d)
        
        self.RecieveManual(recv_list,self.GET_PROTO)
        
        sok.close()
        return
    def Init_Server(self):
        sok = socket(AF_INET,SOCK_STREAM)
        sok.connect((self.host,self.port))
        
       
        
        Pstart = self.ENABLE_HOST_PROTO+'\n'
        Pstarts = str.encode(Pstart)
        sok.send(Pstarts)
        done = False
        recv_list=[]
        print('Waiting Server respond...\n')
        while not done:
            d = sok.recv(1024)
            if d == b'':
                done = True
            else:
                recv_list.append(d)
        
        
        print('Server Started...\n')
        sok.close()
        return
    def Leader_get(self):
        sok = socket(AF_INET,SOCK_STREAM)
        sok.connect((self.host,self.port))
        p = self.REQUEST_LEADER_PROTO +'\n'
        ps = str.encode(p)
        sok.send(ps)
        
        done = False
        recv_list = []
        while not done:
            d = sok.recv(1024)
            if d == b'':
                done = True
            else:
                recv_list.append(d)
        
        self.RecieveManual(recv_list,self.REQUEST_LEADER_PROTO)
        sok.close()
        return 
    def time_to_send_some_real_shit(self):
        '''
        sok = socket(AF_INET,SOCK_STREAM)
        print("socket success")
        sok.connect((self.host,self.port))
        '''
        self.power = True
        # raw input 
        while(self.power):
            print("Enter command below:")
            x = input() 
            xli = str(x).split(" ")
            # x[0] should be using "add" "testcase"
            if xli[0] == "cv" and len(xli) == 3:
                SuperClient.changeval(xli[1],xli[2])
            elif xli[0] == "tstc" and len(xli) == 2:
                SuperClient.testcasechange(xli[1])
            elif xli[0] == "gv":
                SuperClient.get_value(self.GET_PROTO,xli[1])
            elif xli[0] == "gh":
                SuperClient.Leader_get()
            elif xli[0] == "wk":
                SuperClient.Init_Server()
            elif xli[0] == "chgIP" and len(xli) == 2:
                SuperClient.host = xli[1]
            elif xli[0] == "quit":
                self.power = False
                print("Client shut down...\n")
            else:
                be_good_plz="""
                The client can only use these format:
                    cv <char> <number>     : update <char> to <number>
                    tstc <filepath>        : test all the inputs under <filepath> file.
                    gv <char>              : get <char> value from server
                    gh                     : change connection loc to host
                    wk                     : wake the server up (used in initializing server only)
                    chgIP <IPadd>          : change connection IP addr
                    quit                   : quit client 
                """
                print(be_good_plz)
                continue
        #sok.close()
    
    # =============================no probs at top===============================

    # ===========================start testing part . ===============================
    '''
    testing:
        1. the latency betweeeeeeen commu(average latency)
        2. 
        3. 
    '''
    def plot_latency_bar(self):
        x = self.avg_time_num 
        y = self.avg_time_list
        if x and y:
            plt.bar(x,y)
        

if __name__ == "__main__":
    
    if len(sys.argv) != 3:
        help()
    
    SuperClient = Client()
    SuperClient.host = sys.argv[1]
    SuperClient.port = int(sys.argv[2])
    
    # open config file , 
    
    
    init_words = """
    **********************
    Super Client Initiated
    **********************
    """
    print(init_words)
    
    #SuperClient.Init_Server()
    #SuperClient.Leader_get()    

    SuperClient.time_to_send_some_real_shit()
        
    print("\n End testing  :) \n")
        
        
        
        
        
        
        
        
