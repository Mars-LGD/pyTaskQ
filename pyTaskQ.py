#!/usr/bin/python
#coding:utf-8

import os
import sys
import time
import threading
import logging
try:
    import simple_json as json
except:
    import json

import MySQLdb
import torndb
from tornado import httpclient, gen, ioloop, queues
from tornado.web import RequestHandler, Application, url

nr_jobSaver = 1
nr_jobConsumer = 3

db_conf = {
    'host': '127.0.0.1',
    'port': 33060,
    'user': 'root',
    'pass': '123456',
    'db'  : 'queue',
}

"""
CREATE DATABASE queue;
USE queue;
CREATE TABLE jobs (
    id int primary key auto_increment,
    processor char(32),
    args text, -- json
    src_ip char(32),
    state int not null default 0, -- 0=new 1=processing 2=finish 3=fail
    create_at datetime not null,
    update_at timestamp not null default current_timestamp on update current_timestamp
    , KEY `I_create_state` (`create_at`, `state`)
);
"""

refuseJob    = False
currentJobID = 0
RequestQueue = queues.Queue()
ProcessQueue = queues.Queue()

class Job(object):
    JOB_NEW         = 0
    JOB_PROCESSING  = 1
    JOB_FINISH      = 2
    JOB_FAIL        = 3
    def __init__(self, id, processor, args, src_ip='', state=0):
        self.d = {
            'id'        : id,
            'processor' : processor,
            'args'      : args,
            'src_ip'    : src_ip,
            'state'     : state,
            'create_at' : time.time(),
        }

    @gen.coroutine
    def saveToQueue(self, q):
        Log("[JOB:%d] saved to queue", self.d['id'])
        yield q.put(self)

    def saveToLog(self):
        #TODO: save to log
        Log('TODO: save to log ' + json.dumps(self.d))

    def saveToMySQL(self):
        #TODO: save to mysql
        Log("TODO: save to mysql " + json.dumps(self.d))

    def setState(self, newState):
        self.d['state'] = newState
        #TODO: update mysql
        Log("TODO: job(%d) set to state %d", self.d['id'], newState)

    def process(self):
        self.setState(self.JOB_PROCESSING)

        Log("[JOB:%d] process", self.d['id'])
        result = 0 #TODO: process

        if result == 0:
            self.setState(self.JOB_FINISH)
        else:
            self.setState(self.JOB_FAIL)

def Log(message, *args):
    if args != None:
        message = message % (args)
    print >>sys.stderr, message

class XRequestHandler(RequestHandler):
    def json(self, errno, message, data=None):
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps({'errno': errno, 'message': message, 'data': data}))

class JobHandler(XRequestHandler):
    @gen.coroutine
    def get(self):
        global refuseJob
        if refuseJob:
            self.json(1, 'refuseJob')
            return

        args = self.get_argument("args", None)
        if args == None or args == "":
            self.json(2, 'Bad Request')
            return

        processor = self.get_argument("processor", "php")

        global currentJobID
        currentJobID += 1
        job = Job(currentJobID, processor, args, self.request.remote_ip)

        job.saveToLog()

        job.saveToQueue(RequestQueue)

        self.json(0, '', job.d)


class StatusHandler(XRequestHandler):
    def get(self):
        self.json(0, '', {
            'RequestQueue': RequestQueue.qsize(),
            'ProcessQueue': ProcessQueue.qsize()
        })

class StopServerHandler(XRequestHandler):
    def get(self):
        global refuseJob

        if refuseJob != True:
            refuseJob = True

        rqsize = RequestQueue.qsize()
        pqsize = ProcessQueue.qsize()
        if rqsize != 0 or pqsize != 0:
            resp = """
<html>
    <head>
        <title>Please Wait...</title>
        <meta http-equiv="refresh" content="3" />
    </head>
    <body align="center" style="margin:30px;">
        <p>Please wait for jobs to be save and processed.</p>
        <p>This page will refresh untill server stops.</p>
        <p>Jobs in RequestQueue: %d not saved</p>
        <p>Jobs in ProcessQueue: %d not processed</p>
    </body>
</html>
""" % (rqsize, pqsize)
            self.set_header("Refresh", "3")
            self.write(resp)
            return

        self.write("Server Stopped.")

        ioloop.IOLoop.current().stop()

class JobSaver(threading.Thread):
    def __init__(self, num):
        threading.Thread.__init__(self)
        self._id = num

    @gen.coroutine
    def run(self):
        Log("[js:%d] run" % self._id)
        global RequestQueue
        global ProcessQueue
        while True:
            yield gen.sleep(1) #TODO: for test
            job = yield RequestQueue.get()
            job.saveToMySQL()
            job.saveToQueue(ProcessQueue)

class JobConsumer(threading.Thread):
    def __init__(self, num):
        threading.Thread.__init__(self)
        self._id = num

    @gen.coroutine
    def run(self):
        Log("[jc:%d] run" % self._id)
        global ProcessQueue
        while True:
            yield gen.sleep(1.5) #TODO: for test
            job = yield ProcessQueue.get()
            job.process()

def main():
    arr_threads = []

    #TODO: rebuild RequestQueue from log, and set currentJobID


    #prepare Thread to Consume RequestQueue and Produce ProcessQueue
    for i in range(nr_jobSaver):
        arr_threads.append(JobSaver(i))

    #prepare Thread to Consume ProcessQueue
    for i in range(nr_jobConsumer):
        arr_threads.append(JobConsumer(i))

    #start threads
    for t in arr_threads:
        t.start()

    #start server for incoming jobs
    app = Application([
        url(r"/", JobHandler),
        url(r"/status", StatusHandler),
        url(r"/stop", StopServerHandler),
    ])
    app.listen(8888)
    ioloop.IOLoop.current().start()

if __name__ == "__main__":
    logging.basicConfig()
    main()

