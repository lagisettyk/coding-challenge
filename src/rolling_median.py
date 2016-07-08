import json
from datetime import datetime, timedelta
import dateutil.parser
from graph import Graph

def median(l):
    '''
    Method for computing median based on vertex degrees.
    We could optimize this by using numpy package but wanted to provide
    solution based on std python libraries.
    '''
    #print "$$$$$ rolling medain: ", l
    sortedLst = sorted(l)
    lstLen = len(l)
    index = (lstLen - 1) // 2
    if (lstLen % 2):
        return "{0:.2f}".format(sortedLst[index])
    else:
        return "{0:.2f}".format((sortedLst[index] + sortedLst[index + 1])/2.0)

def invalidateEdges(g, activeRecords, max_ts):
    '''
    Utility method for identifying invalid edges in the graph which are
    expired.
    '''
    edgesToRemove = \
      [(payment[0],payment[1]) for payment in activeRecords \
                    if payment[2] < (max_ts - timedelta(seconds=59))]

    ### Remove invalid edges from graph and update list of
    ### active records...
    if len(edgesToRemove) >= 1:
        g.removeEdges(edgesToRemove)
        #### Update activeRecord list by removing the edges...
        activeRecords = \
           [record for record in activeRecords \
              if (record[0],record[1]) not in edgesToRemove]

    return activeRecords

def writeOutput(data, f):
    '''
    Methid for writing out rolling mean to output.
    '''
    f.write(str(data) + "\n")


def processPayments(inputFile, outputFile):
    '''
    Method for processing payment records with moving window of 1 minute.
    simulates real time straming by reading records one by one using python
    generator object to read lines from file.
    '''
    ### Initialize Graph
    g = Graph()

    ### Initialize activerecords list at any given time
    activeRecords = []

    ### Initialize max_ts older date..
    max_ts = dateutil.parser.parse('1970-01-01T23:23:12Z')

    ### Process payment records....
    with open(outputFile,'w') as wf:
        with open(inputFile, 'r+') as f:

            for data in f:
                payment = json.loads(data)
                ts = dateutil.parser.parse(payment['created_time'])
                ### Keep track of current max processing record
                if ts > max_ts:
                    max_ts = ts

                ### Update Graph based on latest payment record
                if ts >= (max_ts - timedelta(seconds=59)):
                    activeRecords.append((payment['actor'],payment['target'],ts))

                    ### Remove edges based on expiry of 1 minute windows
                    activeRecords = invalidateEdges(g, activeRecords, max_ts)

                    ### Update latest payment record to graph
                    g.addVertex(payment['actor'])
                    g.addEdge(payment['actor'], payment['target'])
                    writeOutput(median(g.getVerticesDegrees()), wf)
                else:
                    ### Write out median even in cases of out of order record
                    writeOutput(median(g.getVerticesDegrees()), wf)

if __name__ == '__main__':
    processPayments('venmo_input/venmo-trans.txt', 'venmo_output/output.txt')
