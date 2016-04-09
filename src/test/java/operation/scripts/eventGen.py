import os
import csv
import pandas as pd

# Function for generate timestamp distribution using refrence input
def genTimeStamp(inputfile,outputfile,startTime,milliSeconds,factor):
    with open(inputfile) as ifile:
        reader = csv.DictReader(ifile)
        with open(outputfile, 'wb') as ofile:
            fieldnames = ['msgId', 'timestamp']
            writer = csv.DictWriter(ofile, fieldnames=fieldnames)
            writer.writeheader()
            msgId = 0
            for row in reader:
                packetCount = int(row['PacketCount'])
                nextEventGap = milliSeconds/(packetCount * factor)
                nextTS = startTime
                startTime = startTime + milliSeconds
                count  = 0
                while count < packetCount:
                    count += 1
                    msgId += 1
                    writer.writerow({
                    'msgId': msgId,
                    'timestamp': long(nextTS)/1000})
                    nextTS += nextEventGap

# Function to generate the message size distribution using refrence message size
def msgDist(inputfile, outputfile):
    count = []
    with open(inputfile) as ifile:
        reader = csv.DictReader(ifile)
        for row in reader:
            packetCount = int(row['PacketCount'])
            count.append(packetCount)
    with open(outputfile, 'wb') as ofile:
        fieldnames = ['msgSizeMB']
        writer = csv.DictWriter(ofile, fieldnames=fieldnames)
        # writer.writeheader()
        size = 1
        for c in count:
            msg = 1
            while msg <= c:
                writer.writerow({
                'msgSizeMB': size})
                msg += 1
            size += 1

# Directory location of data used by UIDAI_Enroll_Topology
dataDir = os.popen('cd .. && pwd').read().split("\n")[0]

dataDir="/Users/anshushukla/Downloads/Incomplete/stream/PStormScheduler/src/"
# Input-output files required for timestamp generation and storing
tsInputFile = dataDir + 'test/java/operation/input/refrenceInputDist.csv'
tsOutputFile = dataDir + 'test/java/operation/output/eventDist.csv'
# Input-output files required for message distribution
msgInputFile = dataDir + 'test/java/operation/input/refrenceMsgDist.csv'
msgOutputFile = dataDir + 'test/java/operation/output/msgDist.csv'
# Shuffled message distribution file
msgShufDistFile = dataDir + 'test/java/operation/output/msgShufDist.csv'
# Event distribution file with messageId, timestamp and messageSize in MB
eventDistFile = dataDir + 'test/java/operation/input/eventDist.csv'
# startTime is Thu 24 Sep 2015 12:00:00 AM IST GMT+5:30
startTime = 1443033000000
# Milliseconds in 1 hour
milliSeconds = 3600000
# Factor, used for data rate adjusment
factor = 1

# Calling function to generate intermediate files
genTimeStamp(tsInputFile, tsOutputFile, startTime, milliSeconds, factor)
msgDist(msgInputFile, msgOutputFile)

# Shuffling message distribution file for random message size
# bashCommand = "shuf " + msgOutputFile + " > " + msgShufDistFile
bashCommand = msgOutputFile + " > " + msgShufDistFile
os.system(bashCommand)

# Creating final input file for storm UIDAI_Enroll_Topology
edf = pd.read_csv(tsOutputFile)
mdf = pd.read_csv(msgShufDistFile, header=None)
columnName = ["msgSizeMB"]
mdf.columns = columnName
events = pd.concat([edf, mdf], axis=1)
events.to_csv(eventDistFile, index=False)

# Deleting intermediate files
bashCommand = "rm -rf " + dataDir + "/output/*"
os.system(bashCommand)
