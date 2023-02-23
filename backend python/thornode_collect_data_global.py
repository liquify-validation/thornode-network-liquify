import time

import requests
import json
import random
import datetime
from common import getDB, commitQuery, grabQuery

def getAndSaveBlockTime(height):
    """
    getAndSaveBlockTime looks over the last 100 blocks and passes back the average block time

    :param height: current block height

    :return avgBlock: the average block time over the last 100 blocks
    """
    url1 = "https://rpc.ninerealms.com/block?height="+str(height)
    url2 = "https://rpc.ninerealms.com/block?height="+str(height-100)

    bt1_resp = requests.get(url1, timeout=5)
    bt1 = json.loads(bt1_resp.text)["result"]["block"]["header"]["time"]
    date_format = datetime.datetime.strptime(bt1.split('.', 1)[0] + 'Z', "%Y-%m-%dT%H:%M:%SZ")
    bt1_unix = datetime.datetime.timestamp(date_format)

    bt2_resp = requests.get(url2, timeout=5)
    bt2 = json.loads(bt2_resp.text)["result"]["block"]["header"]["time"]
    date_format = datetime.datetime.strptime(bt2.split('.', 1)[0] + 'Z', "%Y-%m-%dT%H:%M:%SZ")
    bt2_unix = datetime.datetime.timestamp(date_format)

    diff = bt1_unix - bt2_unix
    avgBlock = str(diff/100)
    query = "UPDATE noderunner.thornode_monitor_global SET secondsPerBlock = '{field}' WHERE primary_key = 1;".format(field=avgBlock)
    commitQuery(query)
    return avgBlock

def getCoinGeckoInfoAndSave():
    """
    getCoinGeckoInfoAndSave pulls thor data from coingecko api and save to thornode_monitor_global DB

    :return avgBlock: the average block time over the last 100 blocks
    """
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=thorchain&order=market_cap_desc' \
          '&per_page=100&page=1&sparkline=false '
    resp = requests.get(url)
    data = json.loads(resp.text)
    query = "UPDATE noderunner.thornode_monitor_global SET coingecko = '{field}' WHERE primary_key = 1;".format(field=json.dumps(data))
    commitQuery(query)
    return data

def getAndSaveLastChurn():
    """
    getAndSaveLastChurn update the last churn block in the db

    :return status_since: returns the last churn block
    """
    url = 'https://thornode.ninerealms.com/thorchain/vaults/asgard'
    resp = requests.get(url)
    status_since = json.loads(resp.text)[0]["status_since"]
    query = "UPDATE noderunner.thornode_monitor_global SET lastChurn = '{field}' WHERE primary_key = 1;".format(field=int(status_since))
    commitQuery(query)
    return status_since

def checkRetiringVaults():
    """
    checkRetiringVaults checks if any vaults are returing and updates the DB accordingly

    :return areWeRetiring: returns the retiring flag
    """
    url = 'https://thornode.ninerealms.com/thorchain/vaults/asgard'
    resp = requests.get(url)
    data = json.loads(resp.text)
    areWeRetiring = len([x for x in data if "RetiringVault" == x['status']]) > 0

    query = "UPDATE noderunner.thornode_monitor_global SET retiring = '{field}' WHERE primary_key = 1;".format(field=int(areWeRetiring))
    commitQuery(query)
    return areWeRetiring

def cleanUpDB():
    """
    cleanUpDB purges our DB of nodes they are no longer present

    :return: returns true if purged false if nothing to purge
    """
    currentDBData = (grabQuery('SELECT * FROM noderunner.thornode_monitor'))
    currentAddrList = [x['node_address'] for x in currentDBData]

    response_API = requests.get('https://thornode.ninerealms.com/thorchain/nodes')
    data = json.loads(response_API.text)
    # sanitise data remove any empty elements
    nodes = [x for x in data if '' != x['node_address']]
    nineRelms = [x['node_address'] for x in nodes]

    removeList = list(set(currentAddrList).symmetric_difference(set(nineRelms)))
    if len(removeList) == 0:
        return False

    toRemoveString = "'"+"', '".join(removeList)+"'"
    query = "DELETE FROM noderunner.thornode_monitor where node_address IN {feild}".format(
        field=toRemoveString)

    commitQuery(query)
    return True

def getConstants():
    """
    getConstants grabs the current vaules of CHURNINTERVAL and BADVALIDATORREDLINE and updates the DB

    :returns CHURNINTERVAL BADVALIDATORREDLINE
    """
    url = "https://thornode.ninerealms.com/thorchain/mimir"
    response_API = requests.get(url)
    data = json.loads(response_API.text)

    query = "UPDATE noderunner.thornode_monitor_global SET churnInterval = '{field}' WHERE primary_key = 1;".format(
        field=data['CHURNINTERVAL'])
    commitQuery(query)

    query = "UPDATE noderunner.thornode_monitor_global SET BadValidatorRedline = '{field}' WHERE primary_key = 1;".format(
        field=data['BADVALIDATORREDLINE'])
    commitQuery(query)

    return data['CHURNINTERVAL'],data['BADVALIDATORREDLINE']

def collectDataGlobal():
    """
    collectDataGlobal update the thornode_monitor_global DB
    """
    #Grab the current block height
    height = grabQuery('SELECT maxHeight FROM noderunner.thornode_monitor_global')[0]['maxHeight']

    constants = getConstants()

    # Calculate average block time over the past 100 blocks
    secondsPerBlock = getAndSaveBlockTime(height)

    # Grab the latest coingecko info
    getCoinGeckoInfo = getCoinGeckoInfoAndSave()

    # Grab the last churn block
    lastChurn = getAndSaveLastChurn()

    # Check for any retired vaults
    retiringVault = checkRetiringVaults()

    # Clean up any old nodes from the database
    cleaned = cleanUpDB()

    print(str(secondsPerBlock),str(getCoinGeckoInfo),str(lastChurn),str(retiringVault),str(cleaned))

