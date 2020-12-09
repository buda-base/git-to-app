import hashlib
import io
import os
import json
from pathlib import Path
import glob
from rdflib import URIRef, Literal, BNode, Graph, ConjunctiveGraph
from rdflib.namespace import RDF, RDFS, SKOS, OWL, Namespace, NamespaceManager, XSD
from tqdm import tqdm
import gzip
import sys
from datetime import datetime
import time
import re
import pyewts
import csv

converter = pyewts.pyewts()

#GITPATH = "/home/eroux/BUDA/softs/xmltoldmigration/tbrc-ttl/iinstances"
GITPATH = "../xmltoldmigration/tbrc-ttl/"
if len(sys.argv) > 1:
    GITPATH = sys.argv[1]

OUTDIR = "output/"

VERBMODE = "-v"
if len(sys.argv) > 2:
    VERBMODE = sys.argv[2]

BDR = Namespace("http://purl.bdrc.io/resource/")
BDO = Namespace("http://purl.bdrc.io/ontology/core/")
TMP = Namespace("http://purl.bdrc.io/ontology/tmp/")
BDA = Namespace("http://purl.bdrc.io/admindata/")
ADM = Namespace("http://purl.bdrc.io/ontology/admin/")

NSM = NamespaceManager(Graph())
NSM.bind("bdr", BDR)
NSM.bind("bdo", BDO)
NSM.bind("tmp", TMP)
NSM.bind("bda", BDA)
NSM.bind("adm", ADM)
NSM.bind("skos", SKOS)
NSM.bind("rdfs", RDFS)

INDEXES = {
    "persons": {},
    "works": {},
    "workparts": {}
}

# TODO: only add author persons

UNWANTED = {}
SEENPERSONS = {}

with open('unwantedInstances.txt', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        UNWANTED[row[0]] = True

def getTibNames(s, p, model, index):
    labels = []
    prefLabel = ""
    for t, _, tl in model.triples( (s, SKOS.prefLabel, None) ):
        st = None
        if tl.language == "bo-x-ewts":
            st = converter.toUnicode(str(tl))
        elif tl.language == "bo":
            st = str(tl)
        else:
            continue
        prefLabel = st
        labels.append(st)
    for _, _, t in model.triples( (s, p, None) ):
        for _, _, tl in model.triples( (t, RDFS.label, None) ):
            st = None
            if tl.language == "bo-x-ewts":
                st = pyewts.toUnicode(str(tl))
            elif tl.language == "bo":
                st = str(tl)
            else:
                continue
            if st == prefLabel:
                continue
            labels.append(st)
    if index is None:
        return labels
    for l in labels:
        toindex = l
        for s in ["་བཞུགས་སོ", "༼", "་ཞེས་བྱ་བ", "་ཅེས་བྱ་བ"]:
            lastparidx = toindex.rfind(s)
            if lastparidx != -1:
                toindex = toindex[:lastparidx]
        toindex = toindex.strip("།༔་")
        if toindex not in index:
            index[toindex] = []
        _, _, sLname = NSM.compute_qname_strict(s)
        index[toindex].append(sLname)
    return labels

def getParts(mw, model, parts):
    for _, _, wp in model.triples( (mw, BDO.hasPart, None) ):
        wpt = None
        for _, _, wptO in model.triples( (wp, BDO.partType, None) ):
            wpt = wptO
        if wpt == BDR.PartTypeTableOfContent or wpt == BDR.PartTypeChapter:
            continue
        wainfo = None
        for _, _, wa in model.triples( (wp, BDO.instanceOf, None) ):
            _, _, waLname = NSM.compute_qname_strict(wa)
            wainfo = getWA(waLname)
        idx = INDEXES["workparts"] if wpt != BDR.PartTypeVolume else None
        titles = getTibNames(wp, BDR.hasTitle, model, idx)
        getParts(wp, model, parts)

def inspectMW(iFilePath):
    likelyiLname = Path(iFilePath).stem
    if likelyiLname in UNWANTED:
        return
    model = ConjunctiveGraph()
    model.parse(str(iFilePath), format="trig")
    # if status != released, pass
    if (None,  ADM.status, BDA.StatusReleased) not in model:
        return
    mw = BDR[likelyiLname]
    mwinfo = {}
    wainfo = None
    for _, _, wa in model.triples( (mw, BDO.instanceOf, None) ):
        _, _, waLname = NSM.compute_qname_strict(wa)
        wainfo = getWA(waLname)
    for _, _, pnO in model.triples( (mw, BDO.publisherName, None) ):
        pn = str(pnO)
        if pnO.language == "bo-x-ewts":
            pn = str(pyewts.toUnicode(pnO))
        mwinfo["pn"] = pn
    for _, _, plO in model.triples( (mw, BDO.publisherLocation, None) ):
        pl = str(plO)
        if plO.language == "bo-x-ewts":
            pl = str(pyewts.toUnicode(plO))
        mwinfo["pl"] = pl
    titles = getTibNames(mw, BDR.hasTitle, model, INDEXES["works"])
    if len(titles) != 0:
        mwinfo["title"] = titles
    for _, _, wa in model.triples( (mw, BDO.instanceOf, None) ):
        _, _, waLname = NSM.compute_qname_strict(wa)
        wainfo = getWA(waLname)
        if wainfo and len(wainfo) != 0:
            mwinfo["creator"] = list(wainfo)
    parts = []
    getParts(mw, model, parts)
    if len(parts) != 0:
        mwinfo["hasParts"] = True
    else:
        parts = None
    return [mwinfo, parts]

CACHEDWINFO = {}

def getWA(waLname):
    if waLname in CACHEDWINFO:
        return CACHEDWINFO[waLname]
    md5 = hashlib.md5(str.encode(waLname))
    two = md5.hexdigest()[:2]
    fpath = GITPATH+"works/"+two+"/"+waLname+".trig"
    model = ConjunctiveGraph()
    model.parse(str(fpath), format="trig")
    authors = ()
    for aac, _, p in model.triples( (None, BDO.agent, None) ):
        iinstanceRes = s
        for _, _, r in model.triples( (aac, BDO.role, None) ):
            if r == BDR.R0ER0019 or BDR.R0ER0025:
                _, _, pLname = NSM.compute_qname_strict(p)
                authors.add(pLname)
                SEENPERSONS[pLname] = True
    nbi = 0
    for _, _, i in model.triples( (None, BDO.workHasInstance, None) ):
        nbi += 1
    if nbi > 2:
        CACHEDWINFO[waLname] = authors
    return authors

def inspectPerson(pFname):
    likelypLname = Path(pFname).stem
    if not likelypLname in SEENPERSONS:
        return
    model = ConjunctiveGraph()
    model.parse(str(fpath), format="trig")
    names = getTibNames(BDR[likelypLname], BDR.personName, model, INDEXES["persons"])
    return {"name": names}

MAXKEYSPERINDEX = 20000

def main(mwrid=None):
    i = 0
    os.makedirs("output/persons/bdr/", exist_ok=True)
    os.makedirs("output/works/bdr/", exist_ok=True)
    os.makedirs("output/workparts/bdr/", exist_ok=True)
    l = sorted(glob.glob(GITPATH+'/instances/**/MW*.trig'))
    for fname in VERBMODE == "-v" and tqdm(l) or l:
        infol = inspectMW(fname)
        likelyLname = Path(fname).stem
        mwinfo = infol[0]
        partsinfo = infol[1]
        with open('output/works/bdr/'+likelyLname+'.json', 'w') as f:
            json.dump(mwinfo, f, ensure_ascii=False)
        if partsinfo:
            with open('output/workparts/bdr/'+likelyLname+'.json', 'w') as f:
                json.dump(partsinfo, f, ensure_ascii=False)
        i += 1
        break
    l = sorted(glob.glob(GITPATH+'/persons/**/P*.trig'))
    for fname in VERBMODE == "-v" and tqdm(l) or l:
        pinfo = inspectPerson(fname)
        likelyLname = Path(fname).stem
        with open('output/persons/bdr/'+likelyLname+'.json', 'w') as f:
            json.dump(pinfo, f, ensure_ascii=False)
        i += 1
        break
    for idxname, idx in INDEXES.items():
        fileCnt = 0
        towrite[name] = values
        fpath = OUTDIR+idxname+"-"+fileCnt+".json"
        fp = open(fpath, 'w')
        keyCnt = 0
        for name, values in idx.items():
            if keyCnt == 0:
                fp.write('{')
            else:
                fp.write(',')
            fp.write('"'+name+'":'json.dumps(values))
            keyCnt += 1;
            if keyCnt > maxkeysPerIndex:
                fileCnt += 1
                fp.write('}')
                fp.flush()
                fp.close()
                fpath = OUTDIR+idxname+"-"+fileCnt+".json"
                fp = open(fpath, 'w')
                keyCnt = 0
        if keyCnt != 0:
            fp.write('}')
            fp.flush()
            fp.close()

main()