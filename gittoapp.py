import hashlib
import io
import os
import json
from pathlib import Path
import glob
import rdflib
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
import hashlib

converter = pyewts.pyewts()

# see https://github.com/RDFLib/rdflib/issues/806
x = rdflib.term._toPythonMapping.pop(rdflib.XSD['gYear'])

GITPATH = "../tbrc-ttl/"
if len(sys.argv) > 1:
    GITPATH = sys.argv[1]

OUTDIR = "BDRCLIB/"
if len(sys.argv) > 2:
    OUTDIR = sys.argv[2]

VERBMODE = "-v" in sys.argv
RICMODE = "-ric" in sys.argv
OAMODE = "-oa" in sys.argv

print("converting git repos in %s, output in %s, verb=%s, ricmode=%s, openaccessmode=%s" % (GITPATH, OUTDIR, VERBMODE, RICMODE, OAMODE))

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
    "workparts": {},
    "rititles": {}
}

PTLNAMETOAPP = {
    "PrintMethod_Relief_WoodBlock": "x",
    "PrintMethod_Manuscript": "m",
    "PrintMethod_Modern": "mo",
    "PrintMethod_Xerography": "l"
}

# TODO: only add author persons

UNWANTED = {}
CREATOROF = {}

with open('unwantedInstances.txt', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        UNWANTED[row[0]] = True

def getTibNames(s, p, model, index, rititle = None):
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
                st = converter.toUnicode(str(tl))
            elif tl.language == "bo":
                st = str(tl)
            else:
                continue
            if st == prefLabel:
                continue
            labels.append(st)
    if index is None:
        return labels
    _, _, sLname = NSM.compute_qname_strict(s)
    indexValue = sLname
    #if rititle is not None:
    #    indexValue += '|'+rititle
    for l in labels:
        toindex = l
        # see https://github.com/buda-base/BDRC-Lib-App/issues/70
        # and https://github.com/buda-base/BDRC-Lib-App/issues/79
        # basically the index is used for display so we shouldn't change
        # anything in there
        #for s2 in ["་བཞུགས་སོ", "༼", "་ཞེས་བྱ་བ", "་ཅེས་བྱ་བ"]:
        #    lastparidx = toindex.rfind(s2)
        #    if lastparidx != -1:
        #        toindex = toindex[:lastparidx]
        #toindex = toindex.strip("།༔་")
        if toindex not in index:
            index[toindex] = []
        index[toindex].append(indexValue)
    return labels

# n : [
#   {
#     "id": "W123",
#     "t": ["title1", "title2"],
#     "n": []
#   },
#   ...
# ]
def getParts(mw, model, rititle):
    res = []
    idtopartnum = {}
    for _, _, wp in model.triples( (mw, BDO.hasPart, None) ):
        wpt = None
        _, _, wpLname = NSM.compute_qname_strict(wp)
        for _, _, wptO in model.triples( (wp, BDO.partType, None) ):
            wpt = wptO
        if wpt == BDR.PartTypeTableOfContent or wpt == BDR.PartTypeChapter:
            continue
        for _, _, wptO in model.triples( (wp, BDO.partIndex, None) ):
            idtopartnum[wpLname] = int(wptO)
        idx = INDEXES["workparts"] if wpt == BDR.PartTypeText else None
        titles = getTibNames(wp, BDO.hasTitle, model, idx, rititle)
        _, _, wpLname = NSM.compute_qname_strict(wp)
        node = {"id": wpLname, "t": titles}
        subParts = getParts(wp, model, rititle)
        if subParts is not None and len(subParts) > 0:
            node["n"] = subParts
        # if no subparts and no label, continue
        elif len(titles) < 1:
            continue 
        res.append(node)
    res = sorted(res, key=lambda n: idtopartnum[n["id"]] if n["id"] in idtopartnum else 9999 )
    return res

def inspectMW(iFilePath):
    likelyiLname = Path(iFilePath).stem
    if "FEMC" in likelyiLname or "FPL" in likelyiLname or "EAP" in likelyiLname or "TLM" in likelyiLname or likelyiLname in UNWANTED or "CUDL" in likelyiLname or "LULDC" in likelyiLname or "SBB" in likelyiLname:
        return
    #if "W12827" not in iFilePath:
    #    return
    model = ConjunctiveGraph()
    model.parse(str(iFilePath), format="trig")
    # if status != released, pass
    if (None,  ADM.status, BDA.StatusReleased) not in model:
        return
    if RICMODE and (None, ADM.restrictedInChina, True) in model:
        return
    likelywLname = likelyiLname[1:]
    wok = isWok(likelywLname)
    if not wok:
        print(wok)
        print("w not ok: "+likelywLname)
        return
    mw = BDR[likelyiLname]
    mwinfo = {}
    wainfo = None
    for _, _, wa in model.triples( (mw, BDO.instanceOf, None) ):
        _, _, waLname = NSM.compute_qname_strict(wa)
        wainfo = getWA(waLname, likelyiLname)
    for _, _, pnO in model.triples( (mw, BDO.publisherName, None) ):
        pn = str(pnO)
        if pnO.language == "bo-x-ewts":
            pn = str(converter.toUnicode(pnO))
        mwinfo["pn"] = pn
    for _, _, plO in model.triples( (mw, BDO.publisherLocation, None) ):
        pl = str(plO)
        if plO.language == "bo-x-ewts":
            pl = str(converter.toUnicode(plO))
        mwinfo["pl"] = pl
    for _, _, ptO in model.triples( (mw, BDO.printMethod, None) ):
        _, _, ptLname = NSM.compute_qname_strict(ptO)
        if ptLname not in PTLNAMETOAPP:
            print("error: %s not handled" % ptLname)
            continue
        ptForApp = PTLNAMETOAPP[ptLname]
        mwinfo["pt"] = ptForApp
    titles = getTibNames(mw, BDR.hasTitle, model, INDEXES["works"])
    if len(titles) != 0:
        mwinfo["title"] = titles
    pdate = ""
    for evt, _, _ in model.triples( (None, RDF.type, BDO.PublishedEvent) ):
        for _, p, o in model.triples( (evt, None, None) ):
            if p == BDO.onYear:
                pdate = str(o)
            elif p == BDO.notBefore:
                pdate = str(o)+"-"+pdate
            elif p == BDO.notAfter:
                pdate = pdate+str(o)
    if pdate:
        mwinfo["pd"] = pdate
    for _, _, wa in model.triples( (mw, BDO.instanceOf, None) ):
        _, _, waLname = NSM.compute_qname_strict(wa)
        wainfo = getWA(waLname, likelyiLname)
        if wainfo and len(wainfo) != 0:
            mwinfo["creator"] = list(wainfo)
    parts = getParts(mw, model, titles[0] if len(titles) else None)
    if len(parts) != 0:
        mwinfo["hasParts"] = True
        # write the root instance index:
        if len(titles) != 0:
            INDEXES["rititles"] = titles[0]
    else:
        parts = None
    return [mwinfo, parts]

CACHEDWINFO = {}

def isWok(wLname):
    if not OAMODE and not RICMODE:
        return True
    md5 = hashlib.md5(str.encode(wLname))
    two = md5.hexdigest()[:2]
    fpath = GITPATH+"iinstances/"+two+"/"+wLname+".trig"
    authors = set()
    model = ConjunctiveGraph()
    try:
        model.parse(str(fpath), format="trig")
    except:
        print("missing iinstance file %s" % fpath)
        return False
    if RICMODE and (None, ADM.restrictedInChina, True) in model:
        print("ric")
        return False
    if OAMODE and (None, ADM.access, BDA.AccessOpen) not in model:
        print("noa")
        return False
    return True

def getWA(waLname, mwLname):
    if waLname in CACHEDWINFO:
        return CACHEDWINFO[waLname]
    md5 = hashlib.md5(str.encode(waLname))
    two = md5.hexdigest()[:2]
    fpath = GITPATH+"works/"+two+"/"+waLname+".trig"
    authors = set()
    model = ConjunctiveGraph()
    try:
        model.parse(str(fpath), format="trig")
    except:
        print("missing work file %s" % fpath)
        return authors
    for aac, _, p in model.triples( (None, BDO.agent, None) ):
        for _, _, r in model.triples( (aac, BDO.role, None) ):
            if r == BDR.R0ER0019 or BDR.R0ER0025:
                _, _, pLname = NSM.compute_qname_strict(p)
                authors.add(pLname)
                if pLname not in CREATOROF:
                    CREATOROF[pLname] = []
                CREATOROF[pLname].append(mwLname)
    nbi = 0
    for _, _, i in model.triples( (None, BDO.workHasInstance, None) ):
        nbi += 1
    if nbi > 2:
        CACHEDWINFO[waLname] = authors
    return authors

def inspectPerson(pFname):
    likelypLname = Path(pFname).stem
    if not likelypLname in CREATOROF or 'TLM' in pFname:
        return
    model = ConjunctiveGraph()
    model.parse(pFname, format="trig")
    if RICMODE and (None, ADM.restrictedInChina, True) in model:
        return
    names = getTibNames(BDR[likelypLname], BDO.personName, model, INDEXES["persons"])
    bdate = ""
    ddate = ""
    for evt, _, _ in model.triples( (None, RDF.type, BDO.PersonBirth) ):
        for _, p, o in model.triples( (evt, None, None) ):
            if p == BDO.onYear:
                bdate = str(o)
            elif p == BDO.notBefore:
                bdate = str(o)+"-"+bdate
            elif p == BDO.notAfter:
                bdate = bdate+str(o)
    for evt, _, _ in model.triples( (None, RDF.type, BDO.PersonDeath) ):
        for _, p, o in model.triples( (evt, None, None) ):
            if p == BDO.onYear:
                ddate = str(o)
            elif p == BDO.notBefore:
                ddate = str(o)+"-"+ddate
            elif p == BDO.notAfter:
                ddate = ddate+str(o)
    res = {"name": names, "co": CREATOROF[likelypLname]}
    if bdate:
        res["b"] = bdate
    if ddate:
        res["d"] = ddate
    return res

MAXKEYSPERINDEX = 20000

NBDIGITS = 2

FILES = {
    "works": {},
    "persons": {},
    "workparts": {}
}

def getdigits(lname):
    md5 = hashlib.md5(str.encode(lname))
    return md5.hexdigest()[:NBDIGITS]

def saveData(t, lname, data):
    if NBDIGITS == 0:
        with open(OUTDIR+t+'/'+lname+'.json', 'w') as f:
                json.dump(data, f, ensure_ascii=False)
    digits = getdigits(lname)
    if digits not in FILES[t]:
        FILES[t][digits] = {}
    bucket = FILES[t][digits]
    bucket[lname] = data

def writeData():
    if NBDIGITS == 0:
        return
    for t in ["persons", "works", "workparts"]:
        for bucket in FILES[t]:
            with open(OUTDIR+t+'/'+bucket+'.json', 'w') as f:
                json.dump(FILES[t][bucket], f, ensure_ascii=False)

def main(mwrid=None):
    i = 0
    os.makedirs(OUTDIR+"persons/", exist_ok=True)
    os.makedirs(OUTDIR+"works/", exist_ok=True)
    os.makedirs(OUTDIR+"workparts/", exist_ok=True)
    l = sorted(glob.glob(GITPATH+'/instances/**/MW*.trig'))
    for fname in VERBMODE and tqdm(l) or l:
        likelyLname = Path(fname).stem
        infol = inspectMW(fname)
        if infol is None:
            continue
        mwinfo = infol[0]
        partsinfo = infol[1]
        saveData("works", likelyLname, mwinfo)
        if partsinfo:
            saveData("workparts", likelyLname, partsinfo)
        i += 1
        #if i > 300:
        #    break
    l = sorted(glob.glob(GITPATH+'/works/**/MW*.trig'))
    for fname in VERBMODE and tqdm(l) or l:
        likelyLname = Path(fname).stem
        infol = inspectMW(fname)
        if infol is None:
            continue
        mwinfo = infol[0]
        partsinfo = infol[1]
        saveData("works", likelyLname, mwinfo)
        if partsinfo:
            saveData("workparts", likelyLname, partsinfo)
        i += 1
    l = sorted(glob.glob(GITPATH+'/persons/**/P*.trig'))
    for fname in VERBMODE and tqdm(l) or l:
        pinfo = inspectPerson(fname)
        if pinfo is None or  len(pinfo) == 0:
            continue
        likelyLname = Path(fname).stem
        saveData("persons", likelyLname, pinfo)
        i += 1
        #break
    writeData()
    for idxname, idx in INDEXES.items():
        if idxname == "rititles":
            continue
        fileCnt = 0
        #towrite[name] = values
        fpath = OUTDIR+idxname+"-"+str(fileCnt)+".json"
        fp = open(fpath, 'w')
        keyCnt = 0
        for name, values in idx.items():
            if keyCnt == 0:
                fp.write('{')
            else:
                fp.write(',')
            fp.write(json.dumps(name, ensure_ascii=False)+':'+json.dumps(values, ensure_ascii=False))
            keyCnt += 1;
            if keyCnt > MAXKEYSPERINDEX:
                fileCnt += 1
                fp.write('}')
                fp.flush()
                fp.close()
                fpath = OUTDIR+idxname+"-"+str(fileCnt)+".json"
                fp = open(fpath, 'w')
                keyCnt = 0
        if keyCnt != 0:
            fp.write('}')
            fp.flush()
            fp.close()
    with open(OUTDIR+"rititles.json", 'w') as outfile:
        json.dump(INDEXES["rititles"], ,ensure_ascii=True)

def testPerson(prid):
    CREATOROF[prid] = True
    digits = getdigits(prid)
    fname = GITPATH+"/persons/"+digits+"/"+prid+".trig"
    print(fname)
    pinfo = inspectPerson(fname)
    print("info:")
    print(pinfo)
    for idxname, idx in INDEXES.items():
        keyCnt = 0
        s = ""
        for name, values in idx.items():
            if keyCnt == 0:
                s += '{'
            else:
                s += ','
            s += json.dumps(name, ensure_ascii=False)+':'+json.dumps(values, ensure_ascii=False)
            keyCnt += 1
        if keyCnt != 0:
            s += '}'
        print(idxname)
        print(s)

def testMW(prid):
    digits = getdigits(prid)
    fname = GITPATH+"/instances/"+digits+"/"+prid+".trig"
    print(fname)
    pinfo = inspectMW(fname)
    print("info:")
    print(pinfo)
    for idxname, idx in INDEXES.items():
        if idxname == ""
        keyCnt = 0
        s = ""
        for name, values in idx.items():
            if keyCnt == 0:
                s += '{'
            else:
                s += ','
            s += json.dumps(name, ensure_ascii=False)+':'+json.dumps(values, ensure_ascii=False)
            keyCnt += 1
        if keyCnt != 0:
            s += '}'
        print(idxname)
        print(s)

main()
#testPerson("P3379")
#testMW("MW4CZ7445")