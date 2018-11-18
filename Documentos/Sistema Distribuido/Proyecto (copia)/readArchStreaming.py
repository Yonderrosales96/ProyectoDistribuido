import threading
import geoip2.database
from multiprocessing import Pool
import time
import queue



#******************************
def getCountry (ip):
    try:
        reader = geoip2.database.Reader('./GeoLite2-City_20181113/GeoLite2-City.mmdb')
        response = reader.city(ip)
        reader.close()
        return response.country.name
    except:
        return "Desconocido"

def getCountryAndCity (ip,dictCountry,dictCity):
    try:
        reader = geoip2.database.Reader('./GeoLite2-City_20181113/GeoLite2-City.mmdb')
        response = reader.city(ip)
        reader.close()
        dictCountry.setdefault(response.country.name,1)
        dictCity.setdefault(response.city.name,1)
    except:
        dictCountry.setdefault("Desconocido",1)

def getIp (cadena,protocol):
    if protocol == "qtp5":
        ipBegin = cadena.find("ip=")
        ipEnd = cadena.find(";",ipBegin)
        return cadena[ipBegin+3:ipEnd]
    else:
        ipBegin = cadena.find("oip=")
        ipEnd = cadena.find(";",ipBegin)
        return cadena[ipBegin+4:ipEnd]

def getProtocol (cadena):
    return cadena[1:5]

def getHora (cadena):
    endHora = cadena.find(",")
    # return cadena[0:endHora]
    return cadena[0:2]

def getEmail(cadena):
    beginEmail=cadena.find("=")+1
    endEmail=cadena.find(";")
    return cadena[beginEmail:endEmail]

def updateDictIp(ip,dictIp):
    val={}
    val[ip] = dictIp.setdefault(ip,0)+1
    dictIp.update(val)

def updateDictEmail (email,dictEmail):
    val={}
    val[email] = dictEmail.setdefault(email,0)+1
    dictEmail.update(val)

def updateDictHora (hora,dictHora):
    val={}
    val[hora] = dictHora.setdefault(hora,0)+1
    dictHora.update(val)

def processLine(l,dictIp,dictCountry,dictCity,dictEmail,dictHora):
    line = l.split(" ")
    protocol = getProtocol(line[4])
    ip = getIp(line[5],protocol)
    updateDictIp(ip,dictIp)
    getCountryAndCity(ip,dictCountry,dictCity)
    # country = getCountry(ip)
    # dictCountry.setdefault(country,1)
    email= getEmail(line[9])
    updateDictEmail(email,dictEmail)
    # dictEmail.setdefault(email,1)
    hora = getHora(line[1])
    updateDictHora(hora,dictHora)
    # dictHora.setdefault(hora,1)
#******************************


def readArch (buffer,evn):
    with open ("data","r") as f:
        for line in f:
            buffer.put(line)
    evn.set()


def processArch (buffer,evn,dictIp,dictCountry,dictCity,dictEmail,dictHora):
    cont = 0

    while not evn.is_set() or not buffer.empty():
        if not buffer.empty():
            l = buffer.get()
            cont+=1
            processLine(l,dictIp,dictCountry,dictCity,dictEmail,dictHora)
            # print (line)
    print ("lineas procesadas: ", cont)
def main ():
    #threads = list()
    # num = list()
    dictIp = {}
    dictCountry = {}
    dictCity = {}
    dictHora = {}
    dictEmail = {}
    buffer = queue.Queue()
    evn= threading.Event()
    t1 = threading.Thread(target=readArch,args=(buffer,evn,))
    t2 = threading.Thread(target=processArch,args=(buffer,evn,dictIp,dictCountry,dictCity,dictEmail,dictHora,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    # print(dictIp)
    # print(dictCountry)
    # print(dictCity)
    # print(dictEmail)
    print(dictHora)

tInicio = time.time()
main()
tFin = time.time()
print("tiempo de ejecucion: ",tFin - tInicio)


    # while True:
        # if len(num) < 20:
        # if num.qsize( )  < 20:
            # num.append(getNumRando)
            # num.put(getNumRando())
            # time.sleep(2)
        # else:
        #     time.sleep(2)
