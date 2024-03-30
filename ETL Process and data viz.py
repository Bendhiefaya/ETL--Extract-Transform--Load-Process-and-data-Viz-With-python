# -*- coding: utf-8 -*-
"""
Created on Sat Dec 10 07:02:14 2022

@author: bendh
"""
import psycopg2
import pygrametl
from pygrametl.datasources import CSVSource, TransformingSource, FilteringSource
from pygrametl.tables import Dimension, FactTable
import string
import re
import matplotlib.pyplot as plt
#connexion to database

db = psycopg2.connect(dbname='LPEBI',user='postgres', password='58382297')
c=db.cursor() #curseur bd
"""
c.execute('create schema ProjetLPEBI') #requete creation schema
#creation des tables :""
c.execute('CREATE TABLE ProjetLPEBI.Localisation(idCommune varchar PRIMARY KEY,CodePostal int, nomCommune varchar(100),nomRue varchar)')
c.execute('CREATE TABLE ProjetLPEBI.contactInfo(idContact varchar PRIMARY KEY,email varchar,telephone varchar,siteweb varchar)')

c.execute('CREATE TABLE ProjetLPEBI.infoComplementaire(id_INFO varchar PRIMARY KEY,information varchar)')
c.execute('CREATE TABLE ProjetLPEBI.logement(idLogement varchar PRIMARY KEY, TarifLoge int ,type varchar )')

c.execute('CREATE TABLE ProjetLPEBI.service(idSERVICE varchar PRIMARY KEY, Tarifservice int ,type varchar)')
c.execute('CREATE TABLE ProjetLPEBI.date(id_Date varchar PRIMARY KEY,dateDebut date ,dateFin date)')

c.execute('CREATE TABLE ProjetLPEBI.Hebergement(idCommune varchar references ProjetLPEBI.Localisation(idCommune),idContact varchar references ProjetLPEBI.contactInfo(idContact), id_INFO varchar references ProjetLPEBI.infoComplementaire(id_INFO),idSERVICE varchar references ProjetLPEBI.service(idSERVICE),idLogement varchar references ProjetLPEBI.logement(idLogement),id_Date varchar references ProjetLPEBI.date(id_Date),ID_hebergement varchar PRIMARY KEY,capaciteNBRpersonne int,Classement varchar,NomOffre varchar,nombreSemaineHebergement float)')

db.commit()#execution des requetes
"""
connection = pygrametl.ConnectionWrapper(db)
connection.setasdefault()
connection.execute('set search_path to ProjetLPEBI ')
service_dimension = Dimension(
    name='service',
    key='idSERVICE',
    attributes=['Tarifservice','type']
    )
date_dimension = Dimension(
    name='date',
    key='id_Date',
    attributes=['dateDebut','dateFin']
    )
Localisation_dimension = Dimension(
    name='Localisation',
    key='idCommune',
    attributes=['nomCommune','CodePostal', 'nomRue']
    )
contactInfo_dimension = Dimension(
    name='contactInfo',
    key='idContact',
    attributes=['email', 'telephone','siteweb']
    )
infoComplementaire_dimension = Dimension(
    name='infoComplementaire',
    key='id_INFO',
    attributes=['information']
    )
logement_dimension = Dimension(
    name='logement',
    key='idLogement',
    attributes=['TarifLoge','type']
    )
hebergement_Fact = FactTable(
    name='Hebergement',
    keyrefs=['ID_hebergement','idCommune', 'idContact', 'id_INFO','idLogement','Classement','NomOffre','idSERVICE','id_Date',],
    measures=['capaciteNBRpersonne','nombreSemaineHebergement'])
#ouverture et lecture de fichier csv 
source = open('Hébergements_touristiques.csv', 'r', 150)
data = CSVSource(source, delimiter=',')
print('lecture du fichier réussi')
#fonction de filtrage
def filtrer(data):
    if data["SITE_WEB"] != "":
        return True
    else:
        data["SITE_WEB"] ='no website'
        return False
    if data["TEL"] != "":
        return True
    else:
        data["TEL"] ='no Phone'
        return False
dataFilt = FilteringSource(source=data, filter=filtrer)   
#Transformation de données 
def TransformingNbSemaine(data):
    Nombre=int(data['Nombre_Semaine_Heberge'])//1200
    data['Nombre_Semaine_Heberge']=Nombre
    #print(data['Nombre_Semaine_Heberge'])
#testFonctionTransformingNbSemaine
#for i in data :
#import unicodedata

def transformationNomOffre(data):
        donnee=data['NOM_OFFRE']
        removeSpecialChars = donnee.translate({ord("Ã"):"e"})
        removeSpecialChars2 = removeSpecialChars.translate ({ord(c): "" for c in "¯Â¿Â½"})      
        #donnee.replace('%C3%AF%C2%BF%C2%BD','e')
        #unicodedata.normalize('NFD', donnee).encode('ascii', 'ignore')
        data['NOM_OFFRE']=removeSpecialChars2
        #print(data['NOM_OFFRE'])
        
def transformationInfoCompl(data):
        donnee=data['INFOS_COMPLEMENTAIRES']
        removeSpecialChars = donnee.translate({ord("Ã"):"e"})
        removeSpecialChars2 = removeSpecialChars.translate ({ord(c): "" for c in "¯Â¿Â½"})      
        data['INFOS_COMPLEMENTAIRES']=removeSpecialChars2
        print(data['INFOS_COMPLEMENTAIRES'])        
def transformationRue(data):
        donnee=data['RUE']
        removeSpecialChars = donnee.translate({ord("Ã"):"e"})
        removeSpecialChars2 = removeSpecialChars.translate ({ord(c): "" for c in "¯Â¿Â½"})      
        #donnee.replace('%C3%AF%C2%BF%C2%BD','e')
        #unicodedata.normalize('NFD', donnee).encode('ascii', 'ignore')
        data['RUE']=removeSpecialChars2
        print(data['RUE'])         
def transformPhone(data):
    phone=data['TEL'].replace('#','\n')
    data['TEL']=phone
    print(data['TEL'])
def transformwebsite(data):
    phone=data['SITE_WEB'].replace('#','\n')
    data['SITE']=phone
    print(data['SITE'])
def transformdate(data):
    date=data['Date']
    l=len(date)
    newdate=''
    for i in range(0,l-5):
        newdate=newdate +date[i]
    newdate=newdate.replace(' ','')
    data['Date']=newdate
    print(data['Date'])
"""for i in data :
    x=transformdate(i)"""
#for i in data :
   # x=transformationInfoCompl(i)

dataTransformsemaine = TransformingSource(dataFilt,TransformingNbSemaine)
dataTransformphone=TransformingSource(dataTransformsemaine,transformPhone)
dataTransformRue=TransformingSource(dataTransformphone,transformationRue)
dataTransformOffrename=TransformingSource(dataTransformRue,transformationNomOffre)
dataTransforminfoComplem=TransformingSource(dataTransformOffrename,transformationInfoCompl)
dataTransformweb=TransformingSource(dataTransforminfoComplem,transformwebsite)
dataF=TransformingSource(dataTransformweb,transformdate)
#for i in dataTransforminfoComplem:
    #print(i)
#chargement dimension contactInfo_dimension
"""
CompteurLigne=0
i={}
for row in dataF:
    CompteurLigne+=1
    i["idContact"]=row['Id']
    i["email"]=row["MaiL"]
    i["telephone"]=row["TEL"]
    i["siteweb"]=row["SITE_WEB"]
    contactInfo_dimension.insert(i)
connection.commit()
connection.close()

#chargement dimension 
CompteurLigne=0
i={}
for row in dataF:
    CompteurLigne+=1
    i["idSERVICE"]=row['Id']
    if 'week-end' in row["Tarif_SERVICES"]:
      i['type'] ='tarif par week-end'
      x=row["Tarif_SERVICES"].replace('00| # Tarif service par week-end|','')
      i['Tarifservice']=int(x)
    elif 'semaine' in row["Tarif_SERVICES"]:
      i["type"] ='tarif par semaine'
      x=row["Tarif_SERVICES"].replace('00| # Tarif service par semaine|','')
      i['Tarifservice']=int(x)
    else:
      i['type'] ='no type'
      i['Tarifservice']=0
    service_dimension.insert(i) """
"""
#chargement dimension logement
CompteurLigne=0
i={}
for row in dataF:
    CompteurLigne+=1
    i["idLogement"]=row['Id']
    if 'week-end' in row["Tarif_logement"]:
      i['type'] ='tarif par week-end'
      x=row["Tarif_logement"].replace('Tarif logement par week-end|','')
      #result=my_list[:my_list.find('week-end|')]
      i['TarifLoge']=int(x) 
    elif 'semaine' in row["Tarif_logement"]:
      i['type'] ='tarif par semaine'
      x=row["Tarif_logement"].replace('Tarif logement par semaine|','')
      i['TarifLoge']=int(x)
    else:
      i['type'] ='no type'
      i['TarifLoge']=0
    logement_dimension.insert(i)
#chargement dimension date

CompteurLigne=0
i={}
dates1=[]
#from datetime import datetime
for row in dataF:
    CompteurLigne+=1
    i["id_Date"]=row['Id']
    dates=row['Date'].split('|')
    dates[0].replace('/','-')
    dates[1].replace('/','-')
    #i['dateDebut']=datetime.strptime(dates[0],'%d-%m-%y').date()
    #i['dateFin']=datetime.strptime(dates[1],'%d-%m-%y').date()
    i['dateDebut']=dates[0]
    i['dateFin']=dates[1]
    date_dimension.insert(i)
#chargement dimension  infoComp
i={}
CompteurLigne=0
for row in dataF:
    CompteurLigne+=1
    i["id_INFO"]=row['Id']
    listeinfo=row['INFOS_COMPLEMENTAIRES'].split('#')
          
    i["information"]=listeinfo
    infoComplementaire_dimension.insert(i)
i={}
CompteurLigne=0
for row in dataF:
    CompteurLigne+=1
    i['idCommune']=row['Id']
    i["CodePostal"]=row["CODE_POSTAL"]
    i['nomCommune']=row['COMMUNE']
    i['nomRue']=row['RUE']
    Localisation_dimension.insert(i)
    
"""
"""
i={}
CompteurLigne=0
for row in dataF:
    CompteurLigne+=1
    i["ID_hebergement"]=row["Id"]
    i['capaciteNBRpersonne']=int(row['CAPACITE_NBRE_PERS'])
    i['Classement']=row['CLASSEMENT']
    i['NomOffre']=row['NOM_OFFRE']
    i['idCommune']=row['Id']
    i['id_INFO']=row['Id']
    i['id_Date']=row['Id']
    i["idLogement"]=row['Id']
    i["idSERVICE"]=row['Id']
    i["idContact"]=row['Id']
    i['nombreSemaineHebergement']=row['Nombre_Semaine_Heberge']
    hebergement_Fact.insert(i)
"""

#connection.commit()
#connection.close()
# VISUALISATION EN SECTEUR
c.execute("SELECT SUM(nombreSemaineHebergement) from Hebergement GROUP BY NomOffre")
 #import datetime
db.commit()
capacity=c.fetchall()
c.execute('SELECT DISTINCT NomOffre from ProjetLPEBI.Hebergement' )
db.commit()
nom=c.fetchall()
#c.execute("SELECT dateDebut from Hebergement INNER JOIN ProjetLPEBI.date\
  # ON Hebergement.ID_hebergement = date.id_Date ")
#db.commit()
#days=c.fetchall()
cap=[]
print(cap)
name=[]
for i in capacity:
    for j in i:  
      cap.append(j)
      print(j)
for i in nom:
    for j in i: 
      name.append(j)
      print(j)
capi=cap
n=name

plt.pie(capi)

plt.title('Nombre de semaine hebergement total pour chaque Offre')
plt.legend(n,loc ='center right')
plt.show()

# VISUALISATION EN SECTEUR
c.execute("SELECT SUM(capaciteNBRpersonne) from Hebergement GROUP BY NomOffre")
 #import datetime
db.commit()
capacity=c.fetchall()
c.execute("SELECT NomOffre from ProjetLPEBI.Hebergement WHERE NomOffre ='Village de Vacances VVF Villages Iparla'" )
db.commit()
nom=c.fetchall()
c.execute("SELECT NomOffre from ProjetLPEBI.Hebergement WHERE NomOffre ='Village Club Ceveo Les Dunes'" )
db.commit()
nom2=c.fetchall()
c.execute("SELECT NomOffre from ProjetLPEBI.Hebergement WHERE NomOffre ='Le Carrefour des Landes'" )
db.commit()
nom3=c.fetchall()
c.execute("SELECT SUM(TarifLoge) from Hebergement INNER JOIN ProjetLPEBI.logement\
   ON Hebergement.ID_hebergement = logement.idLogement WHERE NomOffre ='Village de Vacances VVF Villages Iparla'")
db.commit()
tarif3=c.fetchall()
c.execute("SELECT SUM(TarifLoge) from Hebergement INNER JOIN ProjetLPEBI.logement\
   ON Hebergement.ID_hebergement = logement.idLogement  WHERE NomOffre ='Village Club Ceveo Les Dunes'")
db.commit()
tarif2=c.fetchall()
c.execute("SELECT SUM(TarifLoge) from Hebergement INNER JOIN ProjetLPEBI.logement\
   ON Hebergement.ID_hebergement = logement.idLogement WHERE NomOffre ='Le Carrefour des Landes'")
db.commit()
tarif=c.fetchall()
for i in tarif:
    for j in i :
        
        tar1=j   
for i in tarif2:
    for j in i :
        
        tar2=j 
for i in tarif3:
    for j in i :
        tar3=j 
for i in nom:
    for j in i :
        n=j
for i in nom2:
    for j in i :
        n1=j
for i in nom3:
    for j in i :
        n3=j
tarifs=[tarif,tarif2,tarif3]
names=[n,n1,n3]
plt.title('Tarif total par Offre pour les offres Le Carrefour des Landes \n ,Village Club Ceveo Les Dunes et Village de Vacances VVF Villages Iparla')
plt.stem(names,tarifs)
#plt.legend(names,loc ='center right')
plt.show()


#visualisation bar 
c.execute("SELECT COUNT(ID_hebergement) FROM Hebergement WHERE Classement ='3 Ã©toiles'")
db.commit()
nb3=c.fetchall()
for i in nb3:
    for j in i :        
        nomb3=j
#print(nb3)
c.execute("SELECT COUNT(ID_hebergement) FROM Hebergement WHERE Classement ='2 Ã©toiles'")
db.commit()
nb2=c.fetchall()
for i in nb2:
    for j in i :
        nomb2=j
c.execute("SELECT COUNT(ID_hebergement) FROM Hebergement WHERE Classement ='1 Ã©toiles'")
db.commit()
nb1=c.fetchall()
for i in nb1:
    for j in i : 
        nomb1=j
classement=['1 etoile','2 etoile','3 etoile']
nombre=[nomb1,nomb2,nomb3]
plt.title('nombre d hotel par classement')
plt.bar(classement,nombre)
plt.show()