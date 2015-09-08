#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = 'luisangel'


import os
import json
import unicodedata

from location_cleaner import *
from pygeocoder import Geocoder
from pygeocoder import GeocoderError


import sys

GEO_NOM = ['scl', 'chile', 'cl', 'chl']

REGIONES = {'2':'Antofagasta',
            '3': 'Atacama',
            '4': 'Coquimbo',
            '10': 'Los Lagos',
            '7': 'Maule',
            '11': 'Aysen',
            '15':'Arica y Parinacota',
            '9':'Araucania',
            '14':'Los Rios',
            '12':'Magallanes',
            '1':'Tarapaca',
            '5':'Valparaiso',
            '8':'Biobio',
            '6':'O\'Higgins',
            '13':'RM Santiago'
            }
###Metodo
def elimina_tildes(s=''):
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))

###Metodo
def clean_string(text=''):
    text = text.lower()
    text = elimina_tildes(text.decode('utf-8'))
    return text

def is_location(data = {}):

    if data['location'] is None:
        data['chile'] = False
        return data

    location = data['location']
    ubicacion = u''.join(location).encode('utf-8').strip()

    ubicacion = clean_string(ubicacion)

    if len(ubicacion.strip()) == 0:
        data['chile'] = False
        return data

    #=============Coordenada====

    if (hasCoordinates(ubicacion) is True):
        try:
            geo = cleanLocationField(ubicacion)
            location = Geocoder.reverse_geocode(geo.getLatitude(), geo.getLongitude()).__str__()
            ubicacion = clean_string(location)
            data['location'] = ubicacion
        except GeocoderError:
            data['chile'] = False
            return data


    #=============Comuna======
    fcomunas = os.path.join('chile', 'ComunaProvincia.json')
    comunas = json.loads(file(fcomunas).read())

    for clave, valor in comunas.items():
        for nam in valor:
            va = str(nam['name']).lower().strip()
            if ubicacion.find(va) >= 0:
                data['chile'] = True
                data['region']=REGIONES[clave[:len(clave)-1]]
                return data

    #=============Provincia======

    fprovincia = os.path.join('chile', 'ProvinciaRegion.json')
    provincia = json.loads(file(fprovincia).read())

    for clave, valor in provincia.items():
        for nam in valor:
            va = str(nam['name']).lower().strip()
            if ubicacion.find(va) >= 0:
                data['chile'] = True
                data['region']=REGIONES[clave]
                return data


    #=============Region======

    fregiones = os.path.join('chile', 'Regiones.json')
    regiones = json.loads(file(fregiones).read())

    for ob in regiones:
        va = str(ob['name']).lower().strip()
        if ubicacion.find(va) >= 0:
            data['chile'] = True
            data['region']=REGIONES[ob['region_id']]
            return data

    #=========== Otros ======


    for linea in GEO_NOM:
        if ubicacion.find(linea) >= 0:
            data['chile'] = True
            data['region']="Chile"
            return data

    data['chile'] = False
    return data

