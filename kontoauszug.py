#!/usr/bin/env python3
# coding: utf-8

'''
Title: Bank - Statement - Workflow
Filename: kontoauszug.py
Purpose:
Consolidation / Categorization / Pivot Table Creation from download bank statement files (.csv)
Developed for "Deutsche Bank" bank statements, might be adjustable for other banks
Operational description:
We have a .csv - file containing one account activity per line, named "Kontoumsaetze_bbb_aaaaaaass_Alle.csv", where
bbb = subsidiary (Filiale in German)
aaaaaaa = account no (Kontonummer in German)
ss = sub account no (Unterkontonummer in German)
Example:
Buchungstag;Wert;Umsatzart;Begünstigter / Auftraggeber;Verwendungszweck;IBAN;BIC;Kundenreferenz;Mandatsreferenz ; # line continues!
Gläubiger ID;Fremde Gebühren;Betrag;Abweichender Empfänger;Anzahl der Aufträge;Anzahl der Schecks;Soll;Haben;Währung
07.12.2017;07.12.2017;SEPA-Überweisung an;xyz Corp.;Payment for abc;DE12345678901234567800;ANNNNNNNNNZ;;;;;;;;;-999,99;;EUR
01.12.2017;01.12.2017;SEPA-Gutschrift von;Meier and Comp.;Salary;DE23456789012345678901;BMMMMMMMXXX;;;;;;;;;;47,11;EUR

This file collects all bank statements, i.e., after this program has read and consolidated a new file, the above file
will be updated.

Now we download a new .csv file from our bank named "Kontoumsaetze_bbb_aaaaaaass_yyyymmdd_hhmmss.csv", where
bbb_aaaaaaass is the same as above and yyyymmdd_hhmmss is obvious.

This file has 4 lines in the beginning and 1 line in the end which do not fit into the column schemeand will be ignored.

This program reads this file, joins it to the first mentioned data, removes duplicates, and updates "Kontoumsaetze_bbb_aaaaaaass_Alle.csv"
Moreover, this program reads 'KatMapss.json', where again "ss" stands for subaccount no.
This .json file determines the catigorization of bookings, and this works as follows:
The fields "Umsatzart;Begünstigter / Auftraggeber;Verwendungszweck" (= booking type;sender or receiver;booking text)
are concatenated. If one of the keywords in katmap is found in the concatenation, the booking is marked with the category
and booking direction belonging to the keyword.
Caveat: This program does not check whether a keyword occurs more than once.
Also the categorized data is saved ("Kontoumsaetze_bbb_aaaaaaass_Kategorized.csv") so that you can read them with
a spreadsheet program like libre office calc.
The categorization is always created anew, so if you change 'KatMapss.json', also the past data is newly categorized.
As a final step, this program creates 'PivotTabless.csv', which shows the categories on the vertical axis and Year/Month horizontally.

During the program run, a log file 'kontoauszug.log' is created.

Usage: python3 kontoauszug.py fname='Kontoumsaetze_bbb_aaaaaaass_yyyymmdd_hhmmss.csv' \
                              pname='/home/myname/development/Kontoauszuege/'
(More parameters are described in the source code below, they are related to logging and you can keep them alone)

#TODO: (optionally) create "configss.json". If found, take command line parameters from there. Command reduced to 'ss', e.g. '00'
#TODO: (optionally) use pprint for logging data structures

Copyright: Jürgen Diel 2017
License: GPL


    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

    Dieses Programm ist Freie Software: Sie können es unter den Bedingungen
    der GNU General Public License, wie von der Free Software Foundation,
    Version 3 der Lizenz oder (nach Ihrer Wahl) jeder neueren
    veröffentlichten Version, weiterverbreiten und/oder modifizieren.

    Dieses Programm wird in der Hoffnung, dass es nützlich sein wird, aber
    OHNE JEDE GEWÄHRLEISTUNG, bereitgestellt; sogar ohne die implizite
    Gewährleistung der MARKTFÄHIGKEIT oder EIGNUNG FÜR EINEN BESTIMMTEN ZWECK.
    Siehe die GNU General Public License für weitere Details.

    Sie sollten eine Kopie der GNU General Public License zusammen mit diesem
    Programm erhalten haben. Wenn nicht, siehe <http://www.gnu.org/licenses/>.

'''

import pandas as pd
import begin
import logging
import json
from pathlib import Path
from os import remove

loglevelmap = {'CRITICAL' : logging.CRITICAL, 'ERROR' : logging.ERROR, 'WARNING' : logging.WARNING,
               'INFO' : logging.INFO, 'DEBUG' : logging.DEBUG}

def verification(myktodf,myverifyfullname):
    """Verification and logging"""
    logging.info('myverifyfullname = ' + myverifyfullname)
    mynumrows, mynumcols = myktodf.shape
    logging.info(f'Zeilen = {mynumrows} | Spalten = {mynumcols}')
    logging.info('Überschriften' + repr(myktodf.columns))
    sso, sha = myktodf ['Soll'].sum(), myktodf ['Haben'].sum()
    logging.info(f'Summe Soll = {sso:10.2f} | Summe Haben = {sha:10.2f}')
    btmin, btmax = myktodf ["Buchungstag"].min(),myktodf ["Buchungstag"].max()
    logging.info(f'Erster Buch.-tag = {btmin} | Letzter Buch.-tag = {btmax}')
    BookingSumsDF = myktodf.groupby('Buchungstag').sum() # sum per booking day
    BookingSumsDF['Buchungstag'] = BookingSumsDF.index
    BookingSumsDF.set_index('Buchungstag')
    VerifyDF = BookingSumsDF[['Buchungstag' ,'Soll' ,'Haben']] #This is created for debugging purposes.
    VerifyDF.to_csv(myverifyfullname, encoding='latin_1',
                    sep=';' ,decimal=',', index = False, date_format='%d.%m.%Y')
    logging.info('-' * 40)

def getkto_df(fn, skiprowsandfooter=True):
    """Kontodaten einlesen"""
    logging.info('inside getkto_df')
    fnbase, fnext = fn.split('.')
    pathparts = fnbase.split('/')
    logging.info(pathparts)
    fnonly = pathparts[-1]
    logging.info(fnonly)
    mypath = ''.join([x + '/' for x in pathparts[:-1]])
    logging.info(mypath)
    if skiprowsandfooter:
        leadin, filiale, kto, mydate, mytime = fnonly.split('_')
        #logging.info(leadin, filiale, kto, mydate, mytime)
    else:
        leadin, filiale, kto, alle = fnonly.split('_')
        #logging.info(leadin, filiale, kto, alle)
    rowstoskip = 4 if skiprowsandfooter else 0
    footertoskip = 1 if skiprowsandfooter else 0

    myktodf = pd.read_csv(fn, skiprows=rowstoskip, skipfooter=footertoskip, #prüfen auf richtige werte
                          encoding='latin_1', delimiter=';', decimal=',', thousands='.',
                          parse_dates=[0, 1], dayfirst=True, infer_datetime_format=True,
                          engine='python')  # Daten einlesen
    myverifyfullname = mypath + f'verify_{filiale}_{kto}_' + (f'{mydate}_{mytime}' if skiprowsandfooter else 'Alle') + '.csv'
    verification(myktodf, myverifyfullname)
    return myktodf

def categorize(crit,KatMap):
    """This function tries to find a keyword in crit and returns the corresponding category"""
    found = False
    for myindex in KatMap:
        for mykeyword in KatMap[myindex]['Stichworte']:
            if mykeyword in crit:
                return myindex
    if not found:
        return 'no cat.'

def getKatMap(pname, ukto):
    """This function tries to read 'KatMapss.json'. If not found, default (example!!!) is applied.
    Caveat: The Python json module is quite picky regarding acceptable formatting. true/false must be lower case,
    "," after last entry is not accepted, maybe more...
    """
    katmapname = pname + 'KatMap' + ukto + '.json'
    try:
        with open(katmapname) as data_file:
            katmap = json.load(data_file)
            logging.info('Loaded katmap from file ' + katmapname)
    except:
        if ukto == '00':
            katmap = {'Repairs': {'Einnahme': False, 'Stichworte': ['Schlüsseldienst', 'Plumber', 'Electrician']},
                      'Grundsteuer': {'Einnahme': False, 'Stichworte': ['GRUNDBESITZABGABE', 'Grundbesitzabgaben']},
                      'KommunikationVerkehr': {'Einnahme': False, 'Stichworte':
                          ['Posteo','Rundfunk ARD, ZDF, DRadio']},
                      'BeiträgeSpenden': {'Einnahme': False,
                                          'Stichworte': ['Kindergarten', 'Spende', 'Wikimedia Deutschland e.V.',
                                                         'GIORDANO BRUNO STIFTUNG']},
                      'Gehalt': {'Einnahme': True, 'Stichworte': ['Salary', 'Pension']}
                      #lots more....
                      }

            logging.info('katmap created from default')

        with open(katmapname, 'w', encoding='utf8') as fp:
            json.dump(katmap, fp, sort_keys=True, indent=4, ensure_ascii=False)
    return katmap

@begin.start(auto_convert = True) # read this: http://begins.readthedocs.io/en/latest/
def main(fname: 'Dateiname der zuzufügenden Datei' = 'Kontoumsaetze_bbb_aaaaaaass_yyyymmdd_hhmmss.csv',
         pname: 'Arbeitsverzeichnis' = '/home/myname/Development/Kontoauszuege/',
         anfangskontostand: 'Anfangskontostand' = 0.0,
         freshlogfile: 'Delete logfile before start' = True,
         loglevel: 'Loglevel' = 'INFO'):

    # Logging initialization follows
    loglevel = loglevel.upper()
    if loglevel not in loglevelmap.keys(): #['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']:
        print("loglevel must be one of 'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'")
        exit()
    if freshlogfile:
        #my_file = pname + 'kontoauszug.log'
        my_file = Path(pname + 'kontoauszug.log')
        if my_file.is_file():
            remove(pname + 'kontoauszug.log')
    logging.basicConfig(filename = pname + 'kontoauszug.log', format='%(asctime)s |%(levelname)s| %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',  level = loglevelmap[loglevel])
    loglevel = loglevel.upper()
    if loglevel not in loglevelmap.keys(): #['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']:
        print("loglevel must be one of 'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'")
        exit()
    if freshlogfile:
        my_file = Path('kontoauszug.log')
        if my_file.is_file():
            remove('kontoauszug.log')
    logging.basicConfig(filename = 'ICaltoCALDAV.log', format='%(asctime)s |%(levelname)s| %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',  level = loglevelmap[loglevel])
    logging.info ('Parameters:')
    logging.info ('Arbeitsverzeichnis = ' + pname)
    logging.info ('Dateiname der zuzufügenden Datei = ' + fname)

    # Splitting of file name follows --> leadin, filiale, bkto, ukto, mydate, mytime, fnext
    fnbase, fnext = fname.split('.')
    leadin, filiale, kto, mydate, mytime = fnbase.split('_')
    bkto, ukto = kto[:-2], kto[-2:]
    mybasefname = f'{leadin}_{filiale}_{kto}_Alle.csv'

    myKtoAlleDF = getkto_df(pname + mybasefname,False) # read "old" bank statements
    Alle_numrows, Alle_numcols = myKtoAlleDF.shape
    logging.info ('myKtoAlleDF read from ' + pname + mybasefname)

    myKtoNeuDF = getkto_df(pname + fname,True) # read bank statements to add
    Neu_numrows, Neu_numcols = myKtoNeuDF.shape
    logging.info ('myKtoNeuDF read from ' + pname + fname)

    # append new statements to old statements
    myKtoCombiDF = myKtoAlleDF.append(myKtoNeuDF, ignore_index = True, verify_integrity = True) # Neue Daten in alte integrieren
    Combi_numrows, Combi_numcols = myKtoCombiDF.shape
    logging.info (f'Combi_numrows = {Combi_numrows} | Alle_numrows = {Alle_numrows} | Neu_numrows = {Neu_numrows}')
    assert Combi_numrows == Alle_numrows + Neu_numrows, f"Alle_numrows + Neu_numrows != Combi_numrows, {Alle_numrows} + {Neu_numrows} != {Combi_numrows}"
    assert Combi_numcols == Alle_numcols, f"Combi_numcols != Alle_numcols, {Combi_numcols} != {Alle_numcols}"
    assert Combi_numcols == Neu_numcols, f"Combi_numcols != Neu_numcols, {Combi_numcols} != {Neu_numcols}"


    # remove duplicates
    myKtoCleanDF = myKtoCombiDF.drop_duplicates(['Buchungstag', 'Wert', 'Umsatzart', 'Begünstigter / Auftraggeber',
                                                 'Verwendungszweck', 'IBAN', 'BIC', 'Gläubiger ID',
                                                 'Soll', 'Haben', 'Währung'])
    myKtoCleanDF.sort_values(by='Buchungstag', ascending=False, inplace=True)
    verification(myKtoCleanDF, pname + mybasefname)

    # sample check using one day in the overlap // left here if further debugging is needed
    # Beispieltag = myKtoCleanDF[myKtoCleanDF['Buchungstag'] == '2017-06-12']
    # [['Buchungstag' ,'Wert', 'Umsatzart', 'Begünstigter / Auftraggeber', 'Verwendungszweck', 'IBAN', 'BIC',
    #   'Soll', 'Haben', 'Währung']]
    #logging.info ('Beispieltag = ' + repr(Beispieltag))

    # Now we update "Kontoumsaetze_bbb_aaaaaaass_Alle.csv"
    myKtoCleanDF.to_csv(pname + mybasefname, encoding='latin_1', sep=';' ,decimal=',', index = False, date_format='%d.%m.%Y')

    # now, categorization starts
    KatMap = getKatMap(pname, ukto)
    logging.info('KatMap = ' + repr(KatMap))

    # for debugging:
    # if ukto == '00':
    #     begriffe = 'SEPA-Überweisung an | Zoologischer Garten | Spende ...'
    #     assert 'BeiträgeSpenden' == categorize(begriffe,KatMap), "categorization example failed"


    # Spalte "Aus_Ein" erzeugen / create column Debit vs. Credit
    myKtoCleanDF['Aus_Ein'] = myKtoCleanDF.apply(lambda row: 'Ausgabe' if pd.notnull(row.Soll) else 'Einnahme', axis=1)
    # Spalte "Kriterienquellen" erzeugen / create column source for categorization
    myKtoCleanDF['Kriterienquellen'] = myKtoCleanDF.apply(
        lambda row: '{} | {} | {}'.format(row.Umsatzart, row['Begünstigter / Auftraggeber'], row.Verwendungszweck), axis=1)
    # Spalte "Kategorie" erzeugen / create column category
    myKtoCleanDF['Kategorie'] = myKtoCleanDF.apply(lambda row: categorize(row.Kriterienquellen,KatMap), axis=1)
    # Spalte "JahrMonat" erzeugen / create column Year/Month
    myKtoCleanDF['JahrMonat'] = myKtoCleanDF['Buchungstag'].apply(lambda x: str(x.year) + '/' + '{num:02d}'.format(num=x.month))
    # Spalte "Betrag" erzeugen / create column amount
    myKtoCleanDF['Betrag'] = myKtoCleanDF.apply(lambda row: row.Soll if pd.notnull(row.Soll) else row.Haben, axis=1)
    myKtoCleanDF.columns

    # Save categorized data, for example if you would like to use it for kmymoney
    myKtoKatName = pname + f'{leadin}_{filiale}_{kto}_Kategorized.csv'
    myKtoCleanDF.to_csv(myKtoKatName, encoding='latin_1', sep=';', decimal=',', index=False, date_format='%d.%m.%Y')
    logging.info('Erzeugt: ' + myKtoKatName)

    logging.info('myKtoCleanDF.columns: ' + repr(myKtoCleanDF.columns))
    #logging.info('myKtoCleanDF: ' + repr(myKtoCleanDF))

    # Consolidate myKtoCleanDF
    myKonsolidierung = myKtoCleanDF.groupby(by=['JahrMonat','Aus_Ein','Kategorie'])['Betrag'].sum()
    mysummesoll = myKtoCleanDF['Soll'].sum()
    mysummehaben = myKtoCleanDF['Haben'].sum()
    endkontostand = anfangskontostand + mysummesoll + mysummehaben
    msg = f'''Der Endkontostand von {endkontostand:10.2f} ergibt sich aus dem Anfangskontostand von {anfangskontostand:10.2f} sowie 
    der Summe Soll von {mysummesoll:10.2f} und der Summe Haben von {mysummehaben:10.2f}'''
    logging.info(msg)
    # Create pivot table from Consolidation
    # Horizontal: Year/Month, Vertical Categories
    myKFrame  = myKonsolidierung.to_frame()
    mypivottable = myKFrame.pivot_table(myKFrame, index=['Aus_Ein','Kategorie'], columns='JahrMonat', aggfunc='sum',
                                        fill_value=None, margins=False, dropna=True, margins_name='All')
    print(mypivottable)
    mypivottable.to_csv(pname + 'PivotTable' + ukto + '.csv',
                   encoding='latin_1', sep=';',decimal=',', index = True, date_format='%d.%m.%Y')
    logging.info('Erzeugt: ' + pname + 'PivotTable' + ukto + '.csv')
    logging.shutdown()

    pass # Debugging (to set a breakpoint)





