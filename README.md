# bank-account-processing
__consolidates, categorizes, pivots__ bank account booking lists (downloaded from your online banking)


Developed for *"Deutsche Bank"* bank statements, might be adjustable for other banks

## Operational description:
We have a .csv - file containing one account activity per line, named *"Kontoumsaetze_bbb_aaaaaaass_Alle.csv"*, where
* *bbb* = subsidiary (Filiale in German)
* *aaaaaaa* = account no (Kontonummer in German)
* *ss* = sub account no (Unterkontonummer in German)

Example for *"Kontoumsaetze_bbb_aaaaaaass_Alle.csv"*:

`Buchungstag;Wert;Umsatzart;Begünstigter / Auftraggeber;Verwendungszweck;IBAN;BIC;Kundenreferenz;Mandatsreferenz ; # line continues!
Gläubiger ID;Fremde Gebühren;Betrag;Abweichender Empfänger;Anzahl der Aufträge;Anzahl der Schecks;Soll;Haben;Währung
07.12.2017;07.12.2017;SEPA-Überweisung an;xyz Corp.;Payment for abc;DE12345678901234567800;ANNNNNNNNNZ;;;;;;;;;-999,99;;EUR
01.12.2017;01.12.2017;SEPA-Gutschrift von;Meier and Comp.;Salary;DE23456789012345678901;BMMMMMMMXXX;;;;;;;;;;47,11;EUR
`

This file collects all bank statements, i.e., after this program has read and consolidated a new file, the above file
will be updated.

Now we download a new .csv file from our bank named *"Kontoumsaetze_bbb_aaaaaaass_yyyymmdd_hhmmss.csv"*, where
*bbb_aaaaaaass* is the same as above and *yyyymmdd_hhmmss* is obvious.

This file has 4 lines in the beginning and 1 line in the end which do not fit into the column scheme and will be ignored.

This program reads this file, joins it to the first mentioned data, removes duplicates, and updates *"Kontoumsaetze_bbb_aaaaaaass_Alle.csv"*

Moreover, this program reads *"KatMapss.json"*, where again *"ss"* stands for subaccount no.
This *.json* file determines the catigorization of bookings, and this works as follows:

The fields "Umsatzart;Begünstigter / Auftraggeber;Verwendungszweck" (= booking type;sender or receiver;booking text)
are concatenated. If one of the keywords in katmap is found in the concatenation, the booking is marked with the category and booking direction belonging to the keyword.

A basic *"KatMapss.json"* is created by this program, if none is found. Modify it according to your needs; next time your *"KatMapss.json"* will be used.

__Caveat:__ This program does not check whether a keyword occurs more than once.

Also the categorized data is saved (*"Kontoumsaetze_bbb_aaaaaaass_Kategorized.csv"*) so that you can read them with
a spreadsheet program like __libre office calc__.
The categorization is always created anew, so if you change *"KatMapss.json"*, also the past data is newly categorized.

As a final step, this program creates *"PivotTabless.csv"*, which shows the categories on the vertical axis and Year/Month horizontally.

During the program run, a log file *"kontoauszug.log"* is created.

## Technical hints:
Usage: 
`python3 kontoauszug.py fname='Kontoumsaetze_bbb_aaaaaaass_yyyymmdd_hhmmss.csv' \
                        pname='/home/myname/development/Kontoauszuege/'
`

(More parameters are described in the source code, they are related to logging and you can keep them alone)


The program needs the modules __pandas, begins, logging, json, pathlib, os__

__f-strings__ are used, therefore, __python 3.6__ or newer is required.

__Caveat:__ Not tested with other OS's than linux; not tested with other files than "Deutsche Bank Online Banking"

Thanks to *python* and *pandas* developers and community!
