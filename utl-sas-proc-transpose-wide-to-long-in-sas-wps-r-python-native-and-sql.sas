%let pgm=utl-sas-proc-transpose-wide-to-long-in-sas-wps-r-python-native-and-sql;

SAS proc transpose wide to long normailize in sas wps r python native and sql

         1. SAS proc transpose
         2. WPS Proc transpose
         3  SAS gather macro
         4  WPS gather macro
         5  SAS sql
         6  WPS sql
         7  WPS proc r native
         8  WPS proc r sql
         9  Python SQL


github
https://tinyurl.com/bdejewch
https://github.com/rogerjdeangelis/utl-sas-proc-transpose-wide-to-long-in-sas-wps-r-python-native-and-sql

StackOverflow
https://tinyurl.com/5arvrf99
https://stackoverflow.com/questions/2185252/reshaping-data-frame-from-wide-to-long-format

Related

github
https://tinyurl.com/4faw6j7n
https://github.com/rogerjdeangelis/utl-sas-proc-transpose-in-sas-r-wps-python-native-and-sql-code

https://tinyurl.com/apbrv7yn
https://github.com/rogerjdeangelis?tab=repositories&q=python&type=&language=&sort=

https://stackoverflow.com/questions/40432590/r-sprintf-list-of-variables-of-mixed-type
https://stackoverflow.com/questions/7547950/how-can-i-pass-r-variable-into-sqldf

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

proc datasets lib=work kill nodetails nolist;
run;quit;

libname sd1 "d:/sd1";

/*--- output to work and d:/sd1 ----*/
options validvarname=upcase;
data have sd1.have;
informat
CODE $3.
COUNTRY $11.
Y1950 $6.
Y1951 $6.
Y1952 $6.
Y1953 $6.
Y1954 $6.
;input
CODE COUNTRY Y1950 Y1951 Y1952 Y1953 Y1954;
cards4;
AFG Afghanistan 20,249 21,352 22,532 23,557 24,555
ALB Albania 8,097 8,986 10,058 11,123 12,246
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from last table WORK.HAVE total obs=2 27MAY2023:07:31:31                                                  */
/*                                                                                                                        */
/* Obs    CODE    COUNTRY        Y1950     Y1951     Y1952     Y1953     Y1954                                            */
/*                                                                                                                        */
/*  1     AFG     Afghanistan    20,249    21,352    22,532    23,557    24,555                                           */
/*  2     ALB     Albania        8,097     8,986     10,058    11,123    12,246                                           */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*           _               _
  ___  _   _| |_ _ __  _   _| |_
 / _ \| | | | __| `_ \| | | | __|
| (_) | |_| | |_| |_) | |_| | |_
 \___/ \__,_|\__| .__/ \__,_|\__|
                |_|
*/

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from last table WORK.HAVXPO total obs=10 27MAY2023:07:35:19                                               */
/*                                                                                                                        */
/* Obs    COUNTRY        CODE    _NAME_     COL1                                                                          */
/*                                                                                                                        */
/*   1    Afghanistan    AFG     Y1950     20,249                                                                         */
/*   2    Afghanistan    AFG     Y1951     21,352                                                                         */
/*   3    Afghanistan    AFG     Y1952     22,532                                                                         */
/*   4    Afghanistan    AFG     Y1953     23,557                                                                         */
/*   5    Afghanistan    AFG     Y1954     24,555                                                                         */
/*   6    Albania        ALB     Y1950     8,097                                                                          */
/*   7    Albania        ALB     Y1951     8,986                                                                          */
/*   8    Albania        ALB     Y1952     10,058                                                                         */
/*   9    Albania        ALB     Y1953     11,123                                                                         */
/*  10    Albania        ALB     Y1954     12,246                                                                         */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                                          _
/ |  ___  __ _ ___   _ __  _ __ ___   ___  | |_ _ __ __ _ _ __  ___ _ __   ___  ___  ___
| | / __|/ _` / __| | `_ \| `__/ _ \ / __| | __| `__/ _` | `_ \/ __| `_ \ / _ \/ __|/ _ \
| | \__ \ (_| \__ \ | |_) | | | (_) | (__  | |_| | | (_| | | | \__ \ |_) | (_) \__ \  __/
|_| |___/\__,_|___/ | .__/|_|  \___/ \___|  \__|_|  \__,_|_| |_|___/ .__/ \___/|___/\___|
                    |_|                                            |_|
*/

proc transpose data=have out=havXpo;
by country code;
var Y:;
run;quit;

/*___                                                _
|___ \  __      ___ __  ___   _ __  _ __ ___   ___  | |_ _ __ __ _ _ __  ___ _ __   ___  ___  ___
  __) | \ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | __| `__/ _` | `_ \/ __| `_ \ / _ \/ __|/ _ \
 / __/   \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | |_| | | (_| | | | \__ \ |_) | (_) \__ \  __/
|_____|   \_/\_/ | .__/|___/ | .__/|_|  \___/ \___|  \__|_|  \__,_|_| |_|___/ .__/ \___/|___/\___|
                 |_|         |_|                                            |_|
*/

%utl_submit_wps64('

libname  sd1 "d:/sd1";

proc transpose data=sd1.have out=havXpo;
by country code;
var Y:;
run;quit;

proc print data=havXpo;
run;quit;

');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System (SAME WITH SAS)                                                                                         */
/*                                                                                                                        */
/* Obs      COUNTRY      CODE    _NAME_     COL1                                                                          */
/*                                                                                                                        */
/*   1    Afghanistan    AFG     Y1950     20,249                                                                         */
/*   2    Afghanistan    AFG     Y1951     21,352                                                                         */
/*   3    Afghanistan    AFG     Y1952     22,532                                                                         */
/*   4    Afghanistan    AFG     Y1953     23,557                                                                         */
/*   5    Afghanistan    AFG     Y1954     24,555                                                                         */
/*   6    Albania        ALB     Y1950     8,097                                                                          */
/*   7    Albania        ALB     Y1951     8,986                                                                          */
/*   8    Albania        ALB     Y1952     10,058                                                                         */
/*   9    Albania        ALB     Y1953     11,123                                                                         */
/*  10    Albania        ALB     Y1954     12,246                                                                         */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                               _   _
|___ /   ___  __ _ ___    __ _  __ _| |_| |__   ___ _ __   _ __ ___   __ _  ___ _ __ ___
  |_ \  / __|/ _` / __|  / _` |/ _` | __| `_ \ / _ \ `__| | `_ ` _ \ / _` |/ __| `__/ _ \
 ___) | \__ \ (_| \__ \ | (_| | (_| | |_| | | |  __/ |    | | | | | | (_| | (__| | | (_) |
|____/  |___/\__,_|___/  \__, |\__,_|\__|_| |_|\___|_|    |_| |_| |_|\__,_|\___|_|  \___/
                         |___/
*/

%utl_gather(have,var,val,CODE COUNTRY,havXpo,valformat=$12.,withformats=Y);

/*  _                                      _   _
| || |   __      ___ __  ___    __ _  __ _| |_| |__   ___ _ __   _ __ ___   __ _  ___ _ __ ___
| || |_  \ \ /\ / / `_ \/ __|  / _` |/ _` | __| `_ \ / _ \ `__| | `_ ` _ \ / _` |/ __| `__/ _ \
|__   _|  \ V  V /| |_) \__ \ | (_| | (_| | |_| | | |  __/ |    | | | | | | (_| | (__| | | (_) |
   |_|     \_/\_/ | .__/|___/  \__, |\__,_|\__|_| |_|\___|_|    |_| |_| |_|\__,_|\___|_|  \___/
                  |_|          |___/
*/

%utl_submit_wps64('

libname  sd1 "d:/sd1";

%utl_gather(sd1.have,var,val,CODE COUNTRY,havXpo,valformat=$12.,withformats=Y);

proc print data=havXpo;
run;quit;

',resolve=N); /*=== so that gather is executed by wps ----*/

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System  (SAME WITH SAS)                                                                                        */
/*                                                                                                                        */
/* Obs    CODE      COUNTRY       VAR     VAL             _COLFORMAT    _COLTYP                                           */
/*                                                                                                                        */
/*   1    AFG     Afghanistan    Y1950    20,249             $6.           C                                              */
/*   2    AFG     Afghanistan    Y1951    21,352             $6.           C                                              */
/*   3    AFG     Afghanistan    Y1952    22,532             $6.           C                                              */
/*   4    AFG     Afghanistan    Y1953    23,557             $6.           C                                              */
/*   5    AFG     Afghanistan    Y1954    24,555             $6.           C                                              */
/*   6    ALB     Albania        Y1950    8,097              $6.           C                                              */
/*   7    ALB     Albania        Y1951    8,986              $6.           C                                              */
/*   8    ALB     Albania        Y1952    10,058             $6.           C                                              */
/*   9    ALB     Albania        Y1953    11,123             $6.           C                                              */
/*  10    ALB     Albania        Y1954    12,246             $6.           C                                              */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                              _
| ___|   ___  __ _ ___   ___  __ _| |
|___ \  / __|/ _` / __| / __|/ _` | |
 ___) | \__ \ (_| \__ \ \__ \ (_| | |
|____/  |___/\__,_|___/ |___/\__, |_|
                                |_|
*/

%array(_seq,values=0 1 2 3 4);

proc sql;
  create
     table want as
    %do_over(_seq,phrase=%str(
  select
      Code
   ,Country
   ,"Y195?" As Year
   ,Y195? As Value
  From have
  ),between=union)

;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  Up to 40 obs from WANT total obs=10 27MAY2023:08:13:59                                                                */
/*                                                                                                                        */
/*  Obs    CODE    COUNTRY        YEAR     VALUE                                                                          */
/*                                                                                                                        */
/*    1    AFG     Afghanistan    Y1950    20,249                                                                         */
/*    2    AFG     Afghanistan    Y1951    21,352                                                                         */
/*    3    AFG     Afghanistan    Y1952    22,532                                                                         */
/*    4    AFG     Afghanistan    Y1953    23,557                                                                         */
/*    5    AFG     Afghanistan    Y1954    24,555                                                                         */
/*    6    ALB     Albania        Y1950    8,097                                                                          */
/*    7    ALB     Albania        Y1951    8,986                                                                          */
/*    8    ALB     Albania        Y1952    10,058                                                                         */
/*    9    ALB     Albania        Y1953    11,123                                                                         */
/*   10    ALB     Albania        Y1954    12,246                                                                         */
/*                                                                                                                        */
/*                                                                                                                        */
/*  IF YOU WANT THE CODE                                                                                                  */
/*                                                                                                                        */
/*  data _null;                                                                                                           */
/*     %do_over(_seq,phrase=put "Select Code, Country, 'Y195?' As Year, Y195? As Value From have union";);                */
/*  run;quit;                                                                                                             */
/*                                                                                                                        */
/*  Select Code, Country, 'Y1950' As Year, Y1950 As Value From have union                                                 */
/*  Select Code, Country, 'Y1951' As Year, Y1951 As Value From have union                                                 */
/*  Select Code, Country, 'Y1952' As Year, Y1952 As Value From have union                                                 */
/*  Select Code, Country, 'Y1953' As Year, Y1953 As Value From have union                                                 */
/*  Select Code, Country, 'Y1954' As Year, Y1954 As Value From have union                                                 */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*__                                    _
 / /_   __      ___ __  ___   ___  __ _| |
| `_ \  \ \ /\ / / `_ \/ __| / __|/ _` | |
| (_) |  \ V  V /| |_) \__ \ \__ \ (_| | |
 \___/    \_/\_/ | .__/|___/ |___/\__, |_|
                 |_|                 |_|
*/

%utl_submit_wps64('

options validvarname=any;

libname sd1 "d:/sd1";

%array(_seq,values=0 1 2 3 4);

proc sql;
  create
     table want as
    %do_over(_seq,phrase=%str(
  select
      Code
   ,Country
   ,"Y195?" As Year
   ,Y195? As Value
  From sd1.have
  ),between=union)

;quit;

proc print data=want;
run;quit;

',resolve=N);

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*  Obs    CODE      COUNTRY      Year     Value                                                                          */
/*                                                                                                                        */
/*    1    AFG     Afghanistan    Y1950    20,249                                                                         */
/*    2    AFG     Afghanistan    Y1951    21,352                                                                         */
/*    3    AFG     Afghanistan    Y1952    22,532                                                                         */
/*    4    AFG     Afghanistan    Y1953    23,557                                                                         */
/*    5    AFG     Afghanistan    Y1954    24,555                                                                         */
/*    6    ALB     Albania        Y1950    8,097                                                                          */
/*    7    ALB     Albania        Y1951    8,986                                                                          */
/*    8    ALB     Albania        Y1952    10,058                                                                         */
/*    9    ALB     Albania        Y1953    11,123                                                                         */
/*   10    ALB     Albania        Y1954    12,246                                                                         */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                                                                 _   _
|___  | __      ___ __  ___   _ __  _ __ ___   ___   _ __  _ __   __ _| |_(_)_   _____
   / /  \ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `__|| `_ \ / _` | __| \ \ / / _ \
  / /    \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | |   | | | | (_| | |_| |\ V /  __/
 /_/      \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |_|   |_| |_|\__,_|\__|_| \_/ \___|
                 |_|         |_|
*/


%let _pth=%sysfunc(pathname(work));
%utl_submit_wps64('
libname wrk "&_pth";
proc r;
export data=wrk.have r=have;
submit;
library(tidyverse);
want_r<-gather(have,YEAR, VALUE, -CODE, -COUNTRY);
want_r;
endsubmit;
import data=wrk.want_r r=want_r;
run;quit;
');

proc print data=want_r;
run;quit;

/**************************************************************************************************************************/
/*                                    |                                                                                   */
/*                                    |                                                                                   */
/*  The WPS System  (IN R)            |  (BACK TO SAS)                                                                    */
/*                                    |  Obs    CODE    COUNTRY        YEAR     VALUE                                     */
/*     CODE     COUNTRY  YEAR  VALUE  |                                                                                   */
/*  1   AFG Afghanistan Y1950 20,249  |    1    AFG     Afghanistan    Y1950    20,249                                    */
/*  2   ALB     Albania Y1950  8,097  |    2    ALB     Albania        Y1950    8,097                                     */
/*  3   AFG Afghanistan Y1951 21,352  |    3    AFG     Afghanistan    Y1951    21,352                                    */
/*  4   ALB     Albania Y1951  8,986  |    4    ALB     Albania        Y1951    8,986                                     */
/*  5   AFG Afghanistan Y1952 22,532  |    5    AFG     Afghanistan    Y1952    22,532                                    */
/*  6   ALB     Albania Y1952 10,058  |    6    ALB     Albania        Y1952    10,058                                    */
/*  7   AFG Afghanistan Y1953 23,557  |    7    AFG     Afghanistan    Y1953    23,557                                    */
/*  8   ALB     Albania Y1953 11,123  |    8    ALB     Albania        Y1953    11,123                                    */
/*  9   AFG Afghanistan Y1954 24,555  |    9    AFG     Afghanistan    Y1954    24,555                                    */
/*  10  ALB     Albania Y1954 12,246  |   10    ALB     Albania        Y1954    12,246                                    */
/*                                                                                                                        */
/**************************************************************************************************************************/
/*___                                                                 _
 ( _ )  __      ___ __  ___   _ __  _ __ ___   ___   _ __   ___  __ _| |
 / _ \  \ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `__| / __|/ _` | |
| (_) |  \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | |    \__ \ (_| | |
 \___/    \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |_|    |___/\__, |_|
                 |_|         |_|                                   |_|
*/
%utl_submit_wps64('
libname sd1 "d:\sd1";
proc r;
 export data=sd1.have r=wide;
 submit;
 library(sqldf);
 want_r<-sqldf(
      "Select Code, Country, Y1950 As Year, \"1950\" As Value From wide Union All
       Select Code, Country, Y1951 As Year, \"1951\" As Value From wide Union All
       Select Code, Country, Y1952 As Year, \"1952\" As Value From wide Union All
       Select Code, Country, Y1953 As Year, \"1953\" As Value From wide Union All
       Select Code, Country, Y1954 As Year, \"1954\" As Value From wide;");
 want_r;
 endsubmit;
 import data=sd1.want_r_sql r=want_r;
 run;quit;
');

proc print data=sd1.want_r_sql ;
run;quit;

/**************************************************************************************************************************/
/*                                      |                                                                                 */
/*                                      |  Up to 40 obs from SD1.HAVE total obs=2 27MAY2023:09:11:06                      */
/*  The WPS System                      |                                                                                 */
/*                                      |  Obs    CODE    COUNTRY         YEAR     VALUE                                  */
/*     CODE     COUNTRY   Year Value    |                                                                                 */
/*  1   AFG Afghanistan 20,249  1950    |    1    AFG     Afghanistan    20,249    1950                                   */
/*  2   ALB     Albania  8,097  1950    |    2    ALB     Albania        8,097     1950                                   */
/*  3   AFG Afghanistan 21,352  1951    |    3    AFG     Afghanistan    21,352    1951                                   */
/*  4   ALB     Albania  8,986  1951    |    4    ALB     Albania        8,986     1951                                   */
/*  5   AFG Afghanistan 22,532  1952    |    5    AFG     Afghanistan    22,532    1952                                   */
/*  6   ALB     Albania 10,058  1952    |    6    ALB     Albania        10,058    1952                                   */
/*  7   AFG Afghanistan 23,557  1953    |    7    AFG     Afghanistan    23,557    1953                                   */
/*  8   ALB     Albania 11,123  1953    |    8    ALB     Albania        11,123    1953                                   */
/*  9   AFG Afghanistan 24,555  1954    |    9    AFG     Afghanistan    24,555    1954                                   */
/*  10  ALB     Albania 12,246  1954    |   10    ALB     Albania        12,246    1954                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                _   _                             _
 / _ \   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| (_) | | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
 \__, | | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
   /_/  | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
        |_|    |___/                                |_|
*/

proc datasets lib=work kill nodetails nolist;
run;quit;

%utlfkil(d:/xpt/res.xpt);

%utl_pybegin;
parmcards4;
from os import path
import pandas as pd
import xport
import xport.v56
import sqlite3;
import pyreadstat
import numpy as np
import pandas as pd
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
wide, meta = pyreadstat.read_sas7bdat("d:/sd1/have.sas7bdat");
print(wide);
res = pdsql("""
       Select Code, Country, Y1950 As Year, '1950' As Value From wide Union All
       Select Code, Country, Y1951 As Year, '1951' As Value From wide Union All
       Select Code, Country, Y1952 As Year, '1952' As Value From wide Union All
       Select Code, Country, Y1953 As Year, '1953' As Value From wide Union All
       Select Code, Country, Y1954 As Year, '1954' As Value From wide;
""")
print(res);
ds = xport.Dataset(res, name='res')
with open('d:/xpt/res.xpt', 'wb') as f:
    xport.v56.dump(ds, f)
;;;;
%utl_pyend;

libname pyxpt xport "d:/xpt/res.xpt";

proc contents data=pyxpt._all_;
run;quit;

proc print data=pyxpt.res;
run;quit;

data res;
   set pyxpt.res;
run;quit;

 /**************************************************************************************************************************/
 /*                                     |                                                                                  */
 /*  Python                             |   Up to 40 obs from RES total obs=10 27MAY2023:11:22:48                          */
 /*                                     |                                                                                  */
 /*   CODE      COUNTRY    Year Value   |   Obs    CODE    COUNTRY         YEAR     VALUE                                  */
 /*                                     |                                                                                  */
 /* 0  AFG  Afghanistan  20,249  1950   |     1    AFG     Afghanistan    20,249    1950                                   */
 /* 1  ALB      Albania   8,097  1950   |     2    ALB     Albania        8,097     1950                                   */
 /* 2  AFG  Afghanistan  21,352  1951   |     3    AFG     Afghanistan    21,352    1951                                   */
 /* 3  ALB      Albania   8,986  1951   |     4    ALB     Albania        8,986     1951                                   */
 /* 4  AFG  Afghanistan  22,532  1952   |     5    AFG     Afghanistan    22,532    1952                                   */
 /* 5  ALB      Albania  10,058  1952   |     6    ALB     Albania        10,058    1952                                   */
 /* 6  AFG  Afghanistan  23,557  1953   |     7    AFG     Afghanistan    23,557    1953                                   */
 /* 7  ALB      Albania  11,123  1953   |     8    ALB     Albania        11,123    1953                                   */
 /* 8  AFG  Afghanistan  24,555  1954   |     9    AFG     Afghanistan    24,555    1954                                   */
 /* 9  ALB      Albania  12,246  1954   |    10    ALB     Albania        12,246    1954                                   */
 /*                                                                                                                        */
 /**************************************************************************************************************************/

 /*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/


