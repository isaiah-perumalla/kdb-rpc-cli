/q hdb.q /data -p 5012
if[1>count .z.x;show"Supply directory of historical database";exit 0];
hdb:.z.x 0

/Mount the Historical Date Partitioned Database
@[{system"l ",x};hdb;{show "Error message - ",x;exit 0}]
-1 " " sv ("running hdb path"; hdb; "port"; (string system"p"));