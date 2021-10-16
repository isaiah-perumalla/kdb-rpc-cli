# Setup Ticker plant & RDB
1. q tick.q -q -p 5010
2. q feed.q localhost:5010 -t 507 -q
3. q tick/r.q -p 5011 -q
4. q cx.q show