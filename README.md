# README

to reproduce:

start a debugger listening on 5005, eg
 
> `jdb -listen 5005`

run Maven:

> `mvn clean package`

the forked jvm running surefire hangs using 100% on all cores.
