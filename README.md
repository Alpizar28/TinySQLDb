Pasos para ejecutar Jose Alpizar:
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process
. "C:\Users\Pablo\Downloads\TinySQLDb-main\TinySQLDb-main\Client\tinysqlclient.ps1"
Execute-MyQuery -QueryFile "C:\Users\Pablo\Downloads\TinySQLDb-main\TinySQLDb-main\script.tinysql" -Port 11000 -IP 127.0.0.1

Pasos para ejecutar Adrian: 
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
. "C:\PROYECTO2\Client\tinysqlclient.ps1"
Execute-MyQuery -QueryFile "C:\PROYECTO2\script.tinysql" -Port 11000 -IP 127.0.0.1
