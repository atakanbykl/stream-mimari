@echo off
title Stream Mimari - NATS Server
echo ========================================================
echo   Stream Mimari NATS Server Baslatiliyor...
echo   Konfigurasyon: nats-server.conf
echo   Port: 4222  ^|  Monitoring: http://localhost:8222
echo ========================================================
nats-server -c nats-server.conf
pause
