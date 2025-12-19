@echo off
start "perpetual_orderBook" cmd /c "cd /d %~dp0 && echo Start WebSocketStream && python WebSocketStream.py && echo End WebSocketStream"
start "perpetual_orderBook" cmd /c "cd /d %~dp0 && timeout /t 5 /nobreak >nul 2>&1 && echo Start Download && python Download.py && echo End Download && pause"