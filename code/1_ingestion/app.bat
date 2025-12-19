@echo off
echo Starting all modules...

REM Start perpetual_fundingRate module
start "perpetual_fundingRate" cmd /c "cd /d %~dp0\perpetual_fundingRate && app.bat"

REM Wait 2 seconds before starting next module
timeout /t 2 /nobreak > nul

REM Start perpetual_indexPriceKlines module
start "perpetual_indexPriceKlines" cmd /c "cd /d %~dp0\perpetual_indexPriceKlines && app.bat"

REM Wait 2 seconds before starting next module  
timeout /t 2 /nobreak > nul

REM Start perpetual_markPriceKlines module  
start "perpetual_markPriceKlines" cmd /c "cd /d %~dp0\perpetual_markPriceKlines && app.bat"

REM Wait 2 seconds before starting next module
timeout /t 2 /nobreak > nul

REM Start perpetual_trades module
start "perpetual_trades" cmd /c "cd /d %~dp0\perpetual_trades && app.bat"

REM Wait 2 seconds before starting next module
timeout /t 2 /nobreak > nul

REM Start perpetual_orderBook module
start "perpetual_orderBook" cmd /c "cd /d %~dp0\perpetual_orderBook && app.bat"

REM Wait 2 seconds before starting next module
timeout /t 2 /nobreak > nul

REM Start spot_trades module
start "spot_trades" cmd /c "cd /d %~dp0\spot_trades && app.bat"

echo All modules started!
pause