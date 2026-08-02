@echo off
REM GeoInzicht: start alle services in een keer.
REM serve.py start daarna zelf de services met "autostart": true uit services.json.
REM Automatisch starten bij het aanzetten van de laptop:
REM   Win+R -> shell:startup -> plak daar een SNELKOPPELING naar dit bestand.
cd /d E:\scripts\webscraper\CBSbuurt\geoinzicht-app
start "GeoInzicht server (8091)" cmd /k python serve.py
