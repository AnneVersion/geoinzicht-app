@echo off
REM GeoInzicht: commit BAG-levenscyclus op develop, merge naar main, push beide.
cd /d E:\scripts\webscraper\CBSbuurt\geoinzicht-app

echo === Status vooraf ===
git status --short

git checkout develop
git add index.html serve.py
git commit -m "BAG levenscyclus: /api/bag/lvc endpoint in serve.py + BAG Viewer-fallback in popup"
git push origin develop

git checkout main
git merge develop -m "Merge develop: BAG levenscyclus + brondocument"
git push origin main

git checkout develop
echo.
echo === Klaar. GitHub Pages (main) wordt binnen ~1 minuut bijgewerkt. ===
pause
