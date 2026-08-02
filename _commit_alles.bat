@echo off
REM GeoInzicht: commit ALLE wijzigingen op develop, merge naar main, push beide.
cd /d E:\scripts\webscraper\CBSbuurt\geoinzicht-app

echo === Status vooraf ===
git status --short

git checkout develop
git add -A
git commit -m "Overheid Catalogus-tab (data.overheid.nl, peildatum), services-paneel + autostart, bekendmakingen, BAG-status prominent, docs/informatiemodel"
git push origin develop

git checkout main
git merge develop -m "Merge develop: overheid-catalogus, services, bekendmakingen, architectuur-docs"
git push origin main

git checkout develop
echo.
echo === Klaar. GitHub Pages (main) is binnen ~1 minuut bij. ===
pause
