#!/bin/bash

NEW_NAME=$1

if [ ! -f .changeme ] ; then
    exit 1
fi

upperName=`echo ${NEW_NAME} | awk '{print toupper($0)}'`
lowerName=`echo ${NEW_NAME} | awk '{print tolower($0)}'`
# echo $upperName
# echo $lowerName
echo "=> Changing Makefile..."
find . -name 'Makefile' -type f -print0 | xargs -0 sed -i "s|CHANGEME|${upperName}|g"
find . -name 'Makefile' -type f -print0 | xargs -0 sed -i "s|changeme|${lowerName}|g"

echo "=> Changing Python files..."
find . -name '*.py' -type f -print0 | xargs -0 sed -i "s|changeme|${lowerName}|g"
echo "=> Changing docs files..."
find . -name '*.rst' -type f -print0 | xargs -0 sed -i "s|changeme|${lowerName}|g"
# echo "Changing Docker files..."
# find . -name 'docker-compose.yml' -type f -print0 | xargs -0 sed -i "s|CHANGEME|${upperName}|g"
# find . -name 'docker-compose.yml' -type f -print0 | xargs -0 sed -i "s|changeme|${lowerName}|g"
# sed -i "s/changeme/${lowerName}/g" Dockerfile
# sed -i "s/CHANGEME/${upperName}/g" Dockerfile
echo "=> Changing pyproject..."
sed -i "s/changeme/${lowerName}/g" pyproject.toml
# echo "Set envs with direnv..."
# sed -i "s/CHANGEME/${upperName}/g" .env.example
# cp .env.example .envrc 
echo "=> Changin README.md..."
sed -i "s/CHANGEME/${upperName}/g" README.md
sed -i "s/changeme/${lowerName}/g" README.md
echo "=> Changing folder..."

if [ ! -d changeme ] ; then
    echo "(x) Dir 'changeme' no longer exist"
    exit 1
fi
mv changeme $lowerName
rm .changeme

echo -n "(ok) Finished..."
