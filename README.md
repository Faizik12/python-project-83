### Hexlet tests and linter status:
[![Actions Status](https://github.com/Faizik12/python-project-83/workflows/hexlet-check/badge.svg)](https://github.com/Faizik12/python-project-83/actions)
[![Action Status](https://github.com/Faizik12/python-project-83/actions/workflows/check.yml/badge.svg)](https://github.com/Faizik12/python-project-83/actions)
[![Maintainability](https://api.codeclimate.com/v1/badges/d367edca198f7cb06251/maintainability)](https://codeclimate.com/github/Faizik12/python-project-83/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/d367edca198f7cb06251/test_coverage)](https://codeclimate.com/github/Faizik12/python-project-83/test_coverage)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

## Description

[Page Analyzer](https://page-analyzer-x4nx.onrender.com) is a website that analyzes specified pages for SEO suitability.

## Instalation

You must have python 3.10 and newer, poetry and postgresql installed to work properly

### Repository cloning

```
git clone git@github.com:Faizik12/python-project-49.git
```

### Database creation

```
sudo -u postgres createuser --createdb {username} 
createdb {databasename}
```

### Secret keys

The site requires two environment variables: SECRET_KEY and DATABASE_URL.
These can be defined using the .env file:
```
DATABASE_URL='postgresql://{username}:{password}@{host}:{port}/{databasename}'
SECRET_KEY='{your secret key}'
```

### Installing dependencies and customizing the database

```
export DATABASE_URL='postgresql://{username}:{password}@{host}:{port}/{databasename}'
make build
```

### Running a dev server

```
make dev
```

### Starting production server

```
make start
```
