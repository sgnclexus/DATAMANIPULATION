# Data Reports

## Table of Contents

- [About](#about)
- [Getting Started](#getting_started)
- [Usage](#usage)

## About <a name = "about"></a>

This py's are generated to generate reports of scoring unit in a easy way using info that are in califica database. The main idea is to get information of all kind of exams and give the results in a fastest way.

## Getting Started <a name = "getting_started"></a>

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See [deployment](#deployment) for notes on how to deploy the project on a live system.

### Prerequisites

Its important to know the database structure and more important to know about the structure or data domain of each exam

```
Important db schemas
    APPDCGRL - General info of applications
    APPDCENR - Score delivering
    APPDCCAL - Scoring
```

### Installing

A step by step series of examples that tell you how to get a development env running.

Say what the step will be

**Powersheel (Default vs code)** 
```
C:\Users\<user>\AppData\Local\Programs\Python\Python311\python -m venv .\venv
.\venv\Scripts\activate
```

**1ro debemos de cargar las librerias necesarias, para utilizamos el archivo requirement.txt**
```
pip install -r .\requirements.txt
```

End with an example of getting some data out of the system or using it for a little demo.

## Usage <a name = "usage"></a>

The idea of this program is to use the power of python instead of SQL queries or SPs or PL/SQL to get reports or statistics more easily
