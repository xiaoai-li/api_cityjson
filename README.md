# CityJSON + RESTful access + streaming


## demo online

[-->demo](http://hugoledoux.pythonanywhere.com/)

## to run locally

__Watch out, cjio must use the 'develop' branch otherwise nothing will work__

```bash
$ env FLASK_APP=app.py flask run
```

In debug mode:
```bash
$ env FLASK_APP=app.py FLASK_ENV=development flask run
```

## example of WFS3 URL

```
http://localhost:5000/collections/delft/items/?limit=5&offset=10
```

```
http://localhost:5000/collections/delft/items/?f=json
```

```
http://localhost:5000/collections/delft/items/?bbox=1.2,44.9,55.0,1909.1
```

### code note 
```
pip install git+https://github.com/cityjson/cjio.git@develop
```

### question
- bbox filters for multiple collections or collection?bbox
- file_path column? (datasets+ processor in a same server)
- no longer supports THREE.Geometry. Use THREE.BufferGeometry instead.');
-spacial contanioues 
- what is a feature??? currently single-part buiklding + building parts 
multiple building needs their children for at least rederning purposes
- tile insertion or post process
- preprocess


- when select from metadata table default using the latest version (Order by ... limit 1)
- WITHOUT epsg info 
- bug :(index):143 Uncaught ReferenceError: cols is not defined
- I will generate (will I???) the file for the filtered file.
- two tables or one table???
two --> name ...I do not need set a expired a time
one --> 
- trigger or direct delete? multiple sessions. Will it influence this?
trigger :( time consuming 
direct :(  


- grid (some noise)
    from vertices (some noises)
    from geographicalExtend ()

## todo:
- check the X Y order (lat long)
- collection witt unkonw crs filtering when drawing still 4326 can be written daar
- query_feature (relatives)
- set path and {}.metadata during insertion
- integrate with cjio (remove ophan..))
- problem  every time qury full relatives  leads to duplicate 
- refer change to int type in db

- attribute filtering level on cols???
- create index for each attributs

- all vertices transform (waste of time)
- chunking is still necessary

- filter_cols_bbox simplify the SQL

- if user only want building part

- if exceeed 25 in total must put an input daar

- reduce unneccesarry vertices transform (cols with same crs)

- chunk still useful!

- finalize textbox input.

- filter_cols add crs

- no z value 

- vectorize