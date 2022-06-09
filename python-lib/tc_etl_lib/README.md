# Librería Python para ETLs

Esta librería tiene funciones comunes que pueden ser útiles para uso en las ETLs.

Página de la libreria en PyPI:https://pypi.org/project/tc-etl-lib/

## Empaquetado de la librería

La librería permite versionado y se puede acceder a las diferentes versiones en `dist`. En ese directorio se van acumulando el empaquetado de cada versión de la librería.

Para empaquetar se procede de la siguiente manera:

* En el fichero de configuración [setup.py](./setup.py) hay una variable `VERSION` que indica la versión. Se debe incrementar esta versión al empaquetar.
* Se puede empaquetar la librería en diferentes formatos, dependiendo del sistema operativo desde que se ejecuta, puede ser zip, tar.gz, etc.. Por defecto en linux, si no le indicas un formato concreto, usará el .tar.gz. El empaquetado, creará un archivo en la carpeta /dist/ del tipo  `tc_etl_lib-<version>.tar.gz`, que se puede utilizar en los proyectos que sea necesario. Se empaqueta mediante el siguiente comando:

        python3 setup.py sdist bdist_wheel

* Una vez generada la librería, se puede utilizar desde cualquier proyecto instalándola mediante `pip`

## Subida de la libreria a repositorio público de pip

Una vez construido el paquete de la librería según el procedimiento descrito en la sección anterior, se puede realizar
su subida al repositorio público de pip.

En primer lugar, instalar la herramienta twine si no se dispone previamente de ella:

        pip install twine

Para subir la libreria al repositorio público de pip ejecutar el comando:

        twine upload dist/tc_etl_lib-x.y.z.tar.gz

Es necesario estar registrado previamente en https://pypi.org y con permisos en https://pypi.org/project/tc-etl-lib/, 
ya que se solicitará el usuario y password para realizar la subida.

## Instalación de la librería

Existen distintas alternativas

### Desde el repositorio público de pip (recomendada)

Para instalar la última versión:

        pip install tc_etl_lib

Para instalar una versión concreta:

        pip install tc_etl_lib==0.1.0

También se puede añadir la depedencia a `requirements.txt`:

        tc_etl_lib==0.1.0

he instalar (junto con el resto de depedencias) con el habitual:

        pip install -r requirements.txt

### Con el paquete .tar.gz

La librería de tipo `tc_etl_lib-<version>.tar.gz`, se puede instalar en cualquier entorno python con el siguiente comando:

    pip install ./tc_etl_lib/dist/tc_etl_lib-0.0.1.tar.gz

En este caso, la librería está disponible en ese directorio. Si estuviera disponible en cualquier otra ruta, se debería de reemplazar por la ruta relativa o absoluta correspondiente. Si se quiere instalar sobre un entorno venv, primero has de activar el entorno venv y luego ejecutar el comando de instalación de la librería.

Pueden obtenerlse los .tar.gz correspondientes a cada versión de la libreria en [este enlace](https://github.com/telefonicasc/etl-framework/releases).

### Desde el repositorio git

La librería se puede instalar usando el repositorio remoto de github, para ello se ha de añadir la siguiente linea en el fichero requirements.txt.

Para instalar una versión concreta

```
tc_etl_lib @ git+https://github.com/telefonicasc/etl-framework@{tag_version}#package=tc_etl_lib&subdirectory=python-lib/tc_etl_lib
```

Donde:

    {tag_version} : Acceso a una versión concreta de la librería.

Para instalar la última versión

```
tc_etl_lib @ git+https://github.com/telefonicasc/etl-framework#package=tc_etl_lib&subdirectory=python-lib/tc_etl_lib
```

Una vez añadida esa referencia en requeriments.txt, se puede instalar con el comando:

    pip install -r requirements.txt

## Uso de la librería

El uso de la librería una vez se ha instalado en el venv, es similar al resto de librerías que dispondrías en python. A continuación mostramos un ejemplo de referencia:

```python    
# import library
import tc_etl_lib as tc
import json

# create a auth manager and use it
auth: tc.auth.authManager = tc.auth.authManager(endpoint='http://<auth_endpoint>:<port>', 
                                                service = 'alcobendas', 
                                                user = 'usuario_alcobendas', 
                                                password = 'xxx')

auth.get_auth_token_subservice(subservice = '/energia')

# create a cb manager and use it
cbm: tc.cb.cbManager = tc.cb.cbManager(endpoint = 'http://<cb_endpoint>:<port>')
entities = cbm.get_entities_page(subservice='/energia', auth=auth, type='myType')

# have a look to the retrieved entities
print (json.dumps(entities))
```

Ejemplo de uso de Recogida de todos los datos de tipo SupplyPoint con paginación:

```python
# import library
import tc_etl_lib as tc

# declare authManager
auth: tc.auth.authManager = tc.auth.authManager(endpoint='http://<auth_endpoint>:<port>', 
                                                service = 'alcobendas', 
                                                subservice = '/energia',
                                                user = 'usuario_alcobendas', 
                                                password = 'xxx')
# get token
try:
    token = auth.get_auth_token_subservice()
    logger.info(f'token: {token}')
except Exception as err:
    logger.error('get_auth_token_subservice problems!!')
    logger.error(err)

# get all SupplyPoint entities with pg
cbm: tc.cb.cbManager = tc.cb.cbManager(endpoint = 'http://<cb_endpoint>:<port>')

pg = 1
limit = 5
entities = None
while entities != []:
    entities = cbm.get_entities_page(auth = auth, offset = (pg-1)*limit, limit = limit, type='SupplyPoint')
    for i, item in enumerate(entities):
        logger.info(f'(pg:{pg})[{i + (pg-1)*limit}]--> {item["id"]} ({item["type"]})')
    pg += 1
```

Ejemplo de Envío de datos en ráfaga

```python
# import library
import tc_etl_lib as tc

# declare authManager
auth: tc.auth.authManager = tc.auth.authManager(endpoint='http://<auth_endpoint>:<port>', 
                                                service = 'alcobendas', 
                                                subservice = '/energia',
                                                user = 'usuario_alcobendas', 
                                                password = 'xxx')

# get token
try:
    auth.get_auth_token_subservice()
except Exception as err:
    logger.error('get_auth_token_subservice problems!!')
    logger.error(err)

# send entities
cbm: tc.cb.cbManager = tc.cb.cbManager(endpoint = 'http://<cb_endpoint>:<port>')

entities = [
            {
                "id": "myEntity1",
                "type": "myType",
                "description": {
                    "value": "My first happy entity",
                    "type": "Text"
                },
                "online": {
                    "value": 'true',
                    "type": "Boolean"
                },
                "temperature": {
                    "value": '42.3',
                    "type": "Number"
                }
            },
            {
                "id": "myEntity2",
                "type": "myType",
                "description": {
                    "value": "My second happy entity",
                    "type": "Text"
                },
                "online": {
                    "value": 'false',
                    "type": "Boolean"
                },
                "temperature": {
                    "value": '12.3',
                    "type": "Number"
                }
            }
	    ]
 
cbm.send_batch(auth=auth, subservice='/energia', entities=entities)

```

## Funciones disponibles en la librería

La librería está creada con diferentes clases dependiendo de la funcionalidad deseada.

- Clase `authManager`: En esta clase están las funciones relacionadas con la Autenticación (IDM).
    - `__init()__`: constructor de objetos de la clase
        - :param obligatorio `endpoint`: define el endpoint de identificación (ejemplo: https://`<service>`:`<port>`). Se debe especificar en el constructor del objeto de tipo authManager, sino avisará con una excepción ValueError.
        - :param obligatorio `user`: define el usuario para identificarse. Se debe especificar en el constructor del objeto de tipo authManager, sino avisará con una excepción ValueError.
        - :param obligatorio `password`: define la contraseña para identificarse. Se debe especificar en el constructor del objeto de tipo authManager, sino avisará con una excepción ValueError.
        - :param obligatorio `service`: define el servicio donde identificarse. Se debe especificar en el constructor del objeto del tipo authManager, sino avisará con una excepción ValueError.
        - :param opcional `subservice`: define el subservicio donde identificarse. Se puede especificar en el constructor o a posteriori cuando se llamen a las funciones del cbManager.
        - :raises [ValueError](https://docs.python.org/3/library/exceptions.html#ValueError): Se lanza cuando le falta alguno de los argumentos obligatorios
    - `get_auth_token_subservice`: Realiza una petición de token al IDM, identificándose con los creenciales de usuario
      contraseña, servicio y subservicio. El token obtenido se almacena internamente en un caché dentro del propio objeto, de forma
      que otras funciones de la librería que hagan uso del authManager (pe. las del cbManager) intentará utilizar siempre primero
      este caché antes que solicitar un nuevo token via API del IDM.
        - :param opcional `subservice`: se define el subservicio al que identificarse. Este parámetro es opcional. Si no se le indica, cogerá el subservicio inicializado en el objeto de tipo authManager que esté llamando la función. Si no está inicializado en el objeto ni se pasa por parámetro, se lanzará un ValueError.
        - :raises [ValueError](https://docs.python.org/3/library/exceptions.html#ValueError): Se lanza cuando le falta algún argumento o inicializar alguna variable del objeto authManager, para poder realizar la autenticación.
        - :raises [Exception](https://docs.python.org/3/library/exceptions.html#Exception): Se lanza cuando el servicio de identificación, responde con un error concreto.
        - :return: El token que te asigna el servicio de identificación, a parte internamente authManager almacena en caché el token que ha recibido de la autenticación para cada servicio, por si lo necesita a postetiori para realizar acciones con ese token.
- Clase `cbManager`: En esta clase están funciones para la interacción con el Context Broker.
   - `__init()__`: constructor de objetos de la clase
        - :param obligatorio `endpoint`: define el endpoint del context broker (ejemplo: https://`<service>`:`<port>`). Se debe especificar en el constructor del objeto de tipo cbManager, sino avisará con una excepción ValueError.
        - :param opcional `timeout`: timeout definido en segundos (default: 10).
        - :param opcional `post_retry_connect`: Número de reintentos a la hora de realizar un envío de datos (default: 3)
        - :param opcional `post_retry_backoff_factor`: Factor que se usa, para esperar varios segundos tras enviar una ráfaga de datos. (default: 0)
        - :param opcional `sleep_send_batch`: Pausa en segundos, que se realiza cada vez que se envia una ráfaga de datos. (default: 0). 
        - :param opcional `cb_flowcontrol`: Opción del Context Broker, que permite un mejor rendimiento en caso de envío masivo de datos (batch updates). Este mecanismo, requiere arrancar el Context Broker con un flag concreto y en las peticiones de envío de datos, añadir esa opción. Referencia en [documentación de Orion](https://fiware-orion.readthedocs.io/en/master/admin/perf_tuning/index.html#updates-flow-control-mechanism) (default: False)
        - :raises [ValueError](https://docs.python.org/3/library/exceptions.html#ValueError): Se lanza cuando le falta alguno de los argumentos obligatorios
    - `send_batch`: Función que envía un lote de entidades al Context Broker. Se le pasa el authManager, que ya dispone de un listado con todos los tokens por subservicio y usa el correspondiente para realizar la llamada al Context Broker. Si no se dispone de token o ha caducado, se solicita nuevo y luego envía los datos.
        - :param obligatorio `auth`: Se le proporciona el authManager, que tiene las credenciales por si ha de solicitar un token y dispone del listado de tokens asociado. Si no se define en la llamada a la función, avisará con una excepción ValueError.
        - :param obligatorio `entities`: Listado de entidades a enviar, Si no se define en la llamada a la función, avisará con una excepción ValueError.
        - :param opcional `subservice`: Se puede indicar al subservicio al que se le quiere enviar los datos. Sino se indica, usará el que haya inicializado en el objeto authManager. Sino dispone ninguno de los dos definidos, lanzará un ValueError indicando que necesita definirse el subservicio.
        - :raises [ValueError](https://docs.python.org/3/library/exceptions.html#ValueError): Se lanza cuando le falta algún argumento o inicializar alguna varibale del objeto cbManager, para poder realizar la autenticación o envío de datos.
        - :raises [Exception](https://docs.python.org/3/library/exceptions.html#Exception): Se lanza cuando el servicio de Context Broker, responde con un error concreto.
        - :return: True si la operación es correcta (i.e. el CB devolió un code http 204).
    - `get_entities_page`: Función que recoge datos del Context Broker. Se le pasa el authManager, que ya dispone de un listado con todos los tokens por subservicio y usa el correspondiente para realizar la llamada al Context Broker. Si no se dispone de token o ha caducado, se solicita nuevo y luego recoge los datos.
        - :param obligatorio `auth`: Se le proporciona el authManager, que tiene las credenciales por si ha de solicitar un token y dispone del listado de tokens asociado. Si no se define en la llamada a la función, avisará con una excepción ValueError.
        - :param opcional `subservice`: Se puede indicar al subservicio al que se le quiere enviar los datos. Sino se indica, usará el que haya inicializado en el objeto authManager. Sino dispone ninguno de los dos definidos, lanzará un ValueError indicando que necesita definirse el subservicio.
        - :param opcional `offset`: Se establece el offset cuando se recogen los datos. Si no se especifica, enviará la petición al Context Broker sin offset definido. Por lo tanto se aplicará el valor offset por defecto que tiene el Context Broker que es 0.
        - :param opcional `limit`: Se establece un límite de recogida de datos. Si no se especifica, enviará la petición al Context Broker sin el limit definido, por lo tanto se aplirá el valor de limit por defecto que tiene el Context Broker, que es 20.
        - :param opcional `type`: Se establece la recogida de un tipo de datos. Si no se especifica, enviará la petición al Context Broker sin el type definido, por lo tanto los datos no serán filtrados por el tipo de entidad.
        - :param opcional `orderBy`: Se establece un orden en la recogida de datos. Si no se especifica, enviará la petición al Contexts Broker sin el orderBy definido, por lo tanto aplicará el orden por defecto en el Context Broker (i.e. orden de fecha de creación de la entidades)
        - :raises [ValueError](https://docs.python.org/3/library/exceptions.html#ValueError): Se lanza cuando le falta algún argumento o inicializar alguna varibale del objeto cbManager, para poder realizar la autenticación o envío de datos.
        - :raises FetchError: Se lanza cuando el servicio de Context Broker, responde con un error concreto.
        - :return: array de datos cuyos elementos son objeto que representan entidades, según el formato descrito en la sección
          "JSON Entity Representation" de la [NGSIv2 API](https://fiware.github.io/specifications/ngsiv2/stable/)
    
## Changelog

0.1.0 (April 26th, 2022)

- Initial version
