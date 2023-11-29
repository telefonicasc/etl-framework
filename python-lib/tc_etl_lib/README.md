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

        pip install tc_etl_lib==0.10.0

También se puede añadir la depedencia a `requirements.txt`:

        tc_etl_lib==0.10.0

he instalar (junto con el resto de depedencias) con el habitual:

        pip install -r requirements.txt

### Con el paquete .tar.gz

La librería de tipo `tc_etl_lib-<version>.tar.gz`, se puede instalar en cualquier entorno python con el siguiente comando:

    pip install ./tc_etl_lib/dist/tc_etl_lib-0.10.0.tar.gz

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

Ejemplo de uso de Recogida de todos los datos de tipo SupplyPoint con y sin paginación:

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

# get all SupplyPoint entities without pg
entities = cbm.get_entities(auth = auth, type='SupplyPoint')
for i, item in enumerate(entities):
    logger.info(f'[{i}]--> {item["id"]} ({item["type"]})')

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

# (opcional) solo es necesario usar normalizer si los datos que se usan para
# construir el entity id pueden contener caracteres prohibidos por NGSI
# (acentos, paréntesis, etc)
normalize = tc.normalizer()

entities = [
            {
                "id": normalize("myEntity1"),
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
                "id": normalize("myEntity2"),
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

# O, usando un store orion
with tc.orionStore(cb=cbm, auth=auth, subservice='/energia') as store:
    store(entities)

# O un store sqlFile
with tc.sqlFileStore(path="inserts.sql", subservice="/energia", namespace="energy") as store:
    store(entities)
```

Ejemplo de uso de la clase iotaManager

```
#create an iota manager and use it
iotam: tc.iota.iotaManager = tc.iota.iotaManager(endpoint = 'http://<iota_endpoint>:<port>/iot/json', sensor_id='<sensor_id>', api_key='<api_key>')
iotam.send_http(data={"<key_1>": "<value_1>", "<key_2>": "<value_2>"})

# Envío de datos en ráfaga al Agente IoT.
iotam: tc.iota.iotaManager = tc.iota.iotaManager(endpoint = 'http://<iota_endpoint>:<port>/iot/json', sensor_id='<sensor_id>', api_key='<api_key>', sleep_send_batch='<time_sleep>')
iotam.send_batch_http(data=[{"<key_1>": "<value_1>", "<key_2>": "<value_2>"}, {"<key_3>": "<value_3>", "<key_4>": "<value_4>"}])
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
    - `get_auth_token_service`: Realiza una petición de token al IDM, identificándose con los creenciales de usuario, contraseña y servicio (obtiene un token de scope **domain**). El token obtenido es un token *privilegiado* que puede usarse para gestión del propio dominio (creación de subservicios, asignación de roles, etc), siempre que el usuario tenga el rol de administrador de dominio. Este token **no se almacena en caché interna**.
        - :raises [ValueError](https://docs.python.org/3/library/exceptions.html#ValueError): Se lanza cuando le falta algún argumento o inicializar alguna variable del objeto authManager, para poder realizar la autenticación.
        - :raises [Exception](https://docs.python.org/3/library/exceptions.html#Exception): Se lanza cuando el servicio de identificación, responde con un error concreto.
        - :return: un objeto con tres propiedades: { `token`: ..., `user_id`: ..., `domain_id`: ... }. Como este token se usará principalmente para administración del dominio, es útil obtener los IDs de usuario y dominio asociados.
    - `set_token`: Establece el token de subservicio en el caché del objeto. En el caso de que ya hubiera un token asociado al subservicio (pe. porque se haya invocado previamente `get_auth_token_subservice`) se sobreescribe. Esta función es útil cuando el token se obtiene por otros mecanismos ajenos a la negociación con el IDM (pe. de una cabecera `x-auth-token`) y se quiere establecer dentro del authManager. Otras funciones de la librería que hagan uso del authManager (pe. las del cbManager) intentará utilizar siempre primero este caché antes que solicitar un nuevo token via API del IDM.
        - :param `subservice`: subservicio en el que establecer el tokenn.
        - :param `token`: token a establecer.
- Clase `cbManager`: En esta clase están funciones para la interacción con el Context Broker.
   - `__init()__`: constructor de objetos de la clase
        - :param obligatorio `endpoint`: define el endpoint del context broker (ejemplo: https://`<service>`:`<port>`). Se debe especificar en el constructor del objeto de tipo cbManager, sino avisará con una excepción ValueError.
        - :param opcional `timeout`: timeout definido en segundos (default: 10).
        - :param opcional `post_retry_connect`: Número de reintentos a la hora de realizar un envío de datos (default: 3)
        - :param opcional `post_retry_backoff_factor`: Factor que se usa, para esperar varios segundos tras enviar una ráfaga de datos. (default: 0)
        - :param opcional `sleep_send_batch`: Pausa en segundos, que se realiza cada vez que se envia una ráfaga de datos. (default: 0). 
        - :param opcional `cb_flowcontrol`: Opción del Context Broker, que permite un mejor rendimiento en caso de envío masivo de datos (batch updates). Este mecanismo, requiere arrancar el Context Broker con un flag concreto y en las peticiones de envío de datos, añadir esa opción. Referencia en [documentación de Orion](https://fiware-orion.readthedocs.io/en/master/admin/perf_tuning/index.html#updates-flow-control-mechanism) (default: False)
        - :param opcional `block_size`: Cuando se realiza el envío de datos al Context Broker mediante la función de `send_batch`, se realiza envíos en tramos que no excedan el block_size indicado (default: 800000). Se permite modificar el valor de block_size, pero sin superar la limitación de 800000. En caso de indicar un valor que supere ese límite, se lanzará una excepción ValueError indicando que se ha excedido el límite del valor permitido.
        - :param opcional `batch_size`: Si este parámetro es mayor que 0, cuando se realiza el envío de datos al Context Broker mediante la función de `send_batch`, se realizan envíos en tramos que no excedan el número de entidades indicado (default: 0 - no aplica). En el caso de ser mayor que 0, tanto `block_size` como `batch_size` aplican a la hora de dividir un envío en tramos.
        
        - :raises [ValueError](https://docs.python.org/3/library/exceptions.html#ValueError): Se lanza cuando le falta alguno de los argumentos obligatorios
    - `send_batch`: Función que envía un lote de entidades al Context Broker aplicándoles una acción `actionType` que por defecto es `append`. Se le pasa el authManager, que ya dispone de un listado con todos los tokens por subservicio y usa el correspondiente para realizar la llamada al Context Broker. Si no se dispone de token o ha caducado, se solicita nuevo y luego envía los datos.
        - :param opcional `auth`: Se le proporciona el authManager, que tiene las credenciales por si ha de solicitar un token y dispone del listado de tokens asociado. Si no se define en la llamada a la función, se asume que no hay IDM asociado al CB y se realizará las operación sin el uso de autenticación.
        - :param obligatorio `entities`: Listado de entidades a enviar, Si no se define en la llamada a la función, avisará con una excepción ValueError.
        - :param opcional `service`: Se puede indicar al servicio al que se le quiere enviar los datos. Sino se indica, usará el que haya inicializado en el objeto authManager. Sino dispone ninguno de los dos definidos, lanzará un ValueError indicando que necesita definirse el servicio.
        - :param opcional `subservice`: Se puede indicar al subservicio al que se le quiere enviar los datos. Sino se indica, usará el que haya inicializado en el objeto authManager. Sino dispone ninguno de los dos definidos, lanzará un ValueError indicando que necesita definirse el subservicio.
        - :param opcional `actionType`: El tipo de acción que se le va aplicar al batch que se envía al Context Broker. Por defecto es `append`. Referencia en [API NGSIv2 de Orion](http://telefonicaid.github.io/fiware-orion/api/v2/stable/)
        - :param opcional `options`: Lista de opciones separadas que recibe el Context Broker y que permite cierto comportamiento. Se pueden ver las opciones disponibles en [API NGSIv2 de Orion](https://github.com/telefonicaid/fiware-orion/blob/master/doc/manuals/orion-api.md#update-post-v2opupdate). En el caso de que la opcion `flowControl` se especifique dentro de este parámetro, un `cb_flowcontrol` (en la inicializción de cbManager) a `False` se ignora, quedanco como si `cb_flowcontrol` se hubiese establecido a `True`.
        - :raises [ValueError](https://docs.python.org/3/library/exceptions.html#ValueError): Se lanza cuando le falta algún argumento o inicializar alguna varibale del objeto cbManager, para poder realizar la autenticación o envío de datos.
        - :raises [Exception](https://docs.python.org/3/library/exceptions.html#Exception): Se lanza cuando el servicio de Context Broker, responde con un error concreto.
        - :return: True si la operación es correcta (i.e. el CB devolió un code http 204).
    - `get_entities_page`: Función que recoge datos del Context Broker. Se le pasa el authManager, que ya dispone de un listado con todos los tokens por subservicio y usa el correspondiente para realizar la llamada al Context Broker. Si no se dispone de token o ha caducado, se solicita nuevo y luego recoge los datos.
        - :param opcional `auth`: Se le proporciona el authManager, que tiene las credenciales por si ha de solicitar un token y dispone del listado de tokens asociado. Si no se define en la llamada a la función, se asume que no hay IDM asociado al CB y se realizará las operación sin el uso de autenticación.
        - :param opcional `service`: Se puede indicar al subservicio del que recoge los datos. Sino se indica, usará el que haya inicializado en el objeto authManager. Sino dispone ninguno de los dos definidos, lanzará un ValueError indicando que necesita definirse el servicio.
        - :param opcional `subservice`: Se puede indicar al subservicio del que se recoge los datos. Sino se indica, usará el que haya inicializado en el objeto authManager. Sino dispone ninguno de los dos definidos, lanzará un ValueError indicando que necesita definirse el subservicio.
        - :param opcional `offset`: Se establece el offset cuando se recogen los datos. Si no se especifica, enviará la petición al Context Broker sin offset definido. Por lo tanto se aplicará el valor offset por defecto que tiene el Context Broker que es 0.
        - :param opcional `limit`: Se establece un límite de recogida de datos. Si no se especifica, enviará la petición al Context Broker sin el limit definido, por lo tanto se aplirá el valor de limit por defecto que tiene el Context Broker, que es 20.
        - :param opcional `type`: Se establece la recogida de un tipo de datos. Si no se especifica, enviará la petición al Context Broker sin el type definido, por lo tanto los datos no serán filtrados por el tipo de entidad.
        - :param opcional `orderBy`: Se establece un orden en la recogida de datos. Si no se especifica, enviará la petición al Context Broker sin el orderBy definido, por lo tanto aplicará el orden por defecto en el Context Broker (i.e. orden de fecha de creación de la entidades)
        - :param opcional `q`: Se establece un filtro de datos en función del valor de los atributos especificados. Si no se especifica, enviará la petición al Context Broker sin el q definido, por lo tanto los datos no serán filtrados por estos atributos.
        - :param opcional `mq`: Se establece un filtro de datos en función de los metadatos definidos en este parámetro. Si no se especifica, enviará la petición al Context Broker sin el mq definido, por lo tanto los datos no serán filtrados por los metadatos.
        - :param opcional `georel`: Cuando se define un filtro de datos por geolocalización, se ha de especificar en georel la relación espacial que se va a utilizar en el filtrado. Se pueden consultar los diferentes valores de georel en [NGSIv2 API](http://telefonicaid.github.io/fiware-orion/api/v2/stable)
        - :param opcional `geometry`: Cuando se define un filtro de datos por geolocalización, se ha de especificar un tipo de dibujo que se utiliza para resolver el filtrado. Se pueden consultar los diferentes valores de geometry en [NGSIv2 API](http://telefonicaid.github.io/fiware-orion/api/v2/stable)
        - :param opcional `coords`: Cuando se define un filtro de datos por geolocalización, se ha de especificar una lista de coordenadas geograficas separadas por coma. Se pueden consultar los diferentes valores de coords en [NGSIv2 API](http://telefonicaid.github.io/fiware-orion/api/v2/stable)
        - :param opcional `id`: Se establece un filtro por Identificador. Si no se especifica, enviará la petición al Context Broker sin el id definido, por lo tanto los datos no serán filtrados por identificador.
        - :param opcional `options`: Lista de opciones que recibe el Context Broker y que permite cierto comportamiento. Se pueden ver las opciones disponibles en [API NGSIv2 de Orion](https://github.com/telefonicaid/fiware-orion/blob/master/doc/manuals/orion-api.md#list-entities-get-v2entities)  
        - :raises [ValueError](https://docs.python.org/3/library/exceptions.html#ValueError): Se lanza cuando le falta algún argumento o inicializar alguna varibale del objeto cbManager, para poder realizar la autenticación o envío de datos.
        - :raises FetchError: Se lanza cuando el servicio de Context Broker, responde con un error concreto.
        - :return: array de datos cuyos elementos son objeto que representan entidades, según el formato descrito en la sección
          "JSON Entity Representation" de la [NGSIv2 API](https://fiware.github.io/specifications/ngsiv2/stable/)
    - `get_entities`: Función que recoge datos del Context Broker. Se le pasa el authManager, que ya dispone de un listado con todos los tokens por subservicio y usa el correspondiente para realizar la llamada al Context Broker. Si no se dispone de token o ha caducado, se solicita nuevo y luego recoge los datos. Esta función con diferencia a `get_entities_page` usa un mecanismo de paginación interna para retornar todas las entidades que concuerden con los filtros definidos, para ello va realizando solicitudes necesarias al Context Broker para recoger todos los datos en tramos y retornarlos acumulados en un array.
        - :param opcional `auth`: Se le proporciona el authManager, que tiene las credenciales por si ha de solicitar un token y dispone del listado de tokens asociado. Si no se define en la llamada a la función, se asume que no hay IDM asociado al CB y se realizará las operación sin el uso de autenticación.
        - :param opcional `service`: Se puede indicar al servicio del que se recoge los datos. Sino se indica, usará el que haya inicializado en el objeto authManager. Sino dispone ninguno de los dos definidos, lanzará un ValueError indicando que necesita definirse el servicio.
        - :param opcional `subservice`: Se puede indicar al subservicio al que se le quiere enviar los datos. Sino se indica, usará el que haya inicializado en el objeto authManager. Sino dispone ninguno de los dos definidos, lanzará un ValueError indicando que necesita definirse el subservicio.
        - :param opcional `limit`: Se establece un límite de recogida de datos en cada tramo. Internamente se usa un mecanismo de paginación y se van solicitando al context broker los datos en tramos. Si no se especifica, enviará la petición al Context Broker sin el limit definido, por lo tanto se aplirá el valor de limit por defecto, tiene un valor de 100.
        - :param opcional `type`: Se establece la recogida de un tipo de datos. Si no se especifica, enviará la petición al Context Broker sin el type definido, por lo tanto los datos no serán filtrados por el tipo de entidad.
        - :param opcional `orderBy`: Se establece un orden en la recogida de datos. Si no se especifica, enviará la petición al Context Broker sin el orderBy definido, por lo tanto aplicará el orden por defecto en el Context Broker (i.e. orden de fecha de creación de la entidades)
        - :param opcional `q`: Se establece un filtro de datos en función del valor de los atributos especificados. Si no se especifica, enviará la petición al Context Broker sin el q definido, por lo tanto los datos no serán filtrados por estos atributos.
        - :param opcional `mq`: Se establece un filtro de datos en función de los metadatos definidos en este parámetro. Si no se especifica, enviará la petición al Context Broker sin el mq definido, por lo tanto los datos no serán filtrados por los metadatos.
        - :param opcional `georel`: Cuando se define un filtro de datos por geolocalización, se ha de especificar en georel la relación espacial que se va a utilizar en el filtrado. Se pueden consultar los diferentes valores de georel en [NGSIv2 API](http://telefonicaid.github.io/fiware-orion/api/v2/stable)
        - :param opcional `geometry`: Cuando se define un filtro de datos por geolocalización, se ha de especificar un tipo de dibujo que se utiliza para resolver el filtrado. Se pueden consultar los diferentes valores de geometry en [NGSIv2 API](http://telefonicaid.github.io/fiware-orion/api/v2/stable)
        - :param opcional `coords`: Cuando se define un filtro de datos por geolocalización, se ha de especificar una lista de coordenadas geograficas separadas por coma. Se pueden consultar los diferentes valores de coords en [NGSIv2 API](http://telefonicaid.github.io/fiware-orion/api/v2/stable)
        - :param opcional `id`: Se establece un filtro por Identificador. Si no se especifica, enviará la petición al Context Broker sin el id definido, por lo tanto los datos no serán filtrados por identificador.
        - :param opcional `options`: Lista de opciones que recibe el Context Broker. Se pueden ver las opciones disponibles en [API NGSIv2 de Orion](https://github.com/telefonicaid/fiware-orion/blob/master/doc/manuals/orion-api.md#list-entities-get-v2entities)  
        - :raises [ValueError](https://docs.python.org/3/library/exceptions.html#ValueError): Se lanza cuando le falta algún argumento o inicializar alguna varibale del objeto cbManager, para poder realizar la autenticación o envío de datos.
        - :raises FetchError: Se lanza cuando el servicio de Context Broker, responde con un error concreto.
        - :return: array de datos cuyos elementos son objeto que representan entidades, según el formato descrito en la sección
          "JSON Entity Representation" de la [NGSIv2 API](https://fiware.github.io/specifications/ngsiv2/stable/)
    - `delete_entities`: Función que borra entidades del Context Broker. Se le pasa el authManager, que ya dispone de un listado con todos los tokens por subservicio y usa el correspondiente para realizar la llamada al Context Broker. Si no se dispone de token o ha caducado, se solicita nuevo y luego recoge los datos. Esta función busca en el Context Broker todas las entidades que coincidan con el filtrado que se pasa por parámetro y tras recoger las entidades, se genera un batch update de tipo `delete` por id y tipo de entidad de cada una de las entidades.
        - :param opcional `auth`: Se le proporciona el authManager, que tiene las credenciales por si ha de solicitar un token y dispone del listado de tokens asociado. Si no se define en la llamada a la función, se asume que no hay IDM asociado al CB y se realizará las operación sin el uso de autenticación.
        - :param opcional `service`: Se puede indicar al servicio del que se eliminan los datos. Sino se indica, usará el que haya inicializado en el objeto authManager. Sino dispone ninguno de los dos definidos, lanzará un ValueError indicando que necesita definirse el servicio.
        - :param opcional `subservice`: Se puede indicar al subservicio del que se eliminan los datos. Sino se indica, usará el que haya inicializado en el objeto authManager. Sino dispone ninguno de los dos definidos, lanzará un ValueError indicando que necesita definirse el subservicio.
        - :param opcional `limit`: Se establece un límite de recogida de datos en cada tramo. Internamente se usa un mecanismo de paginación y se van solicitando al context broker los datos en tramos. Si no se especifica, enviará la petición al Context Broker sin el limit definido, por lo tanto se aplirá el valor de limit por defecto, tiene un valor de 100.
        - :param opcional `type`: Se establece la recogida de un tipo de datos. Si no se especifica, enviará la petición al Context Broker sin el type definido, por lo tanto los datos no serán filtrados por el tipo de entidad.
        - :param opcional `q`: Se establece un filtro de datos en función del valor de los atributos especificados. Si no se especifica, enviará la petición al Context Broker sin el q definido, por lo tanto los datos no serán filtrados por estos atributos.
        - :param opcional `mq`: Se establece un filtro de datos en función de los metadatos definidos en este parámetro. Si no se especifica, enviará la petición al Context Broker sin el mq definido, por lo tanto los datos no serán filtrados por los metadatos.
        - :param opcional `georel`: Cuando se define un filtro de datos por geolocalización, se ha de especificar en georel la relación espacial que se va a utilizar en el filtrado. Se pueden consultar los diferentes valores de georel en [NGSIv2 API](http://telefonicaid.github.io/fiware-orion/api/v2/stable)
        - :param opcional `geometry`: Cuando se define un filtro de datos por geolocalización, se ha de especificar un tipo de dibujo que se utiliza para resolver el filtrado. Se pueden consultar los diferentes valores de geometry en [NGSIv2 API](http://telefonicaid.github.io/fiware-orion/api/v2/stable)
        - :param opcional `coords`: Cuando se define un filtro de datos por geolocalización, se ha de especificar una lista de coordenadas geograficas separadas por coma. Se pueden consultar los diferentes valores de coords en [NGSIv2 API](http://telefonicaid.github.io/fiware-orion/api/v2/stable)
        - :param opcional `id`: Se establece un filtro por Identificador. Si no se especifica, enviará la petición al Context Broker sin el id definido, por lo tanto los datos no serán filtrados por identificador.
        - :param opcional `options_get`: Cadena de opciones separadas por coma, que recibe el Context Broker cuando va recoger las entidades a eliminar. Se pueden ver las opciones disponibles en [API NGSIv2 de Orion](https://github.com/telefonicaid/fiware-orion/blob/master/doc/manuals/orion-api.md#list-entities-get-v2entities).  
        - :param opcional `options_send`: Lista de opciones que recibe el Context Broker cuando va a eliminar las entidades. Se pueden ver las opciones disponibles en [API NGSIv2 de Orion](https://github.com/telefonicaid/fiware-orion/blob/master/doc/manuals/orion-api.md#update-post-v2opupdate). En el caso de que la opcion `flowControl` se especifique dentro de este parámetro, un `cb_flowcontrol` (en la inicializción de cbManager) a `False` se ignora, quedanco como si `cb_flowcontrol` se hubiese establecido a `True`.
        - :raises [ValueError](https://docs.python.org/3/library/exceptions.html#ValueError): Se lanza cuando le falta algún argumento o inicializar alguna varibale del objeto cbManager, para poder realizar la autenticación o envío de datos.
        - :raises FetchError: Se lanza cuando el servicio de Context Broker, responde con un error concreto.

- Clase `normalizer`: Esta clase en encarga de normalizar cadenas unicode, reemplazando o eliminado cualquier caracter que no sea válido como parte de un ID de entidad NGSI.
   - `__init__`: constructor de objetos de la clase.
      - :param opcional `replacement`: define el carácter de reemplazo que sustituirá a todos los caracteres prohibidos (`&`, `?`, `/`, `#`, `<`, `>`, `"`, `'`, `=`, `;`, `(`, `)`). Esta lista de caracteres se ha extraido de https://github.com/telefonicaid/fiware-orion/blob/master/doc/manuals/orion-api.md#general-syntax-restrictions
      - :param opcional `override`: diccionario de pares "caracter prohibido": "carácter reemplazo", que permite especificar un reemplazo personalizado para caracteres especiales particulares. Si se usa como carácter reemplazo `""` o `None`, el caracter prohibido se borra en lugar de reemplazarse.
    - `__call__`: Función que ejecuta el reemplazo de los caracteres especiales. 
      - :param: obligatorio `text`: Cadena de texto a normalizar. El normalizador devuelve una nueva cadena de texto con estos cambios:
        - Convierte los caracteres acentuados (á, é, í, ó, u) en sus variantes sin acento.
        - Elimina otros caracteres unicode no disponibles en ascii.
        - Elimina códigos de control ascii.
        - Reemplaza los caracteres prohibidos por el caracter de reemplazo (por defecto `-`, puede cambiarse con los overrides que se indican en el constructor)
        - Reemplaza todos los espacios en blanco consecutivos por el carácter de reemplazo.
        - NOTA: Esta función no recorta la longitud de la cadena devuelta a 256 caracteres, porque el llamante puede querer conservar la cadena entera para por ejemplo guardarla en algún otro atributo, antes de truncarla.

- Clase `iotaManager`: En esta clase están las funciones relacionadas con el agente IoT.

  - `__init__`: constructor de objetos de la clase.
    - :param obligatorio `sensor_id`: El ID del sensor.
    - :param obligatorio `api_key`: La API key correspondiente al sensor.
    - :param obligatorio `endpoint`: La URL del servicio al que se le quiere enviar los datos.
    - :param opcional `sleep_send_batch`: Es el tiempo de espera entre cada envío de datos en segundos (default: 0).
  - `send_http`: Función que envía un archivo en formato JSON al agente IoT por petición HTTP.
    - :param obligatorio: `data`: Datos a enviar. La estructura debe tener pares de elementos clave-valor (diccionario).
    - :raises [TypeError](https://docs.python.org/3/library/exceptions.html#TypeError): Se lanza cuando el tipo de dato es incorrecto.
    - :raises [ConnectionError](https://docs.python.org/3/library/exceptions.html#ConnectionError): Se lanza cuando no puede conectarse al servidor.
    - :raises FetchError: se lanza cuando se produce un error en en la solicitud HTTP.
    - :return: True si el envío de datos es exitoso.
  - `send_batch_http`: Función que envía un conjunto de datos en formato JSON al agente IoT por petición HTTP.
    - :param obligatorio: `data`: Datos a enviar. Puede ser una lista de diccionarios o un DataFrame.
    - :raises SendBatchError: Se levanta cuando se produce una excepción dentro de `send_http`. Atrapa la excepción original y se guarda y se imprime el índice donde se produjo el error.


Algunos ejemplos de uso de `normalizer`:

```
# Reemplazar los espacios por "+", al estilo "url encoding".
# El resto de caracteres especiales, sustituirlos por el carácter
# de reemplazo por defecto.
norm = tc.normalizer(override={" ": "+"})
norm("text (with spaces)") # devuelve "text+-with+spaces-"

# Eliminar directamente todos los caracteres especiales,
# dejando solo los espacios (reemplazados por "-")
norm = tc.normalizer(replacement="", override={" ": "-"})
norm("text (with spaces)") # devuelve "text-with-spaces"
```

La librería además proporciona [context managers](https://docs.python.org/3/reference/datamodel.html#context-managers) para abstraer la escritura de entidades en formato NGSIv2 a distintos backends (`store`s). Estos son:

- `orionStore`: Genera un store asociado a una instancia particular de `cbManager` y `authManager`. Todas las entidades que se envíen a este store, se almacenarán en el cbManager correspondiente.
    - todos los parámetros son idénticos a los de la función send_batch de la clase cbManager.
    - :return: un `callable` que recibe una lista de entidades y las envía a la función `send_batch` del `cb` especificado. Como tal, puede lanzar cualquiera de las excepciones que lanza la función `send_batch` de la clase `cbManager`.

- `sqlFileStore`: Genera un store asociado a un fichero local. Todas las entidades que se envíen a este store se almacenarán como órdenes SQL `INSERT` en el fichero local. Además de los atributos de la entidad, cada `INSERT` añadirá las columnas `fiwareservicepath` y `recvtime` para ser consistente con el formato típico de tabla histórica de entidad, y no dar error de inserción (ya que esas columnas suelen ser NOT NULL).
    - :param: `subservice`: Nombre de subservicio a escribir en la columna `fiwareservicepath`
    - :param: `schema` opcional: Nombre del schema a utilizar en los INSERT. Por defecto es `":target_schema"`, por lo que es al ejecutar el comando `psql` cuando se debe especificar con `-v target_schema=...`. Pero se le puede dar aquí un valor explícito.
    - :param: `namespace` opcional: Prefijo opcional para los nombres de tabla generados a partir del entityType. Si se especifica, el nombre de tabla se construye como `f"{namespace}_{entitytType.lower()}"`
    - :param: `table_names` opcional: mapeo de nombre de entidad a nombre de tabla. Permite especificar nombres de tabla por entidad distintos a los por defecto.
        - si `table_name[entityType]` existe y es un string `!= ""`, se usa como nombre de tabla para el tipo de entidad.
        - si `table_name[entityType]` no existe, se usa el nombre de tabla por defecto (`f"{entitytType.lower()}"` o `f"{namespace}_{entitytType.lower()}"`)
        - si `table_name[entityType]` existe y es *falsy* (`None`, `""`, etc), las entidades de ese tipo no se escriben al fichero SQL.
    - :param: `chunk_size` opcional: máximo número de líneas a incluir en un solo `INSERT`. Default=10000
    - :param: `append` opcional: en caso de que el fichero exista, `append=True` añade los INSERT mientras que `append=False` sobrescribe el fichero. Default False.
    - :param `replace_id` opcional: diccionario `tipo de entidad` => `lista de atributos replace_id`.
        Reemplaza el ID de las entidades del tipo o tipos especificados, por un valor construido a partir de la lista de atributos indicados en este parámetro, separados por `_`.
        Imita el comportamiento del atributo `replaceId` de los FLOW_HISTORIC de URBO DEPLOYER, para poder usar este *store* en ETLs que alimenten *singletons*.
    - :return: un `callable` que recibe una lista de entidades y las escribe como instrucciones sql `INSERT` en el fichero especificado.

El modo de uso de cualquiera de los context managers es idéntico:

```python
# En primer lugar, se selecciona el store en función del criterio que convenga.
# por ejemplo, en este caso, una variable `use_file_store`
if use_file_store == True:
    new_store = lambda: tc.sqlFileStore(
        path=Path("my_file_name.sql"),
        subservice="/my_subservice"
    )
else:
    new_store = lambda: tc.orionStore(
        cb=my_cb,
        auth=my_auth,
        subservice="/my_subservice"
    )

# A partir de aqui, el código sería independiente del tipo de store usado
with new_store() as store:
    entities = ...
    store(entities)
```

## Testing

Se recomienda trabajar en un virtualenv, en el que se instalarán las dependencias
de la librería:

```
$ virtualenv venv
$ source venv/bin/activate
$ (venv)$ pip install -e python-lib/tc_etl_lib  # directorio en el que está setup.py de la lilbreria
```

Adicionalmente, instalar las herramientas necesarias para ejecutar los tests:

```
pip install pytest==7.2.0 coverage==7.0.5
```

Para ejecutar un solo fichero de tests:

```
$ (venv)$ pytest python-lib/tc_etl_lib/tc_etl_lib/test_store.py
================================================================ test session starts =================================================================
platform linux -- Python 3.9.2, pytest-7.2.0, pluggy-1.0.0
rootdir: /home/fermin/src/etl-framework/python-lib/tc_etl_lib
collected 7 items                                                                                                                                    

python-lib/tc_etl_lib/tc_etl_lib/test_store.py .......                                                                                         [100%]

================================================================= 7 passed in 0.32s ==================================================================
```

Para ejecutar todos los tests se puede utilizar `pytest` sin parámetros:

```
$ (venv)$ pytest
================================================================ test session starts =================================================================
platform linux -- Python 3.9.2, pytest-7.2.0, pluggy-1.0.0
rootdir: /home/fermin/src/etl-framework/python-lib/tc_etl_lib
collected 7 items                                                                                                                                    

python-lib/tc_etl_lib/tc_etl_lib/test_store.py .......                                                                                         [100%]

================================================================= 7 passed in 0.32s ==================================================================
```

(**FIXME [#3](https://github.com/telefonicasc/etl-framework/issues/3)**: ahora mismo solo tenemos un fichero de tests y queda un poco raro la
ejecución con `pytest` a secas, pero cuando se acometa ese issue tendremos ya más).

Se pueden obtener estadísticas de cobertura de código de la siguiente forma:

```
$ (venv)$ cd python-lib/tc_etl_lib
$ (venv)$ coverage run -m unittest
.......
----------------------------------------------------------------------
Ran 7 tests in 0.041s

OK

$ (venv)$ coverage report
Name                       Stmts   Miss  Cover
----------------------------------------------
tc_etl_lib/__init__.py         3      0   100%
tc_etl_lib/auth.py            95     76    20%
tc_etl_lib/cb.py             178    141    21%
tc_etl_lib/store.py           80      4    95%
tc_etl_lib/test_store.py      47      0   100%
----------------------------------------------
TOTAL                        403    221    45%
```

## Changelog

0.10.0 (November 29th, 2023)

- Add: new class `iotaManager` to deal with IoT Agent interactions, with methods `send_http` and `send_batch_http`([#55](https://github.com/telefonicasc/etl-framework/issues/55))

0.9.0 (May 16th, 2023)

- Add: new class `normalizer` to clean up text strings to be used as NGSI entity IDs, by replacing or removing forbidden characters ([#54](https://github.com/telefonicasc/etl-framework/pull/54))

0.8.0 (March 22nd, 2023)

- Add: new optional parameter called `replace_id` in sqlFileStore context manager ([#58](https://github.com/telefonicasc/etl-framework/pull/58))

0.7.0 (December 23rd, 2022)

- Add: new stores for saving entity batches, `orionStore` and `sqlFileStore` ([#46](https://github.com/telefonicasc/etl-framework/pull/46))

0.6.0 (December 15th, 2022)

- Add: new optional parameter called `batch_size` in cbManager constructor ([#37](https://github.com/telefonicasc/etl-framework/issues/37))

0.5.0 (November 23rd, 2022)

- Add: new optional parameter called `options` in get_entities, get_entities_page and send_batch ([#38](https://github.com/telefonicasc/etl-framework/issues/38))
- Add: new optionals parameters called `options_send` and `options_get` in delete_entities ([#38](https://github.com/telefonicasc/etl-framework/issues/38))
- Fix: cleaner logs, avoiding printing warnings in the case of unsecure CB API calls

0.4.0 (August 31st, 2022)

- Add: new optional parameter called `service` in get_entities, get_entities_page, delete_entities and send_batch
- Add: new function set_token, to set specific token bypassing IDM negotiation
- Add: new function delete_entities, to remove entities from Context Broker ([#14](https://github.com/telefonicasc/etl-framework/issues/14))
- Add: new parameter actionType in the function send_batch to define the action (append, appendStrict, update, delete or replace) ([#16](https://github.com/telefonicasc/etl-framework/issues/16))
- Fix: parameter authManager becomes optional in get_entities, get_entities_page, delete_entities and send_batch

0.3.0 (August 2nd, 2022)

- Add: get_auth_token_service function to get domain tokens from IDM

0.2.0 (July 13th, 2022)

- Add: get_entities function to get entities with internal pagination ([#5](https://github.com/telefonicasc/etl-framework/issues/6))
- Add: block control to send_batch function to overcome CB limitation (new cbManager constructor optional param block_size) ([#6](https://github.com/telefonicasc/etl-framework/issues/6))
- Add: more filters (q, mq, georel, geometry, coords, id) to get_entities_page function ([#13](https://github.com/telefonicasc/etl-framework/issues/13))

0.1.0 (April 26th, 2022)

- Initial version
