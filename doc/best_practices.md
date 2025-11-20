# <a name="top"></a>ETLS - Recomendaciones y directrices - EPG (Enhancement Proposals and Guidelines)

* [Versión](#version)
* [Estructura de ficheros](#estructura-de-ficheros)
* [Uso de la librería común](#tc_etl_lib)
* [Log format](#logging-format)
* [Logging niveles y f-strings](#logging-string)
* [Configuración de la ETL](#etl-config)
* [Ficheros CSV](#etl-csv)
* [Terminación controlada y manejo de Errores](#etl-control-errors)

## <a name="version"></a>Versión de Python

Para los desarrollos de ETLs se usa la versión >= 3.8 de Python. [Ref.](https://docs.python.org/3.8/)

Para ejecutar las ETLs en python, se hace uso de Virtual Enviroment, referenciado como venv. Este entorno virtualizado, permite trabajar con python en un entorno controlado, donde poder instalar las librerías que se necesitan en cada momento, independiente de las librerías generales que tenga el sistema instalado. [Ref.](https://docs.python.org/3/library/venv.html)

## Estructura de ficheros

La ETL estará contenida en un directorio, cuyos contenidos serán:

* `README.md`: documentando la ETL con al menos las siguientes secciones: 
    - Descripción funcional de la ETL.
    - Requisitos de la ETL (versión de python, versión de pip, etc..)
    - Instalación de la ETL. Como crear el entorno virtual de python, lanzar la instalación de librerías con pip y ejecución de la etl (si quiere parámetros en la linea de comandos, etc..).
    - Configuración de la ETL. Lista de variables en entorno (y valores por defecto o comportamiento en el caso de no encontrarse la variable de entorno) que usa la ETL
    - Ficheros de carga o adicionales. En ocasiones las ETLs se usan para cargar ficheros de datos concretos. Se debe indicar que tipos de ficheros son, formato, etc..
    - Ejecución de la ETL. Como lanzar la ETL, en caso de que sea por comando o si es necesario programar algún Job de Jenkins, especificar los pasos para crear el job de jenkins.
    - Resultado o Ejemplos. Si es posible algunos ejemplos genéricos de ejecución y resultado esperado.
    - Referencias. Se pueden adjuntar algunas referencias como Tutorial de virtualenv, etc..
* `requirements.txt`: dependencias de la ETL. Especialmente significativa es la [tc_etl_lib](#tc_etl_lib), en el caso de ETLs que
  interactúen con la plataforma. **NOTA:** no se deberían usar `requirements.txt` "generales" en el directorio que contiene el conjunto de ETLs.
* ~`config.example.cfg`: fichero de configuración de ejemplo, según el formato descrito [en la siguiente sección](#etl-config). **Téngase
  precaución de ofuscar cualquier tipo de información sensible que pueda haber en esta configuración, pe. IPs, passwords, etc.**~ DEPRECADO.
  En la actualidad, las buenas prácticas recomiendan [usar configuraion via variables de entorno en vez de con fichero](#etl-config). No obstante,
  en el caso de que por alguna razón haya que usar un fichero de configuración, ha de seguirse esta recomendación.
* `config.py`: fichero de configuración con al recogida de las [variables de entorno](#etl-config). 
* `etl.py`: el fichero ejecutable de la ETL
* Otros ficheros `.py` que la ETL pueda necesitar. Idealmente, si la ETL es sencilla, no deberían hacer falta ficheros `.py` extra

## <a name="tc_etl_lib"></a>Uso de la librería común

Se ha creado una librería (`tc_etl_lib`) que recoge las funciones más comunes utilizadas en las ETLs. Esta librería engloba llamadas relacionadas con la autentificación y comunicación con el Context Broker. Siempre que sea posible, se debería de utilizar esta librería en su última versión.

Las funciones que actualmente soporta la librería son:
- Librería tc_etl_lib
    - modulo `auth`: Módulo que incluye funciones relacionadas con la autenticación.
        -   `get_auth_token_subservice`: Función identificarse mediante usuario y contraseña y recibir un token con el que operar.
    - modulo `cb`: Módulo que incluye funciones relacionadas con la comunicación con el Context Broker.
        -   `send_batch`: Función que envía un lote de entidades al Context Broker. Recibe un listado con todos los tokens por subservicio y usa el correspondiente para realizar la llamada al Context Broker. Si no se dispone de token o ha caducado, se solicita o renueva el token según el caso y luego envía los datos.
        -   `get_entities_page`: Función que permite la recogida de datos del Context Broker. Permite el uso de ciertos parámetros como offset, limit, orderBy y type para filtrar la recogida de datos.
    - modulo `iota`: Módulo que incluye funciones relacionadas con la comunicación con el agente IoT.
        - `send_http`: Función que envía un JSON al agente IoT por una petición HTTP.
        - `send_batch_http`: Función que envía un un conjunto de datos en formato JSON al agente IoT por una petición HTTP. Puede recibir una lista de diccionarios o un DataFrame. En el caso de DataFrames, convierte cada fila en un diccionario y los envía uno por uno cada cierto tiempo definido en `sleep_send_batch`.

Se puede encontrar más detalles de la librería en la documentación de esta. [Ref.](../python-lib/tc_etl_lib/README.md)

## <a name="logging-format"></a> Logging formato

Se recomienda mantener el mismo formato de log en todas las ETLs, ya que eso facilita el uso de las herramientas comunes para procesar esos logs a posteriori.

Para ello se recomienda el siguiente formato:

```
time=<hora:min:seg:mseg> | lvl=<nivel> | comp=<componente>| op=<archivo>[<linea>]:<function> | msg=mensaje

```

El formato de log, se puede configurar de la siguiente manera:

``` Python
import logging
import os

# get ETL_LOG_LEVEL var environment
# note this is an string which is translated to the actual logLevel
# number (eg. logging.INFO, logging.DEBUG, etc.) by logging.getLevelName() function
logLevelString = os.getenv('ETL_LOG_LEVEL', 'INFO')

# sets the logging configuration
logging.basicConfig(
    level=logging.getLevelName(logLevelString),
    format="time=%(asctime)s | lvl=%(levelname)s | comp=ETL-xxxx | op=%(name)s:%(filename)s[%(lineno)d]:%(funcName)s | msg=%(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

# get logger to use it
logger = logging.getLogger()

# log something
logger.info('lore ipsum...')
```

reemplazando 'ETL-xxx' por el nombre que se le quiera dar a la ETL.

Se recomienda recoger el nivel de log a través de una variable de entorno llamada `ETL_LOG_LEVEL` como se muestra en el ejemplo más arriba, con valor por defecto `INFO`. Los diferentes valores soportados por logging son `CRITICAL`, `FATAL`, `ERROR`, `WARN`, `WARNING`, `INFO`, `DEBUG`, `NOTSET`.

## <a name="logging-string"></a> Logging niveles y f-strings

Es recomendable, cuando estés codificando agrupar la información del log, en los diferentes niveles, pensando en que información, puede ser prescindible, pero de ayuda (DEBUG), información de ejecución (INFO), información importante a tener en cuenta (WARNING) o errores (ERROR). 

Teniendo en cuenta que mensajes van a ser importantes que sean visibles en LIVE (producción), cuando se está desarrollando o cuando se está depurando.

Hay diferentes maneras de mostrar una cadena en el log. Se puede usar los mecanismos "old-school": %-formatting, str.format() o f-Strings, el mecanismo de f-String se incorporó desde Python 3.6 [Ref.PEP 498](https://peps.python.org/pep-0498/). A continuación mostramos ejemplos de cada uno:

`%-formatting`

``` Python
logger.info("Hello, %s. You are %s." % (name, age))
```

`str.format`

``` Python
logger.info("Hello, {1}. You are {0}.".format(age, name))
```

`f-String`
``` Python
logger.info(f"Hello, {name}. You are {age}.")
```

El mecanismo de f-String a la hora de formatear los strings (y en este caso usarlo en el log), es más rápido y comprensible que los mecanismos anteriores. Por lo tanto f-String es el recomendado a usar.

## <a name="etl-config"></a> Configuración de la ETL

Cada ETL tiene su propia configuración, dependiendo de su necesidades a nivel funcional. Es importante parametrizar todo lo posible y que las ETLs sean lo más flexible posible.

El mecanismo recomendado para la configuración de una ETL es del *variables de entorno*.

Se recomienda utilizar grupos de configuración que agrupen variables de un mismo tipo (pe. el grupo `ENVIRONMENT` para la configuración que tenga que ver con el entorno de despliegue, `SETTINGS` para otras configuraciones, etc.) y el uso de nombre con la siguiente estructura:

```
ETL_<nombre o mnemónico de la ETL>_<grupo de configuracion>_<elemento del grupo>
```

Notas adicionales:

* Solo se permiten mayúsculas y el underscore en el nombre de la variable
* Con respecto a `<elemento del grupo>` se permite el uso de `_` si se trata de una palabra compuesta

Por ejemplo:

```
ETL_MYETL_ENVIRONMENT_PROTOCOL=http
ETL_MYETL_ENVIRONMENT_ENDPOINT_CB=<endpoint_cb>:<port>
ETL_MYETL_ENVIRONMENT_ENDPOINT_KEYSTONE=<endpoint_keystone>:<port>
ETL_MYETL_ENVIRONMENT_SERVICE=dip_castellon
ETL_MYETL_ENVIRONMENT_SUBSERVICE=/energia
ETL_MYETL_ENVIRONMENT_USER=admin_castellon
ETL_MYETL_ENVIRONMENT_PASSWORD=xxx
```

A la hora de recoger esa configuración, se utiliza [`os.getenv()`](https://docs.python.org/3/library/os.html#os.getenv), existien do distintas variantes posibles.

Con comprobacion de existencia:

``` Python
import os

protocol = os.getenv('ETL_MYETL_ENVIRONMENT_PROTOCOL')
endpoint_cb = os.getenv('ETL_MYETL_ENVIRONMENT_ENDPOINT_CB')
if protocol is None or endpoint_db is None:
    logger.error('Some critical configuration is missing')
    sys.exit()
```

Con uso de default si la variable no existe:

``` Python
import os

protocol = os.getenv('ETL_MYETL_ENVIRONMENT_PROTOCOL', 'http')
endpoint_cb = os.getenv('ETL_MYETL_ENVIRONMENT_ENDPOINT_CB', '<endpoint_cb>:<port>')
```

Es habitual y recomendable, que agrupemos las variables de entorno de configuración en un fichero de configuración, llamado `config.py`. Este fichero contiene el mecanismo de recogida de variables de entorno y las asocia a variables que luego se usarán en el ETL, así como establece los valores por defecto en el caso de que la variable no se encuentre. Es reomendable que las comprobaciones relacionadas con la recogida de parámetros, se hagan en este punto y así quede desvinculado luego del propio código de la ETL, a menos que esas comprobaciones dependan de la propia lógica de la etl.

Ejemplo de fichero `config.py`:

``` Python
import os

protocol = os.getenv('ETL_MYETL_ENVIRONMENT_PROTOCOL', 'http')
endpoint_cb = os.getenv('ETL_MYETL_ENVIRONMENT_ENDPOINT_CB', '<endpoint_cb>:<port>')
```

Uso del fichero `config.py` en `etl.py`

``` Python
import config

logger.info(f'Protocol {config.protocol}')
```

Ejemplo de como aplicar cierta lógica en la recogida de parámetros. Queremos recoger un parámetro que ha de ser booleano en python para luego usarlo en el código de la etl.py. La lógica de comprobación, se recomienda incorporarla en el propio fichero config.py:

fichero `config.py`:
``` Python
import os

check_it = (os.getenv(f"ETL_MYETL_ENVIRONMENT_CHECK_IT", "False").lower() in ('true','t','yes','on', '1'))
```

fichero `etl.py`:
``` Python
import config
if (config.check_it):
    logger.info('Check it!')
else:
    logger.info('Nothing to do')
```

Si bien el mecanismo preferido para configurar ETLs son las variables de entorno, se permite también la configuración vía *ficheros
de configuración* en casos excepcionales. En concreto:

* ETLs legacy anteriores a la creación de estas buenas práticas (si bien estas ETLs legacy habrían de ser migradas progresivamente)
* Configuraciones que sería muy complejo especificar en la forma de variable de entorno. Por ejemplo, ficheros JSON de credenciales
  generados por algunos servicios (como Google) o ficheros grandes de datos estáticos, necesarios para la configuración de la ETL.
  
Es un requisito en estos casos que los ficheros de configuración que se usen sean tal cual estén en los repositorios, evitando
cualquier modificación posterior en tiempo de ejecución del job (pe. mediante comandos `sed` para cambiar algún tipo de placeholder
en la configuración). Cuando se detecten casos como estos en ETLs legacy, considerar su paso a env vars (tipicamente se ha venido
usando hasta ahora con configuración sensible, como passwords, etc.).

Otro requisito es que la ETL **permita especificar la ruta a esos ficheros de configuración**, si los necesita. Es decir, la ETL no debe simplemente asumir que sus ficheros de configuración van a estar en el mismo directorio donde esté el código, o en el directorio de trabajo desde el que se ejecute la ETL. En su lugar, debe aceptar algún parámetro o variable de entorno que le indique la ruta a dichos ficheros de configuración.

## <a name="etl-csv"></a> Manejo de CSV en las ETLs

Para la recogida de datos de CSV en las ETLs, se recomienda usar la librería `pandas`, que dispone de mecanismos de captación de datos, entre ellos importanción de datos desde CSV. [Ref.](https://pandas.pydata.org/docs/user_guide/io.html#io-read-csv-table)

Para la lectura de un csv, se necesita los siguientes parámetros:
- csv_name: nombre del csv 
- sep: separador del csv
- usecols: indices de las columnas
- names: array con el nombre de las columnas.
- encoding: codificación del fichero

Ejemplo de uso

``` Python
# import library
import pandas as pd

# define columns data
csv_municipality_column_cif = "CIF"
csv_municipality_column_name= "NAME"
csv_municipality_column_municipality= "MUNICIPALITY"

csv_municipality_columns_names = [
    csv_municipality_column_cif,
    csv_municipality_column_name,
    csv_municipality_column_municipality]

csv_municipality_columns_index = [0, 1, 2]

# read data from csv
df = pd.read_csv(csv_name, sep=",", dtype="object",
                        usecols=csv_municipality_columns_index, names=csv_municipality_columns_names,
                        header=None, encoding="UTF-8")

# use data
for index, row in df.iterrows():
    # Check if csv contains header
    if row[csv_municipality_column_cif] == csv_municipality_column_cif:
        logger.warning(f'Line {index + 1} ignored: This line contains the header')
    else:
        logger.warning(f'Data CIF: {row[csv_municipality_column_cif]}')
        ...
```
## <a name="etl-control-errors"></a> Terminación Controlada y Manejo de Errores (ETL)

Es crucial que cualquier proceso ETL o script de larga duración finalice de forma controlada y predecible. Una terminación descontrolada puede dejar recursos abiertos, datos inconsistentes o, peor aún, ocultar la causa raíz de un fallo.

Un buen control de excepciones garantiza tres cosas:

- Robustez: Permite manejar errores esperados (como fallos de red o datos inválidos) de manera específica.
- Observabilidad: Proporciona un registro claro de la causa, permitiendo la depuración y la automatización de reintentos.
- Gestión de Recursos: Asegura que los recursos (conexiones de bases de datos, archivos abiertos, etc.) se cierren correctamente.

Para lograr una terminación controlada, se recomienda encapsular el código en una estructura `try...except...finally` y utilizar códigos de salida (sys.exit) específicos para clasificar la naturaleza del fallo. Todo el flujo de ejecución principal de la ETL debe estar contenido en este patrón (`try...except...finally`) para garantizar una salida controlada y un manejo consistente de los recursos.

Un ejemplo de uso sería: 

```python
import sys
import logging
# Assume logger is initialized somewhere
logger = logging.getLogger(__name__)

# NOTE: NetworkException and CustomAppException should be user-defined classes.

try:
    # Main code for extraction, transformation, and loading (ETL)
    # Functions that might raise known exceptions are executed here
    logger.info("Starting ETL process...")
    # <etl working code> 
    
except NetworkException as ne:
    # Exit Code 2: External Resource/Connectivity Failure (Retryable)
    # Network or API errors: log specific details for debugging.
    try:
        # Try to use specific attributes of your NetworkException class
        logger.error(f'⚠️ Network error: {ne.msg} | URL: {ne.url} | Status: {ne.status_code}')
    except AttributeError:
        # Fallback if exception object is missing fields (safeguard)
        logger.error(f'⚠️ Unexpected network error format: {ne}')
    sys.exit(2)
    
except CustomAppException as cae:
    # Exit Code 3: Application Logic Failure (Non-retryable without changes)
    # Data validation errors, missing configuration, business logic issues.
    logger.error(f'🚫 Application error: {cae.msg}')
    sys.exit(3)
    
except Exception:
    # Exit Code 1: Unexpected/Unknown Failure (Needs intervention)
    # Catch any other unhandled error. Include the full traceback.
    logger.exception('💥 Unexpected error running the ETL. Review traceback.')
    sys.exit(1)
    
finally:
    # The 'finally' block executes ALWAYS (whether there was an exception or not)
    # Essential for releasing external resources, ensuring a clean termination.
    logger.info("Releasing resources...")
    # source.close() 
    # db_connection.close()
```

Los Códigos de Salida Específicos, es una best practice que permite a los sistemas externos (shells, orquestadores, etc.) entender la naturaleza del fallo sin leer los logs. 

| Código (N) | Significado Estándar | Clasificación del Error | Recomendación de Automatización |
|-----------:|----------------------|-------------------------|---------------------------------|
| 0          | Éxito | N/A | Continuar |
| 1 | Fallo General | Errores no manejados, bugs... | Parar | Necesita revisión humana |
| 2 | Fallo Transitorio | Red, timeouts, servicio externo caído... |Reintentar después de un tiempo |
| 3+ | Fallo de Lógica/Datos | Errores de validación, configuración... | Parar | La lógica del script o los datos de entrada deben cambiarse |

La ETL de be hacer uso de excepciones más adecuadas, en función del problema que de o quieras controlar.

Puedes utilizar tus propias Excepciones, defindiendo esas excepciones en el código de la etl, ejemplo de definción:

```python
    @dataclass(frozen=True)
    class NetworkException(Exception):
        """Exception raised for network errors.

        Attributes:
            msg: the failure message
            url -- URL the request was sent to
            status_code -- response status code
            text -- response body
        """
        msg: str
        url: str
        status_code: int
        text: str
...
```

Y luego darle uso, por ejemplo:

```python
   if res.status_code != 200:
      raise NetworkException(msg='Failed to get zone manager token (galgus zoneManager)', url=req_url, status_code=res.status_code, text=res.text)
...
```
