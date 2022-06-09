# <a name="top"></a>ETLS - Recomendaciones y directrices - EPG (Enhancement Proposals and Guidelines)

* [Versión](#version)
* [Estructura de ficheros](#estructura-de-ficheros)
* [Uso de la librería común](#tc_etl_lib)
* [Log format](#logging-format)
* [Logging niveles y f-strings](#logging-string)
* [Configuración de la ETL](#etl-config)
* [Ficheros CSV](#etl-csv)

## <a name="version"></a>Versión de Python

Para los desarrollos de ETLs se usa la versión >= 3.8 de Python. [Ref.](https://docs.python.org/3.8/)

Para ejecutar las ETLs en python, se hace uso de Virtual Enviroment, referenciado como venv. Este entorno virtualizado, permite trabajar con python en un entorno controlado, donde poder instalar las librerías que se necesitan en cada momento, independiente de las librerías generales que tenga el sistema instalado. [Ref.](https://docs.python.org/3/library/venv.html)

## Estructura de ficheros

La ETL estará contenida en un directorio, cuyos contenidos serán:

* `README.md`: documentando la ETL con al menos las siguientes secciones: 
    - Descripción funcional de la ETL.
    - Requisitos de la ETL (versión de python, versión de pip, etc..)
    - Instalación de la ETL. Como crear el entorno virtual de python, lanzar la instalación de librerías con pip y ejecución de la etl (si quiere parámetros en la linea de comandos, etc..).
    - Configuración de la ETL. Fichero de configuración, que valores tiene y que configuración se ha de modificar. Normalmente se usa un `config.example.cfg` como ejemplo/plantilla, se realiza una copia de este como config.cfg y dentro se incluyen las diferentes secciones de configuración.
    - Ficheros de carga o adicionales. En ocasiones las ETLs se usan para cargar ficheros de datos concretos. Se debe indicar que tipos de ficheros son, formato, etc..
    - Ejecución de la ETL. Como lanzar la ETL, en caso de que sea por comando o si es necesario programar algún Job de Jenkins, especificar los pasos para crear el job de jenkins.
    - Resultado o Ejemplos. Si es posible algunos ejemplos genéricos de ejecución y resultado esperado.
    - Referencias. Se pueden adjuntar algunas referencias como Tutorial de virtualenv, etc..
* `requirements.txt`: dependencias de la ETL. Especialmente significativa es la `[tc_etl_lib](#tc_etl_lib)`, en el caso de ETLs que
  interactúen con la plataforma.
* `config.example.cfg`: fichero de configuración de ejemplo, según el formato descrito [en la siguiente sección](#etl-config). **Téngase
  precaución de ofuscar cualquier tipo de información sensible que pueda haber en esta configuración, pe. IPs, passwords, etc.**
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

Se puede encontrar más detalles de la librería en la documentación de esta. [Ref.](../python-lib/tc_etl_lib/README.md)

## <a name="logging-format"></a> Logging formato

Se recomienda mantener el mismo formato de log en todas las ETLs, ya que eso facilita el uso de las herramientas comunes para procesar esos logs a posteriori.

Para ello se recomienda el siguiente formato:

```
time=<hora:min:seg:mseg> | lvl=<nivel> | comp=<componente>| op=<archivo>[<linea>]:<function> | msg=mensaje

```

El formato de log, se puede configurar de la siguiente manera:

``` Python
# import logging
import logging

# sets the logging configuration
logging.basicConfig(
    level=logging.DEBUG,
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

reemplazando 'ETL-xxx' por el nombre que se le quiera dar a la ETL

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

Cada ETL tiene su propia configuración, dependiendo de su necesidades a nivel funcional. Es importante parametrizar todo lo posible y que las ETLs sean lo más flexible posible. Para ello, se usa un fichero de configuración `config.cfg` donde viene una configuración básica que es común para todas las ETLs y luego una configuración concreta según las necesidades. Este fichero tiene el siguiente formato:

```
[environment]
protocol = http
endpoint_cb = <endpoint_cb>:<port>
endpoint_keystone = <endpoint_keystone>:<port>
service = dip_castellon
subservice = /energia
user = admin_castellon
password = xxx
```

A la hora de recoger esa configuración, la realizamos con `configparser` [Ref.](https://docs.python.org/3/library/configparser.html)
Configparser te permite la recogida de datos con cierto formato, usando get, getint, getboolean.

``` Python

#import confirparser
import configparser


# Gets the config
config = configparser.ConfigParser()
config.read(module_path.parent.joinpath('config.cfg'))

# Checks if the config has a environment section
if 'environment' not in config.sections():
    logger.error('Environment configuration not found')
    sys.exit()

# Get the config variables
protocol = config.get('environment', 'protocol')
endpoint_cb = config.get('environment', 'endpoint_cb')
```

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
