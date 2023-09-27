# ETL de ejemplo

## Descripción

Esta ETL es un ejemplo de referencia de ETL siguiendo las [buenas prácticas](../../doc/best_practices.md) descritas en este mismo repositorio.

Puede utilizarse como plantilla (incluyendo este mismo fichero README.md de documentación) para la creación de nuevas ETLs.

## Requisitos
- Versión de Python 3.9.10
- Versión de pip 22.0.4
- [PyPI ETL Dependencies](requirements.txt)

## Instalación

### Instalación del entorno virtual

1. Comprobar si pip está instalado mediante el comando `pip -h`. En caso de no estarlo, instalarlo.
2. Instalar el paquete de virtualenv con `pip install virtualenv`.
3. Crear un entorno virtual mediante siguiente comando: `virtualenv mypath`, donde "mypath" es la ruta donde se quiere
   instalar el entorno.
4. Activar el entorno. En Mac OS y Linux se activa por medio de `source mypath/bin/activate`, mientras que en Windows
   ejecutando `mypath\Scripts\activate`.
5. Finalmente, para desactivar el entorno, ejecutar `deactivate`.

### Instalación de las librerías necesarias con pip

1. Activar el entorno virtual previamente instalado.
2. En la consola donde esté el entorno activado, escribir `pip install requirements.txt`, y especificar la ruta
   correcta.

## Configuración

La ETL se configura mediante variables de entorno. Las variables disponibles son:

- `ETL_LOG_LEVEL`: Nivel de log disponible para la ETL. Los distintos valores soportados son `CRITICAL`, `FATAL`, `ERROR`, `WARN`, `WARNING`, `INFO`, `DEBUG` y  `NOTSET`. Por defecto es `INFO`.
- `ETL_HELLO_WORLD_GRETTING`: Mensaje de saludo usado por la ETL (por defecto `Hello`)
- `ETL_HELLO_WORLD_NAME`: Destinatario del saludo (por defecto `world`)

## Ejecución

Para ejecutar la etl, se realiza con python3 mediante el comando:

```
python3 etl.py
```