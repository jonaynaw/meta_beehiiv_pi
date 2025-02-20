import azure.functions as func
import logging
from .beehiiv_database import (
    fetch_data_from_facebook_api,
    fetch_data_from_beehiiv_api,
    create_db_rows,
    create_db_connection,
    insert_db_data
)

def main(timer: func.TimerRequest) -> None:
    logging.info('Iniciando sincronización de datos...')
    
    try:
        beehiiv_info = fetch_data_from_beehiiv_api()
        facebook_info = fetch_data_from_facebook_api()
        rows = create_db_rows(beehiiv_info, facebook_info)
        connection, cursor = create_db_connection()
        insert_db_data(connection, cursor, rows)
        
        logging.info('Sincronización completada exitosamente')
        
    except Exception as e:
        logging.error(f'Error en la sincronización: {str(e)}')
        raise