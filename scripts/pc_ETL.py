import requests
import datetime
from datetime import datetime
import wget
import logging
from dotenv import load_dotenv
from src.utils.logging import Logger
from src.utils.datalake import DataLakeWrapper


class ETL():

    def __init__(self) -> None:
        self._logger = Logger.get_logger()
        self._dlc = DataLakeWrapper().get_client()
        
        
    def run(self, args):
        self._logger.info('Script initialized')
        if any([args.extract, args.transform, args.preprocessing]):
            self._logger.debug('Some parameters passed')
            if args.extract:
                self._extract()
            if args.transform:
                self._transform()
            if args.preprocessing:
                self._preprocessing()
        else:
            self._logger.debug('No parameters passed, performing all steps')
            self._extract()
            self._transform()
            self._preprocessing()
        self._logger.info('Script finished')


        def _extract(self):
        self._logger.info('Performing extract step')
            
            lf = list(open('/home/jose/Dropbox/cefet/IC/pc/copia.txt','r'))
            lf1 = [s.rstrip() for s in lf]
            lf2 = [s.replace(" ", "") for s in lf1]
            lf3 = [datetime.strptime(s, '%d/%m/%Y').date() for s in lf2]
            datas = [i.strftime('%Y%m%d') for i in lf3]
            r1 = [str(r) for r in range(24)]
            r2 = [i.zfill(2) for i in r1]
            datas_horas = [j + i for j in datas for i in r2]
            self._logger.info("Iniciando requisição")
            requisicao = [requests.get('https://api-redemet.decea.mil.br/produtos/radar/maxcappi?api_key=P53UkUuaqdJ3GjMoRhNElsIf5gzBD2w2iwPNqhUd&anima=3&data=' + i) for i in datas_horas]
            self._logger.info("Requisição completa")
            json = [i.json() for i in requisicao]
            
            self._dlc.fput_object(
                bucket_name="landing",
                object_name=f"simcosta/simcosta_{id}.json",
                file_path="data/temp.json"
            )
            self._logger.info(f'Data id {id} extracted')
        os.remove('data/temp.json')
        
        
        
        
        def _transform(self):
        self._logger.info('Performing transform step')
        
            true = [i for i in json if i["status"] ==True]
            radares = [i["data"]["radar"] for i in true]
            rad1 = [i[0] for i in radares]
            rad2 = [i[1] for i in radares]
            rad3 = [i[2] for i in radares]
            radares_1 = rad1 + rad2 + rad3
            
            date1 = [i[11]["data"] for i in radares_1]
            date_t = [x for x in date1 if x is not None]
            date_t = [datetime.strptime(i, '%Y-%m-%d %H:%M:%S').date() for i in date_t]
            date_days = [i.strftime('%Y%m%d') for i in date_t]
            date_days = list(set(date_days))
            datas_sem_retorno = list(set(datas) - set(date_days))
            datas_sem_retorno = datas_sem_retorno.sort()
            self._logger.info(f'Datas sem retorno da api, ou sem dados válidos: {datas_sem_retorno}')
            
            pc = [i[11]["path"] for i in radares_1]
            path_pc = list(set(pc))
            path_pc = [i for i in path_pc if type(i) is str]
            
            for i in path_pc:
           
              image_url = i
              image_filename = wget.download(image_url)

            self._logger.info("Imagens baixadas")
        
        
        
        
        def _preprocessing(self):
        self._logger.info('Performing preprocessing step')
            
            
def parameter_parser():
    description = 'Script to perform ETL on PICODOCOUTO data. \n \
        Accepts [-e, -t, -p] arguments to perform extract, transform and preprocessing respectively. \n \
        If no arguments are passed, execute all ETL steps. '

    parser = argparse.ArgumentParser(description=description,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser = Logger.add_log_parameters(parser, os.path.basename(__file__))

    parser.add_argument("-e", "--extract",
                        action='store_true',
                        help = "Perform extract process.")
    parser.add_argument("-t", "--transform",
                        action='store_true',
                        help = "Perform light transform process.")
    parser.add_argument("-p", "--preprocessing",
                        action='store_true',
                        help = "Perform heavy transform process.")
    return parser.parse_args()


def main():
    ETL().run(args)


if __name__ == '__main__':
    load_dotenv('config/.env')
    args = parameter_parser()
    Logger.init(filename=args.logfile,
                level=args.loglevel, 
                verbose=args.verbose)
