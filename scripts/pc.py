import requests
import datetime
from datetime import datetime
import time
import wget
import logging

logging.basicConfig(level=logging.INFO, filename="pc_s.log", format="%(asctime)s - %(levelname)s - %(message)s")



lf = list(open('/home/jose/Dropbox/cefet/IC/pc/copia.txt','r'))
lf1 = [s.rstrip() for s in lf]
lf2 = [s.replace(" ", "") for s in lf1]
lf3 = [datetime.strptime(s, '%d/%m/%Y').date() for s in lf2]
datas = [i.strftime('%Y%m%d') for i in lf3]

#construindo horas do dia:
r1 = [str(r) for r in range(24)]
r2 = [i.zfill(2) for i in r1]

datas1 = [j + i for j in datas for i in r2]



logging.info("Iniciando requisição")

requisicao = [requests.get('https://api-redemet.decea.mil.br/produtos/radar/maxcappi?api_key=P53UkUuaqdJ3GjMoRhNElsIf5gzBD2w2iwPNqhUd&anima=3&data=' + i) for i in datas1]

logging.info("Requisição completa")

json = [i.json() for i in requisicao]



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
logging.warning(f'Datas sem retorno da api: {datas_sem_retorno}')

pc = [i[11]["path"] for i in radares_1]
path_pc = list(set(pc))
path_pc = [i for i in path_pc if type(i) is str]

for i in path_pc:

 image_url = i
 image_filename = wget.download(image_url)

logging.info("Imagens baixadas")