from asyncio.log import logger
import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id="rocket_launch_dag",
    start_date=days_ago(14),
    schedule_interval="@daily",
)
launches_folder = "/tmp/rocket_launches"
launches_json = f"{launches_folder}/launches.json"

download_launches = BashOperator(
    task_id="download_launches",
    bash_command=f"curl -o {launches_json} https://api.spacexdata.com/v3/launches/past",
    dag=dag,
)



def _get_pictures(*args, **kwargs):
    #  ensure dir exists
    pathlib.Path(kwargs['images']).mkdir(parents=True, exist_ok=True)
    try:
        logger.info('args :', str(list(args)))
        
        logger.info('kwargs :', str(dict(kwargs)))
    except Exception as e:
        logger.info('Exception :', e)
    # download all pictures from launches.json
    with open(launches_json) as f:
        launches = json.load(f)
        image_urls = {launch["mission_name"]:launch["links"]["flickr_images"] for launch in launches if len(launch["links"]["flickr_images"]) > 0}
        for mission_name, images in image_urls.items():
            for i in range(len(images)):
                try:
                    response = requests.get(images[i])
                except requests_exceptions.RequestException as e:
                    print(f"Failed to download {mission_name} {images[i]}")
                    continue
                except Exception as e:
                    print(f'an exception occured {e}')
                    continue
                image_name = f"{mission_name}_{i}.jpg"
                image_path = sanatize(f"{launches_folder}/images/{image_name}")
                with open(image_path, "w+b") as f:
                    f.write(response.content)
                print(f'downloaded {image_name}')

get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
    op_kwargs={"launches_json": launches_json, "launches_folder": launches_folder, 'images':f'{launches_folder}/images'},
)


notify = BashOperator(
    task_id="notify",
    bash_command=f'echo "There are now $(ls {launches_folder}/images | wc -l) images."',
    dag=dag,
)
def sanatize(val:str)->str:
    return str(val).strip()\
                    .replace(' ', '_')\
                    .replace('/', '_')\
                    .replace('\\', '_')\
                    .replace('.', '_')
download_launches >> get_pictures >> notify