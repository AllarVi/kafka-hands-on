import logging
import docker

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 27),
}


def read_xcoms(**context):
    for idx, task_id in enumerate(context['data_to_read']):
        data = context['task_instance'].xcom_pull(task_ids=task_id, key='data')
        logging.info(f'[{idx}] I have received data: {data} from task {task_id}')


def launch_docker_container(**context):
    image_name = context['image_name']
    client = docker.from_env()

    logging.info(f"Creating image {image_name}")

    # set network='host' since kafka cluster sits in its own separate docker network and
    # also exposes :29092 to host network.
    container = client.containers.run(detach=True, image=image_name, network='host')

    container_id = container.id
    logging.info(f"Running container with id {container_id}")
    container.start()

    logs = container.logs(follow=True, stderr=True, stdout=True, stream=True, tail='all')

    try:
        while True:
            line = next(logs)
            logging.info(f"Task log: {line}")
    except StopIteration:
        pass

    logging.info("Inspecting container")

    inspect = client.api.inspect_container(container_id)
    logging.info(f"Inspection: {inspect}")

    if inspect['State']['ExitCode'] != 0:
        raise Exception("Container has not finished with exit code 0")

    logging.info(f"Task ends!")

    my_id = context['my_id']
    context['task_instance'].xcom_push('data', f'my name is {my_id}', context['execution_date'])


def do_test_docker():
    logging.info('do_test_docker: executed')

    client = docker.from_env()

    logging.info('do_test_docker: got client')

    for image in client.images.list():
        logging.info(str(image))


with DAG('pipeline_python_2', default_args=default_args) as dag:
    t1 = BashOperator(
        task_id='print_date1',
        bash_command='date')

    t1_5 = PythonOperator(
        task_id="test_docker",
        python_callable=do_test_docker
    )

    t2_1_id = 'do_task_one'
    t2_1 = PythonOperator(
        task_id=t2_1_id,
        provide_context=True,
        op_kwargs={
            'image_name': 'task1',
            'my_id': t2_1_id
        },
        python_callable=launch_docker_container
    )

    t2_2_id = 'do_task_two'
    t2_2 = PythonOperator(
        task_id=t2_2_id,
        provide_context=True,
        op_kwargs={
            'image_name': 'task2',
            'my_id': t2_2_id
        },
        python_callable=launch_docker_container
    )

    t3 = PythonOperator(
        task_id='read_xcoms',
        provide_context=True,
        python_callable=read_xcoms,
        op_kwargs={
            'data_to_read': [t2_1_id, t2_2_id]
        }
    )

    t1 >> t1_5 >> [t2_1, t2_2] >> t3
