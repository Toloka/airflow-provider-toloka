from pendulum import datetime
from datetime import timedelta
import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import toloka_provider.tasks.toloka as tlk_tasks
import toloka_provider.sensors.toloka as tlk_sensors


@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={'retries': 0},
    catchup=False,
    tags=['example'],
)
def text_classification():
    """
    ### Example DAG for text classification.

    Showcases usage of the toloka provider package's tasks and sensors.
    Graph creates toloka project which gathers people answers about article headlines: are they clickbaits or not.
    There are also examination pools and control tasks created for selecting good workers and monitor their answers quality.
    Answers are aggregated in the end to compile the final results.

    To run this example, create a Toloka connection with:
    - Conn id: toloka_default
    - Conn type: Toloka
    - password: your toloka token
    (You can learn more about how to get it [here](https://toloka.ai/docs/api/concepts/access.html#access__token).
    """

    @task
    def download_json(url):
        """Download and parse json config stored at given url."""
        import requests

        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task(multiple_outputs=True)
    def prepare_datasets(unlabeled_url: str, labeled_url: str):
        from sklearn.model_selection import train_test_split
        import pandas as pd

        labeled = pd.read_csv(labeled_url)
        labeled, exam_tasks = train_test_split(labeled, test_size=10, stratify=labeled.category)
        _, honeypots = train_test_split(labeled, test_size=20, stratify=labeled.category)

        main_tasks = pd.read_csv(unlabeled_url).sample(n=100)

        return {
            'main_tasks': main_tasks.to_json(),
            'exam_tasks': exam_tasks.to_json(),
            'honeypots': honeypots.to_json()
        }

    @task
    def prepare_tasks(main_tasks):
        main_tasks = json.loads(main_tasks)
        return [{'input_values': {'headline': headline}}
                for headline in main_tasks['headline'].values()]

    @task
    def prepare_exam_tasks(exam_tasks):
        exam_tasks = json.loads(exam_tasks)
        return [{'input_values': {'headline': headline},
                 'known_solutions': [{'output_values': {'category': category}}],
                 'message_on_unknown_solution': category}
                for headline, category in zip(exam_tasks['headline'].values(), exam_tasks['category'].values())]

    @task
    def prepare_honeypots(honeypots):
        honeypots = json.loads(honeypots)
        return [{'input_values': {'headline': headline},
                 'known_solutions': [{'output_values': {'category': category}}]}
                for headline, category in zip(honeypots['headline'].values(), honeypots['category'].values())]

    @task
    def aggregate_assignments(assignments):
        from crowdkit.aggregation import DawidSkene
        from toloka.client import structure, Assignment
        import pandas as pd

        assignments = [Assignment.from_json(assignment) for assignment in assignments]
        tasks = []
        labels = []
        workers = []
        for assignment in assignments:
            for task, solution in zip(assignment.tasks, assignment.solutions):
                tasks.append(task.input_values['headline'])
                labels.append(solution.output_values['category'])
                workers.append(assignment.user_id)
        assignments = {
            'task': tasks,
            'worker': workers,
            'label': labels
        }
        assignments = pd.DataFrame.from_dict(assignments)

        df = DawidSkene(n_iter=20).fit_predict(assignments).to_frame().reset_index()
        df.columns = ['headline', 'category']

        print('RESULT', df)

    project_conf = download_json(
        'https://raw.githubusercontent.com/Toloka/airflow-provider-toloka/main/toloka_provider/example_dags/configs/project.json')
    exam_conf = download_json(
        'https://raw.githubusercontent.com/Toloka/airflow-provider-toloka/main/toloka_provider/example_dags/configs/exam.json')
    pool_conf = download_json(
        'https://raw.githubusercontent.com/Toloka/airflow-provider-toloka/main/toloka_provider/example_dags/configs/pool.json')

    project = tlk_tasks.create_project(project_conf)
    exam = tlk_tasks.create_training_pool(exam_conf, project=project)
    pool = tlk_tasks.create_pool(pool_conf, project=project, training_pool=exam, expiration=timedelta(days=1))

    dataset = prepare_datasets(
        unlabeled_url='https://raw.githubusercontent.com/Toloka/airflow-provider-toloka/main/toloka_provider/example_dags/data/not_known.csv',
        labeled_url='https://raw.githubusercontent.com/Toloka/airflow-provider-toloka/main/toloka_provider/example_dags/data/known.csv',
    )

    tasks = prepare_tasks(dataset['main_tasks'])
    exam_tasks = prepare_exam_tasks(dataset['exam_tasks'])
    honeypots = prepare_honeypots(dataset['honeypots'])

    _exam_upload = tlk_tasks.create_tasks(exam_tasks, pool=exam, additional_args={'open_pool': True, 'allow_defaults': True})
    _honeypots_upload = tlk_tasks.create_tasks(honeypots, pool=pool, additional_args={'allow_defaults': True})
    _tasks_upload = tlk_tasks.create_tasks(tasks, pool=pool, additional_args={'allow_defaults': True})

    opened_pool = tlk_tasks.open_pool(pool)
    _waiting = tlk_sensors.WaitPoolSensor(task_id='wait_pool', toloka_pool=opened_pool)

    assignments = tlk_tasks.get_assignments(pool, 'ACCEPTED')
    aggregate_assignments(assignments)

    [_exam_upload, _honeypots_upload, _tasks_upload] >> opened_pool >> _waiting >> assignments


dag = text_classification()
