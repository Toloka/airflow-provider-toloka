from datetime import timedelta
import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from toloka_airflow import operators as tlk_ops

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5),
    'email': ['airflow@my_first_dag.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(default_args=default_args, schedule_interval=None, catchup=False, tags=['example'])
def text_classification():

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
        performers = []
        for assignment in assignments:
            for task, solution in zip(assignment.tasks, assignment.solutions):
                tasks.append(task.input_values['headline'])
                labels.append(solution.output_values['category'])
                performers.append(assignment.user_id)
        assignments = {
            'task': tasks,
            'performer': performers,
            'label': labels
        }
        assignments = pd.DataFrame.from_dict(assignments)

        df = DawidSkene(n_iter=20).fit_predict(assignments).to_frame().reset_index()
        df.columns = ['headline', 'category']

        print('RESULT', df)

    project_conf = download_json(
        'https://raw.githubusercontent.com/Pocoder/toloka-airflow/main/configs/project.json')
    exam_conf = download_json(
        'https://raw.githubusercontent.com/Pocoder/toloka-airflow/main/configs/exam.json')
    pool_conf = download_json(
        'https://raw.githubusercontent.com/Pocoder/toloka-airflow/main/configs/pool.json')

    project = tlk_ops.create_project(project_conf)
    exam = tlk_ops.create_exam_pool(exam_conf, project=project)
    pool = tlk_ops.create_pool(pool_conf, project=project, exam_pool=exam, expiration=timedelta(days=1))

    dataset = prepare_datasets(
        unlabeled_url='https://raw.githubusercontent.com/Pocoder/toloka-airflow/main/data/not_known.csv',
        labeled_url='https://raw.githubusercontent.com/Pocoder/toloka-airflow/main/data/known.csv',
    )
    main_tasks, exam_tasks, honeypots = dataset['main_tasks'], dataset['exam_tasks'], dataset['honeypots']
    tasks = prepare_tasks(main_tasks)
    exam_tasks = prepare_exam_tasks(exam_tasks)
    honeypots = prepare_honeypots(honeypots)

    _exam_upload = tlk_ops.create_tasks(exam_tasks, pool=exam, kwargs={'open_pool': True, 'allow_defaults': True})
    _hp_upload = tlk_ops.create_tasks(honeypots, pool=pool, kwargs={'allow_defaults': True})
    _tasks_upload = tlk_ops.create_tasks(tasks, pool=pool, kwargs={'allow_defaults': True})

    _waiting = tlk_ops.wait_pool(pool, open_pool=True)
    assignments = tlk_ops.get_assignments(pool)

    aggregate_assignments(assignments)

    [_exam_upload, _hp_upload, _tasks_upload] >> _waiting >> assignments


dag = text_classification()
