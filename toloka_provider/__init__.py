def get_provider_info():
    return {
        'package-name': 'airflow-provider-toloka',
        'name': 'Toloka Airflow Provider',
        'description': 'An Apache Airflow provider for Toloka',
        'connection-types': [
            {
                'connection-type': 'toloka',
                'hook-class-name': 'toloka_provider.hooks.toloka.TolokaHook',
            },
        ],
        'version': ['0.0.6'],
    }
