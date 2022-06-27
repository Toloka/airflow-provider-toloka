# Airflow Toloka Provider

[![GitHub Tests](https://github.com/Toloka/airflow-provider-toloka/workflows/Tests/badge.svg?branch=main)](//github.com/Toloka/airflow-provider-toloka/actions?query=workflow:Tests)
[![Codecov][codecov_badge]][codecov_link]

[codecov_badge]: https://codecov.io/gh/Toloka/airflow-provider-toloka/branch/main/graph/badge.svg
[codecov_link]: https://codecov.io/gh/Toloka/airflow-provider-toloka

This library allows you to run crowdsourcing [Toloka](https://toloka.ai/) processes in [Apache Airflow](https://airflow.apache.org/) - a widely used workflow management system

Here you can find a collection of ready-made Airflow tasks for the most frequently used actions in [Toloka-Kit](https://github.com/Toloka/toloka-kit).

Getting started
--------------
```
$ pip install airflow-provider-toloka
```

A good way to start is to follow the [example](https://github.com/Toloka/airflow-provider-toloka/blob/main/toloka_provider/example_dags/text_classification.ipynb) in this repo.

`TolokaHook`
--------------
`TolokaHook` is used for getting toloka OAuth token and creating [`TolokaClient`](https://toloka.ai/en/docs/toloka-kit/reference/toloka.client.TolokaClient) with it. 
You can get `TolokaClient` from `TolokaHook` by calling `get_conn()` method.

To make an appropriate Airflow Connection you need to create it in the Airflow Connections UI with following parameters:

* `Conn ID`: `toloka_default`
* `Conn Type`: `Toloka`
* `Token`: enter your OAuth token for Toloka.
        You can learn more about how to get it [here](https://toloka.ai/docs/api/concepts/access.html#access__token).
* `Environment`: enter `production` or `sandbox`

Tasks use the `toloka_default` connection id by default, but
if needed, you can create additional Airflow Connections and reference them
as the function `toloka_conn_id` argument.

Tasks and Sensors
--------------
There are several tasks and sensors that give you easy way to interact with Toloka from Airflow DAGs.
Creating a project and a pool, adding tasks and getting assignments are among them.
You can easily create your own task using `TolokaHook` if it is beyond the scope of implemented ones.
And it would be nice to have your pull request with updates.

Check out our [example](https://github.com/Toloka/airflow-provider-toloka/blob/main/toloka_provider/example_dags/text_classification.ipynb) to see tasks and sensors in the battlefield.

Useful Links
--------------
- [Toloka homepage.](https://toloka.ai/)
- [Apache Airflow homepage.](https://airflow.apache.org/)
- [Toloka API documentation.](https://yandex.com/dev/toloka/doc/concepts/about.html?lang=en)
- [Toloka-kit usage examples.](https://github.com/Toloka/toloka-kit/tree/main/examples#toloka-kit-usage-examples)

Questions and bug reports
--------------
* For reporting bugs please use the [Toloka/bugreport](https://github.com/Toloka/airflow-provider-toloka/issues) page.
* Join our English-speaking [slack community](https://toloka.ai/community) for both tech and abstract questions.

License
-------
© YANDEX LLC, 2022. Licensed under the Apache License, Version 2.0. See LICENSE file for more details.
