0.0.8
-------------------
* Add `success_on_reasons` param to `WaitAppBatchSensor` init
* Format all docs according to sphinx standard

0.0.7
-------------------
* Add `WaitAppBatchSensor` for waiting app batches
* Add `test_connection` method to `TolokaHook` for testing connection from UI
* Add `extra__toloka__url` field for setting `TolokaClient`'s url

0.0.6
-------------------
* Update Dockerfile in example. 
Now it installs the latest version of Airflow.
* Changed TolokaHook fields: 
  * Change field for token: now `extra__toloka__token` instead of `password`
  * Add separate text field `extra__toloka__environment` for environment
  * Hide unnecessary fields in UI
* `toloka_provider.sensors.wait_pool` is `toloka_provider.sensors.WaitPoolSensor` now and is inherited from `BaseSensor`.
Also, name of argument `pool` changed to keyword argument `toloka_pool`. 

0.0.5
-------------------
* Fixed import troubles


0.0.4 [YANKED]
-------------------
* Rename package to `airflow-provider-toloka`
* Structure package by standard
* Rename `operators` to `tasks`
* Remove `get_assignments_df`


0.0.3
-------------------
* Fixed `open_pool` issues
* Added tests with code coverage


0.0.2 [YANKED]
-------------------
Yanked due to `open_pool` issues
* Fixed dependencies


0.0.1 [YANKED]
-------------------
Yanked due to dependencies problem
* Initial release
