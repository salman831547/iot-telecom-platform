"""IoT telecom data platform: reusable pipeline library.

Shared by Airflow DAGs (Cloud Composer), the data generator and CLI tooling.
Runtime dependencies are limited to packages pre-installed in Cloud Composer
images (pydantic, PyYAML, google-cloud-*), so Composer can run as a fully
private environment without PyPI access.
"""

__version__ = "0.1.0"
