# Dagster

[Dagster](https://github.com/dagster-io/dagster) is a Python library for building data applications.

## Install Chart

To install the Dagster chart into your Kubernetes cluster:

```bash
helm install --namespace "dagster" dagster dagster-k8s/helm/dagster/
```

After installation succeeds, you can get the chart status with:

```bash
helm status dagster
```

To remove Dagster:

```bash
helm delete dagster
```

The Dagster chart configures a Dagit web host Deployment/Service, Celery worker Deployment, and
various other Kubernetes objects to support executing Dagster pipelines within a Kubernetes cluster.

Currently, this chart also installs RabbitMQ and PostgreSQL Helm charts as dependencies; a future
update of this chart will support "bring your own database/queue" configuration.

## Helm Chart Configuration

Full and up-to-date documentation can always be found in the comments of the `values.yaml` file.

See more detailed notes on configuring environment variables in the section below.

## Environment Variables

We support two ways of setting environment variables: either directly add them to `values.yaml`, or
provide an external ConfigMap/Secret object to mount using `envFrom:` in the chart pods:

- `job_runner.env.SOME_ENV_VAR`: Set `SOME_ENV_VAR` in the Celery Worker and Job pods
- `dagit.env.SOME_ENV_VAR`: Set `SOME_ENV_VAR` in the Dagit pod
- `job_runner.env_from_config_maps`: names of ConfigMaps to use for environment variables in the
  Celery Worker and Job pods
- `job_runner.env_from_secrets`: names of Secrets to use for environment variables in the
  Celery Worker and Job pods
- `dagit.env_from_config_maps`: names of ConfigMaps to use for environment variables in the Dagit
  pod
- `dagit.env_from_secrets`: names of Secrets to use for environment variables in the
  Celery Worker and Job pods

### Implementation Notes

This chart installs a base pair of Kubernetes ConfigMap objects for environment variables. These
are:

- `dagster-job-runner-env`: Environment variables for the Celery workers and the spawned Job pod
- `dagster-dagit-env`: Environment variables for the Dagit Deployment pod

Each of these ConfigMaps set `DAGSTER_HOME` by default, and also enable user-specification of
additional environment variables directly in `values.yaml`.

Additionally, this Helm chart supports pulling in other external environment variables from external
Kubernetes ConfigMaps and Secrets. ConfigMaps and Secrets will be loaded in specified order.
