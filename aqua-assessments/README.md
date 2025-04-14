# Application Deployment Pipeline

This repository contains a GitHub pipeline for testing and deploying applications. The pipeline creates a set of test applications and, if successful, deploys the actual applications. No additional `MODAL_SUFFIX` is required for this process.

## Deploying Additional Sets of Applications

You also have the option to deploy an additional set of applications by appending a suffix to the applications. This feature needs to be run from your local machine. 

To achieve this, set up the environment variable `MODAL_SUFFIX` and run the `modal_deploy.sh` script. For example:

```bash
MODAL_SUFFIX='_aws' ./modal_deploy.sh