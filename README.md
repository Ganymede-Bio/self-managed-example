# Ganymede Self Managed Repo Template
A template for self managed repos on the Ganymede platform.

## Setup

* Create a Github repository in your organization, or if you have an existing repository you can use that 
* Create a new repo from this templated repo. If you have an existing repo you can copy Github Workflows (deploy-from-action.yaml & pull-from-action.yaml) found in this repo into your repo. They should be in the .github/workflows folder
* In your repo go to Settings > Secrets & Variables > Actions and set a Github repository variable named GANYMEDE_SUBDOMAIN to the corresponding Ganymede subdomain value. If you log into Ganymede at https://mycompany.ganymede.bio, this would be mycompany
* Generate an API token for each Ganymede environment. (Create an environment secret in the Ganymede UI with a name starting with “ganymede_api_key” and a randomly generated value).
* Set environment secrets and variables.
  * If using Github environments (recommended), create an environment corresponding to each Ganymede environment.
  * Set the secret GANYMEDE_API_TOKEN with the generated API token in the corresponding Ganymede environment.
* **pull-from-action.yaml**
    * Update the github_username and github_email environment variables to your desired values. These will only be used for creating pull requests in your Github repository.
    * Update the ENVIRONMENT variable to the Ganymede environment name.
    * Run `pull-from-action.yaml` via workflow dispatch for the desired environment to populate repo with current flows.
* **deploy-from-action.yaml**
  * Define the variable ENVIRONMENT to match the environment for deployment. This should correspond to the directory structure of your repo as well. (ENVIRONMENT/…) (You may want multiple actions or have multiple jobs if you want multiple environments)

## Usage

### Syncing
* Run workflow dispatch on the `pull-from-action.yaml` workflow to pull the latest flow code. This will create a pull request with the changes. You can optionally specify a single flow to sync code from.
* The workflow can also be scheduled to run on a cron schedule.
### Deploying
* Code changes merged into the Main branch will be automatically deployed to Ganymede. The Ganymede commit message will be based on the pull request's commit message.
* Deployment can also be triggered using the workflow dispatch action, provided a user email matching a Ganymede user is supplied.

## Limitations
* The author email in the deploy-from-action must match an email that is registered in Ganymede. If using the push github event, then the committer email address is used by default