# vizzuality-large-file-importer
My solution to the coding challenge [Vizzuality, "Large File Importer Challenge"](https://github.com/Vizzuality/coding-challenge-examples/tree/software-engineer/importer-large-files)

# Deployment instructions
To deploy the project, the only thing needed is to run 
```console
docker-compose up -d
```
at the root of the project.


# API endpoints
#### GET /files/status
  * params: {url: string}
Checks the import status of the file located at url.
#### POST /files/import
  * params: {url: string}
Imports the file located at url into DB (takes a while).

#### POST /files/cancel
  * params: {url: string}
Cancels an importing process for the same file located at url.



