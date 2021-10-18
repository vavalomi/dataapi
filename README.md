## Expose Survey Solutions interviews data over GraphQL.

# This is very experimental, don't use it for any real project!



The easiest way to run would be to build the docker image:

```$ docker build -t dataapi .```

Then run it:

```$ docker run -p 8000:80 -e CONNECTION_STRING='connection string' dataapi```

You can now open the browser to http://localhost:8000/graphql to get the interactive GUI.
The same endpoint listens to the POST requests.

`CONNECTION_STRING` must point to the Survey Solutions database.

dataapi reads interviews directly from the database, but still requires the export to be run.
So interiews only become available through GraphQL API only if there is a generated export (in any format).

Query fields for each questionnaire in ALL workspaces are presented, they are labeled as `workspace_questionnaire.var_version`