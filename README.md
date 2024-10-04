# MariaDB migration

This repository contains tests for MariaDB migration as described in my blog post.

You may start the DB with:

```shell
podman run --name mariadb --replace --volume ./data:/var/lib/mysql --publish 3306:3306 --env MARIADB_ROOT_PASSWORD=dev docker.io/mariadb:11.5.2-ubi9@sha256:f83375f42705e73fa752878e9f7ce31d805a99dd5c256a1df3ea05cc1b74f195
```
