# Tower Test :tokyo_tower:

This is a test application that runs inside Tower!

## Deployment

### Preparing `requirements.txt` for Tower

Export your dependencies to a `requirements.txt` file:

```bash
$ poetry export --without-hashes -f requirements.txt -o requirements.txt --without=local
``````

Then add the local depedencies to the `requirements.txt` file:

```bash
$ echo "./.dist/dlt_plus-0.2.5.tar.gz" >> requirements.txt
```
