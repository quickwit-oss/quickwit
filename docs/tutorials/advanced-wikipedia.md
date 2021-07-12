"tokenizer": "en_stem", // Like `default`, but also applies stemming.
"record": "position" // Useful for phrase query.


You can also use a phrase query by using double quotes `"barack obama"`:
```bash
curl "http://0.0.0.0:8080/api/v1/wikipedia/search?query=body:%22barack+obama%22"
```