FROM sd2e/reactors:python3

ADD datacatalog /datacatalog

# reactor.py, config.yml, and message.jsonschema will be automatically
# added to the container when you run docker build or abaco deploy
